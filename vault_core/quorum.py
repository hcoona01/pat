"""Durability policies, quorum bounds, and cluster validation."""

from enum import Enum
from pathlib import Path
from typing import Dict, Optional
import yaml
from pydantic import BaseModel, Field, model_validator

from vault_core.cluster import ClusterConfig


class PolicyScheme(str, Enum):
    """Storage scheme for an object durability policy."""
    REPLICATION = "replication"
    ERASURE_CODING = "erasure_coding"


class PolicyValidationError(Exception):
    """Base error for policy validation failures."""
    pass


class InsufficientNodesError(PolicyValidationError):
    """Raised when active node count is insufficient to satisfy policy."""
    pass


class InsufficientZonesError(PolicyValidationError):
    """Raised when active zone count is insufficient to satisfy policy."""
    pass


class DurabilityPolicy(BaseModel):
    """Defines replication or erasure coding durability policy rules."""
    name: str = Field(..., description="Unique policy identifier")
    scheme: PolicyScheme = Field(..., description="Replication or erasure coding")
    replication_factor: Optional[int] = Field(default=None, ge=1)
    data_write_quorum: Optional[int] = Field(default=None, ge=1)
    data_read_quorum: Optional[int] = Field(default=None, ge=1)
    data_fragments: Optional[int] = Field(default=None, ge=1)
    parity_fragments: Optional[int] = Field(default=None, ge=1)
    minimum_distinct_zones: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def validate_policy_parameters(self) -> "DurabilityPolicy":
        if self.scheme == PolicyScheme.REPLICATION:
            if not self.replication_factor:
                raise ValueError("Replication policy must specify replication_factor")
            if not self.data_write_quorum or not self.data_read_quorum:
                raise ValueError("Replication policy must specify data_write_quorum and data_read_quorum")
            if self.data_write_quorum > self.replication_factor:
                raise ValueError("data_write_quorum cannot exceed replication_factor")
            if self.data_read_quorum > self.replication_factor:
                raise ValueError("data_read_quorum cannot exceed replication_factor")
            # Overlap condition: write_quorum + read_quorum >= replication_factor
            if self.data_write_quorum + self.data_read_quorum < self.replication_factor:
                raise ValueError(
                    f"Quorum violation: write_quorum ({self.data_write_quorum}) + read_quorum "
                    f"({self.data_read_quorum}) must be at least replication_factor ({self.replication_factor})"
                )
        elif self.scheme == PolicyScheme.ERASURE_CODING:
            if not self.data_fragments or not self.parity_fragments:
                raise ValueError("Erasure coding policy must specify data_fragments and parity_fragments")
        return self

    @property
    def total_target_nodes(self) -> int:
        """Total number of storage nodes needed to hold all replicas/fragments of a chunk."""
        if self.scheme == PolicyScheme.REPLICATION:
            return self.replication_factor or 3
        return (self.data_fragments or 4) + (self.parity_fragments or 2)

    @property
    def write_quorum(self) -> int:
        """Required successful node writes before acknowledging PUT."""
        if self.scheme == PolicyScheme.REPLICATION:
            return self.data_write_quorum or 2
        # For initial EC write, all fragments (K + M) must be acknowledged
        return self.total_target_nodes

    @property
    def read_quorum(self) -> int:
        """Minimum valid replicas/fragments needed to satisfy a GET."""
        if self.scheme == PolicyScheme.REPLICATION:
            return self.data_read_quorum or 1
        return self.data_fragments or 4

    @property
    def storage_overhead(self) -> float:
        """Approximate storage amplification ratio."""
        if self.scheme == PolicyScheme.REPLICATION:
            return float(self.replication_factor or 3)
        return float(self.total_target_nodes) / float(self.data_fragments or 4)


def load_policies_from_yaml(path: Path | str) -> Dict[str, DurabilityPolicy]:
    """Parse policies YAML and return a dictionary of named DurabilityPolicy objects."""
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Policies configuration file not found at {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_policies = yaml.safe_load(f)

    policies: Dict[str, DurabilityPolicy] = {}
    for policy_name, policy_data in raw_policies.items():
        if isinstance(policy_data, dict):
            policies[policy_name] = DurabilityPolicy(name=policy_name, **policy_data)

    return policies


def validate_policy_against_cluster(policy: DurabilityPolicy, cluster: ClusterConfig) -> None:
    """
    Validate that active cluster nodes and zones satisfy the policy.
    Rejects impossible policies. Never silently degrades placement.
    """
    active_nodes = cluster.get_active_storage_nodes()
    if len(active_nodes) < policy.total_target_nodes:
        raise InsufficientNodesError(
            f"Policy '{policy.name}' requires {policy.total_target_nodes} active nodes, "
            f"but only {len(active_nodes)} active nodes are available in cluster"
        )

    active_zones = cluster.distinct_active_zones()
    if len(active_zones) < policy.minimum_distinct_zones:
        raise InsufficientZonesError(
            f"Policy '{policy.name}' requires at least {policy.minimum_distinct_zones} distinct zones, "
            f"but active nodes only span {len(active_zones)} zones: {active_zones}"
        )
