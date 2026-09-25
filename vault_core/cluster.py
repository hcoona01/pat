"""Cluster topology and storage node configuration models."""

from pathlib import Path
from typing import List, Optional, Set
import yaml
from pydantic import BaseModel, Field


class StorageNodeConfig(BaseModel):
    """Configuration definition for a storage node."""
    node_id: str = Field(..., description="Unique alphanumeric identifier for the storage node")
    url: str = Field(..., description="Base HTTP URL for accessing the node API")
    region: str = Field(default="us-east-1", description="Geographic region of the node")
    zone: str = Field(..., description="Availability zone identifier (e.g. us-east-1a)")
    active: bool = Field(default=True, description="Whether the node is active and accepting traffic")
    capacity_weight: float = Field(default=1.0, ge=0.1, le=100.0, description="Relative capacity weight for HRW placement")

    def __hash__(self) -> int:
        return hash(self.node_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StorageNodeConfig):
            return False
        return self.node_id == other.node_id


class MetadataNodeConfig(BaseModel):
    """Configuration definition for a metadata consensus node."""
    id: str = Field(..., description="Unique metadata node identifier")
    host: str = Field(..., description="Hostname or IP address")
    url: Optional[str] = Field(default=None, description="HTTP API URL")
    api_port: int = Field(default=9001, description="REST API port")
    raft_port: int = Field(default=9002, description="Raft consensus communication port")


class ClusterConfig(BaseModel):
    """Complete cluster configuration describing topology and membership."""
    cluster_id: str = Field(default="vault-cluster", description="Logical cluster ID")
    metadata_nodes: List[MetadataNodeConfig] = Field(default_factory=list)
    storage_nodes: List[StorageNodeConfig] = Field(default_factory=list)

    @classmethod
    def from_yaml_file(cls, path: Path | str) -> "ClusterConfig":
        """Load and parse cluster configuration from a YAML file."""
        file_path = Path(path)
        if not file_path.is_file():
            raise FileNotFoundError(f"Cluster configuration file not found at {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data)

    def get_active_storage_nodes(self) -> List[StorageNodeConfig]:
        """Return list of all currently active storage nodes."""
        return [node for node in self.storage_nodes if node.active]

    def get_storage_node(self, node_id: str) -> Optional[StorageNodeConfig]:
        """Find storage node by ID."""
        for node in self.storage_nodes:
            if node.node_id == node_id:
                return node
        return None

    def distinct_active_zones(self) -> Set[str]:
        """Set of unique availability zones populated by active storage nodes."""
        return {node.zone for node in self.get_active_storage_nodes()}

    def distinct_active_regions(self) -> Set[str]:
        """Set of unique regions populated by active storage nodes."""
        return {node.region for node in self.get_active_storage_nodes()}
