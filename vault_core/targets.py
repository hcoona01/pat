"""Configurable prototype acceptance targets and SLO specifications."""

import os
from pathlib import Path
from typing import Dict, Optional
from pydantic import BaseModel, Field
import yaml


class PolicyTarget(BaseModel):
    """Measurable resilience and efficiency targets for a durability policy."""
    policy_name: str
    data_loss_tolerance_nodes: int
    expected_storage_amplification: float
    description: str = ""


class AcceptanceTargets(BaseModel):
    """Aggregate measurable prototype acceptance targets."""
    repair_slo_seconds: float = 15.0
    large_object_size_bytes: int = 1024 * 1024 * 1024  # 1 GiB
    large_object_count: int = 1000
    policy_targets: Dict[str, PolicyTarget] = Field(default_factory=lambda: {
        "hot": PolicyTarget(
            policy_name="hot",
            data_loss_tolerance_nodes=2,
            expected_storage_amplification=3.0,
            description="Replication Factor 3 across >=3 zones; tolerates 2 node failures",
        ),
        "durable": PolicyTarget(
            policy_name="durable",
            data_loss_tolerance_nodes=3,
            expected_storage_amplification=4.0,
            description="Replication Factor 4 across >=3 zones; tolerates 3 node failures",
        ),
        "archive": PolicyTarget(
            policy_name="archive",
            data_loss_tolerance_nodes=2,
            expected_storage_amplification=1.5,
            description="Reed-Solomon 4+2 across >=3 zones; tolerates 2 fragment/node failures",
        ),
    })


def load_acceptance_targets(config_path: Optional[Path] = None) -> AcceptanceTargets:
    """Load acceptance targets from YAML file or return defaults."""
    path = config_path or Path(os.environ.get("VAULT_TARGETS_CONFIG_PATH", "config/targets.yaml"))
    if path.is_file():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return AcceptanceTargets.model_validate(data)
        except Exception:
            pass
    return AcceptanceTargets()
