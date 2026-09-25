"""Smoke test to verify project structure, packaging, and configuration integrity."""

from pathlib import Path
import yaml
import vault_core


def test_package_metadata():
    """Verify that vault_core package is importable and has a version."""
    assert hasattr(vault_core, "__version__")
    assert vault_core.__version__ == "0.1.0"


def test_project_rules_exists():
    """Verify that PROJECT_RULES.md exists and is non-empty."""
    rules_path = Path(__file__).resolve().parents[2] / "PROJECT_RULES.md"
    assert rules_path.is_file()
    content = rules_path.read_text(encoding="utf-8")
    assert "PROJECT RULES: VAULT" in content
    assert "Do NOT copy code" in content
    assert "No Fabrication of Evidence" in content


def test_policies_config_structure():
    """Verify policies.yaml contains hot, durable, and archive definitions."""
    policies_path = Path(__file__).resolve().parents[2] / "config" / "policies.yaml"
    assert policies_path.is_file()
    with open(policies_path, "r", encoding="utf-8") as f:
        policies = yaml.safe_load(f)

    assert "hot" in policies
    assert "durable" in policies
    assert "archive" in policies

    assert policies["hot"]["replication_factor"] == 3
    assert policies["hot"]["data_write_quorum"] == 2
    assert policies["hot"]["minimum_distinct_zones"] == 3

    assert policies["durable"]["replication_factor"] == 4
    assert policies["durable"]["data_write_quorum"] == 3
    assert policies["durable"]["minimum_distinct_zones"] == 3

    assert policies["archive"]["scheme"] == "erasure_coding"
    assert policies["archive"]["data_fragments"] == 4
    assert policies["archive"]["parity_fragments"] == 2
    assert policies["archive"]["minimum_distinct_zones"] == 3


def test_cluster_config_structure():
    """Verify cluster.yaml defines at least 3 metadata nodes and 6 storage nodes across >=3 zones."""
    cluster_path = Path(__file__).resolve().parents[2] / "config" / "cluster.yaml"
    assert cluster_path.is_file()
    with open(cluster_path, "r", encoding="utf-8") as f:
        cluster = yaml.safe_load(f)

    assert "metadata_nodes" in cluster
    assert len(cluster["metadata_nodes"]) >= 3

    assert "storage_nodes" in cluster
    storage_nodes = cluster["storage_nodes"]
    assert len(storage_nodes) >= 6

    zones = {node["zone"] for node in storage_nodes}
    assert len(zones) >= 3, f"Expected at least 3 distinct zones, found: {zones}"
