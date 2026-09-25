"""Unit tests for deterministic Rendezvous (HRW) placement and policy validation."""

import pytest
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.placement import compute_rendezvous_score, select_placement_nodes
from vault_core.quorum import (
    DurabilityPolicy,
    InsufficientNodesError,
    InsufficientZonesError,
    PolicyScheme,
    PolicyValidationError,
    load_policies_from_yaml,
)


@pytest.fixture
def standard_cluster() -> ClusterConfig:
    """Fixture providing standard 6-node cluster spanning 3 zones."""
    return ClusterConfig(
        cluster_id="test-cluster",
        storage_nodes=[
            StorageNodeConfig(node_id="store-1", url="http://s1:8001", region="us-east-1", zone="us-east-1a", active=True),
            StorageNodeConfig(node_id="store-2", url="http://s2:8001", region="us-east-1", zone="us-east-1a", active=True),
            StorageNodeConfig(node_id="store-3", url="http://s3:8001", region="us-east-1", zone="us-east-1b", active=True),
            StorageNodeConfig(node_id="store-4", url="http://s4:8001", region="us-east-1", zone="us-east-1b", active=True),
            StorageNodeConfig(node_id="store-5", url="http://s5:8001", region="us-east-1", zone="us-east-1c", active=True),
            StorageNodeConfig(node_id="store-6", url="http://s6:8001", region="us-east-1", zone="us-east-1c", active=True),
        ]
    )


@pytest.fixture
def policies() -> dict[str, DurabilityPolicy]:
    """Standard policies dictionary."""
    return {
        "hot": DurabilityPolicy(
            name="hot",
            scheme=PolicyScheme.REPLICATION,
            replication_factor=3,
            data_write_quorum=2,
            data_read_quorum=1,
            minimum_distinct_zones=3,
        ),
        "durable": DurabilityPolicy(
            name="durable",
            scheme=PolicyScheme.REPLICATION,
            replication_factor=4,
            data_write_quorum=3,
            data_read_quorum=1,
            minimum_distinct_zones=3,
        ),
        "archive": DurabilityPolicy(
            name="archive",
            scheme=PolicyScheme.ERASURE_CODING,
            data_fragments=4,
            parity_fragments=2,
            minimum_distinct_zones=3,
        ),
    }


def test_deterministic_placement(standard_cluster, policies):
    """Verify that multiple evaluations for the same input produce identical placement."""
    policy = policies["hot"]
    bucket = "test-bucket"
    key = "documents/file.txt"
    version_id = "v-uuid-12345"
    chunk_index = 0

    first_result = [
        n.node_id for n in select_placement_nodes(bucket, key, version_id, chunk_index, policy, standard_cluster)
    ]

    for _ in range(50):
        subsequent = [
            n.node_id for n in select_placement_nodes(bucket, key, version_id, chunk_index, policy, standard_cluster)
        ]
        assert subsequent == first_result


def test_no_duplicate_node_selection(standard_cluster, policies):
    """Verify that no node is selected more than once for a chunk's replicas/fragments."""
    for policy_name in ("hot", "durable", "archive"):
        policy = policies[policy_name]
        selected = select_placement_nodes("b", "k", "v", 0, policy, standard_cluster)
        node_ids = [n.node_id for n in selected]
        assert len(node_ids) == len(set(node_ids)), f"Duplicate nodes detected in policy {policy_name}: {node_ids}"
        assert len(node_ids) == policy.total_target_nodes


def test_zone_separation_prioritization(standard_cluster, policies):
    """Verify that placement strictly separates across distinct zones."""
    # Hot policy (3 replicas) across 3 zones must pick 1 node per zone
    hot_selected = select_placement_nodes("b", "k", "v1", 0, policies["hot"], standard_cluster)
    hot_zones = [n.zone for n in hot_selected]
    assert len(set(hot_zones)) == 3, f"Hot policy did not achieve 3 distinct zones: {hot_zones}"

    # Durable policy (4 replicas) across 3 zones must pick from all 3 zones
    durable_selected = select_placement_nodes("b", "k", "v1", 0, policies["durable"], standard_cluster)
    durable_zones = [n.zone for n in durable_selected]
    assert len(set(durable_zones)) == 3, f"Durable policy did not span 3 zones: {durable_zones}"

    # Archive policy (6 fragments) must select all 6 nodes across 3 zones
    archive_selected = select_placement_nodes("b", "k", "v1", 0, policies["archive"], standard_cluster)
    archive_zones = [n.zone for n in archive_selected]
    assert len(set(archive_zones)) == 3
    assert len(archive_selected) == 6


def test_multi_region_prioritization(policies):
    """Verify that region diversity takes precedence over zone/node selection."""
    cluster_multi_region = [
        StorageNodeConfig(node_id="us-1", url="http://u1:8001", region="us-east", zone="zone-1", active=True),
        StorageNodeConfig(node_id="us-2", url="http://u2:8001", region="us-east", zone="zone-2", active=True),
        StorageNodeConfig(node_id="eu-1", url="http://e1:8001", region="eu-central", zone="zone-3", active=True),
        StorageNodeConfig(node_id="eu-2", url="http://e2:8001", region="eu-central", zone="zone-4", active=True),
    ]

    policy_2_replicas = DurabilityPolicy(
        name="cross-region",
        scheme=PolicyScheme.REPLICATION,
        replication_factor=2,
        data_write_quorum=2,
        data_read_quorum=1,
        minimum_distinct_zones=2,
    )

    selected = select_placement_nodes("b", "k", "v", 0, policy_2_replicas, cluster_multi_region)
    regions = {n.region for n in selected}
    assert regions == {"us-east", "eu-central"}, f"Failed to pick across distinct regions: {regions}"


def test_placement_stability_unchanged_membership(standard_cluster, policies):
    """Verify placement stability across 50 distinct keys when membership is unchanged."""
    policy = policies["hot"]
    key_placements_run1 = {}
    for i in range(50):
        key = f"objects/file_{i}.dat"
        selected = [n.node_id for n in select_placement_nodes("my-bucket", key, "v1", 0, policy, standard_cluster)]
        key_placements_run1[key] = selected

    key_placements_run2 = {}
    for i in range(50):
        key = f"objects/file_{i}.dat"
        selected = [n.node_id for n in select_placement_nodes("my-bucket", key, "v1", 0, policy, standard_cluster)]
        key_placements_run2[key] = selected

    assert key_placements_run1 == key_placements_run2


def test_reject_impossible_policy_insufficient_nodes(policies):
    """Verify rejection when cluster lacks sufficient active nodes."""
    small_cluster = [
        StorageNodeConfig(node_id="s1", url="http://s1:8001", region="r1", zone="z1", active=True),
        StorageNodeConfig(node_id="s2", url="http://s2:8001", region="r1", zone="z2", active=True),
    ]
    with pytest.raises(InsufficientNodesError) as exc_info:
        select_placement_nodes("b", "k", "v", 0, policies["hot"], small_cluster)

    assert "requires 3 active nodes, but only 2" in str(exc_info.value)


def test_reject_impossible_policy_insufficient_zones(policies):
    """Verify rejection when cluster lacks required distinct zones."""
    # 6 nodes, but all in 2 zones only
    two_zone_cluster = [
        StorageNodeConfig(node_id=f"s{i}", url=f"http://s{i}:8001", region="r1", zone=f"z{i % 2}", active=True)
        for i in range(6)
    ]
    with pytest.raises(InsufficientZonesError) as exc_info:
        select_placement_nodes("b", "k", "v", 0, policies["hot"], two_zone_cluster)

    assert "requires at least 3 distinct zones, but active nodes only span 2" in str(exc_info.value)


def test_policy_strict_consistency_validation():
    """Verify that quorum condition W + R > N is strictly enforced."""
    with pytest.raises(ValueError) as exc_info:
        DurabilityPolicy(
            name="broken-quorum",
            scheme=PolicyScheme.REPLICATION,
            replication_factor=3,
            data_write_quorum=1,  # 1 + 1 = 2 <= 3 (violates W + R > N)
            data_read_quorum=1,
            minimum_distinct_zones=1,
        )
    assert "Quorum violation" in str(exc_info.value)


def test_capacity_weight_influences_hrw_selection():
    """Verify that higher capacity weight yields higher placement preference on average."""
    light_node = StorageNodeConfig(node_id="light", url="http://l:8001", region="r1", zone="z1", capacity_weight=1.0)
    heavy_node = StorageNodeConfig(node_id="heavy", url="http://h:8001", region="r1", zone="z1", capacity_weight=10.0)

    heavy_wins = 0
    total_samples = 500

    for i in range(total_samples):
        key = f"key_{i}"
        s_light = compute_rendezvous_score(key, light_node)
        s_heavy = compute_rendezvous_score(key, heavy_node)
        if s_heavy > s_light:
            heavy_wins += 1

    # Heavy node with 10x weight should win the vast majority of keys
    win_rate = heavy_wins / total_samples
    assert win_rate > 0.70, f"Expected heavy node to win majority of evaluations, got {win_rate:.2%}"
