"""Deterministic Rendezvous (HRW) hashing and failure-domain prioritized placement."""

import hashlib
from typing import List, Sequence, Set, Union
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.quorum import DurabilityPolicy, InsufficientNodesError, InsufficientZonesError, PolicyValidationError


def compute_rendezvous_score(key_context: str, node: StorageNodeConfig) -> float:
    """
    Compute deterministic Highest Random Weight (HRW) score for a given key context and node.
    Incorporates capacity_weight: score = h ** (1 / weight) where h in (0, 1).
    Higher score indicates higher placement priority.
    """
    digest_input = f"{key_context}:{node.node_id}".encode("utf-8")
    hash_bytes = hashlib.sha256(digest_input).digest()
    # Convert first 8 bytes into a 64-bit integer
    int_val = int.from_bytes(hash_bytes[:8], byteorder="big")
    # Normalize to open interval (0, 1)
    normalized = (int_val + 1) / (float(1 << 64) + 1.0)

    weight = max(0.01, node.capacity_weight)
    return normalized ** (1.0 / weight)


def select_placement_nodes(
    bucket: str,
    key: str,
    version_id: str,
    chunk_index: int,
    policy: DurabilityPolicy,
    nodes_or_cluster: Union[ClusterConfig, Sequence[StorageNodeConfig]],
) -> List[StorageNodeConfig]:
    """
    Compute deterministic placement for an object chunk/fragment.
    Strictly prioritizes:
    1. Different regions
    2. Different zones
    3. Different nodes (no duplicate node selection)
    4. Rendezvous-hash score

    Rejects impossible policies. Never silently degrades placement.
    """
    if isinstance(nodes_or_cluster, ClusterConfig):
        available = nodes_or_cluster.get_active_storage_nodes()
    else:
        available = [n for n in nodes_or_cluster if n.active]

    required_nodes = policy.total_target_nodes
    if len(available) < required_nodes:
        raise InsufficientNodesError(
            f"Policy '{policy.name}' requires {required_nodes} active nodes, "
            f"but only {len(available)} active nodes are available"
        )

    available_zones = {node.zone for node in available}
    if len(available_zones) < policy.minimum_distinct_zones:
        raise InsufficientZonesError(
            f"Policy '{policy.name}' requires at least {policy.minimum_distinct_zones} distinct zones, "
            f"but active nodes only span {len(available_zones)} zones: {available_zones}"
        )

    key_context = f"{bucket}/{key}/{version_id}/{chunk_index}"
    candidates = list(available)
    selected: List[StorageNodeConfig] = []

    while len(selected) < required_nodes:
        selected_regions: Set[str] = {n.region for n in selected}
        selected_zones: Set[str] = {n.zone for n in selected}

        # Rank all remaining candidates
        best_candidate: StorageNodeConfig | None = None
        best_rank = None

        for candidate in candidates:
            is_new_region = 1 if candidate.region not in selected_regions else 0
            is_new_zone = 1 if candidate.zone not in selected_zones else 0
            zone_replica_count = sum(1 for n in selected if n.zone == candidate.zone)
            region_replica_count = sum(1 for n in selected if n.region == candidate.region)
            hrw_score = compute_rendezvous_score(key_context, candidate)

            # Sort tuple: maximize new region, then new zone, minimize zone count, minimize region count, maximize score
            rank = (
                is_new_region,
                is_new_zone,
                -zone_replica_count,
                -region_replica_count,
                hrw_score,
            )

            if best_rank is None or rank > best_rank:
                best_rank = rank
                best_candidate = candidate

        if best_candidate is None:
            raise PolicyValidationError("Could not select required placement nodes")

        selected.append(best_candidate)
        candidates.remove(best_candidate)

    # Post-selection validation: ensure required zone separation was achieved
    achieved_zones = {n.zone for n in selected}
    if len(achieved_zones) < policy.minimum_distinct_zones:
        raise PolicyValidationError(
            f"Placement algorithm failed to achieve required {policy.minimum_distinct_zones} zones. "
            f"Achieved: {achieved_zones}"
        )

    return selected
