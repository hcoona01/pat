"""Dynamic HRW placement rebalance engine and task definitions."""

from enum import Enum
import time
import uuid
from typing import Dict, List, Optional, Set
from pydantic import BaseModel, Field

from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.manifest import ObjectManifest
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.placement import select_placement_nodes
from vault_core.quorum import DurabilityPolicy, PolicyScheme


class RebalanceTaskState(str, Enum):
    """Lifecycle states of a chunk rebalance migration task."""
    QUEUED = "queued"
    COPYING = "copying"
    COPIED = "copied"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    COMMITTED = "committed"
    COMPLETED = "completed"
    FAILED = "failed"


class RebalanceTask(BaseModel):
    """Specification of an individual chunk or fragment migration during rebalancing."""
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    bucket: str
    key: str
    version_id: str
    chunk_index: int
    is_erasure_coded: bool = False
    fragment_index: Optional[int] = None
    source_node_id: str
    target_node_id: str
    expected_sha256: str
    size_bytes: int = 0
    state: RebalanceTaskState = RebalanceTaskState.QUEUED
    obsolete_source_node_id: Optional[str] = None
    error: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)


class RebalanceStatus(BaseModel):
    """Aggregate progress report for an active or completed rebalance operation."""
    status: str = "idle"  # idle, running, completed, failed
    total_tasks: int = 0
    queued_tasks: int = 0
    copied_tasks: int = 0
    verified_tasks: int = 0
    failed_tasks: int = 0
    bytes_moved: int = 0
    completion_percentage: float = 0.0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error: Optional[str] = None


def compute_rebalance_plan(
    cluster: ClusterConfig,
    policies: Dict[str, DurabilityPolicy],
    metadata_raft: RaftMetadataStateMachine,
) -> List[RebalanceTask]:
    """
    Compare current chunk replica/fragment locations against deterministic Rendezvous (HRW)
    placement for the current cluster topology, generating migration tasks for all discrepancies.
    """
    tasks: List[RebalanceTask] = []
    current_versions = getattr(metadata_raft, "_current_versions", {})

    for obj_key, curr in list(current_versions.items()):
        if curr.get("is_tombstone"):
            continue

        parts = obj_key.split("/", 1)
        if len(parts) != 2:
            continue
        bucket, key = parts[0], parts[1]

        manifest_dict = metadata_raft.get_latest_manifest(bucket, key)
        if not manifest_dict or manifest_dict.get("is_tombstone"):
            continue

        manifest = ObjectManifest.model_validate(manifest_dict)
        policy_name = manifest.policy
        policy = policies.get(policy_name)
        if not policy:
            continue

        for chunk_meta in manifest.chunks:
            # 1. Erasure-Coded Chunks (e.g. 4+2 archive policy)
            if chunk_meta.is_erasure_coded and chunk_meta.fragments:
                try:
                    desired_target_nodes = select_placement_nodes(
                        bucket=bucket,
                        key=key,
                        version_id=manifest.version_id,
                        chunk_index=chunk_meta.chunk_index,
                        policy=policy,
                        nodes_or_cluster=cluster,
                    )
                except Exception:
                    continue

                for frag_info in chunk_meta.fragments:
                    frag_idx = frag_info.fragment_index
                    if frag_idx >= len(desired_target_nodes):
                        continue
                    desired_node = desired_target_nodes[frag_idx]

                    if frag_info.node_id != desired_node.node_id:
                        tasks.append(RebalanceTask(
                            bucket=bucket,
                            key=key,
                            version_id=manifest.version_id,
                            chunk_index=chunk_meta.chunk_index,
                            is_erasure_coded=True,
                            fragment_index=frag_idx,
                            source_node_id=frag_info.node_id,
                            target_node_id=desired_node.node_id,
                            obsolete_source_node_id=frag_info.node_id,
                            expected_sha256=frag_info.sha256,
                            size_bytes=frag_info.size_bytes,
                        ))

            # 2. Replicated Chunks (hot / durable policies)
            else:
                try:
                    desired_target_nodes = select_placement_nodes(
                        bucket=bucket,
                        key=key,
                        version_id=manifest.version_id,
                        chunk_index=chunk_meta.chunk_index,
                        policy=policy,
                        nodes_or_cluster=cluster,
                    )
                except Exception:
                    continue

                desired_node_ids = {n.node_id for n in desired_target_nodes}
                current_node_ids = set(chunk_meta.placement_nodes)

                added_node_ids = list(desired_node_ids - current_node_ids)
                obsolete_node_ids = list(current_node_ids - desired_node_ids)

                if not added_node_ids:
                    continue

                # Choose best active source node among existing placements
                active_sources = [
                    nid for nid in current_node_ids
                    if cluster.get_storage_node(nid) and cluster.get_storage_node(nid).active
                ]
                if not active_sources:
                    active_sources = list(current_node_ids)

                for idx, target_nid in enumerate(added_node_ids):
                    source_nid = active_sources[idx % len(active_sources)] if active_sources else target_nid
                    obsolete_nid = obsolete_node_ids[idx] if idx < len(obsolete_node_ids) else None

                    tasks.append(RebalanceTask(
                        bucket=bucket,
                        key=key,
                        version_id=manifest.version_id,
                        chunk_index=chunk_meta.chunk_index,
                        is_erasure_coded=False,
                        source_node_id=source_nid,
                        target_node_id=target_nid,
                        obsolete_source_node_id=obsolete_nid,
                        expected_sha256=chunk_meta.sha256,
                        size_bytes=chunk_meta.size_bytes,
                    ))

    return tasks
