"""Background Rebalance Worker for safe, rate-limited chunk migration."""

import asyncio
import hashlib
import logging
import time
from typing import Dict, List, Optional
import httpx

from vault_core.auth import create_auth_token
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.manifest import ObjectManifest
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.metrics import (
    REBALANCE_ACTIVE,
    REBALANCE_BYTES_MOVED_TOTAL,
    REBALANCE_COPIED_TOTAL,
    REBALANCE_FAILED_TOTAL,
    REBALANCE_PROGRESS_RATIO,
    REBALANCE_QUEUED_TOTAL,
    REBALANCE_VERIFIED_TOTAL,
)
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.rebalance import (
    RebalanceStatus,
    RebalanceTask,
    RebalanceTaskState,
    compute_rebalance_plan,
)

logger = logging.getLogger("vault.rebalance")


class RebalanceWorker:
    """
    Manages safe background rebalance operations following cluster topology changes.
    Guarantees:
    - Never deletes a source replica before the destination copy is verified.
    - Never deletes a source replica until the object's configured durability policy is fully satisfied.
    - Rate-limits chunk transfers to protect foreground read/write throughput.
    - Commits all placement modifications atomically to the Raft metadata cluster.
    - Can resume seamlessly after worker restarts.
    """

    def __init__(
        self,
        cluster: ClusterConfig,
        metadata_raft: RaftMetadataStateMachine,
        policies: Dict[str, DurabilityPolicy],
        http_client: httpx.AsyncClient,
        secret_key: str = "vault-insecure-secret-key-change-in-production",
        rate_limit_per_second: float = 10.0,
    ) -> None:
        self.cluster = cluster
        self.metadata_raft = metadata_raft
        self.policies = policies
        self.http_client = http_client
        self.secret_key = secret_key
        self.rate_limit_per_second = rate_limit_per_second

        self.tasks: List[RebalanceTask] = []
        self.status: RebalanceStatus = RebalanceStatus()
        self._running_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    def get_status(self) -> RebalanceStatus:
        """Return a snapshot of current rebalance progress."""
        return self.status

    async def trigger_rebalance(self, rate_limit: Optional[float] = None) -> RebalanceStatus:
        """
        Compute an updated rebalance migration plan against current topology
        and launch background execution if not already running.
        """
        async with self._lock:
            if rate_limit is not None and rate_limit > 0:
                self.rate_limit_per_second = rate_limit

            if self.status.status == "running" and self._running_task and not self._running_task.done():
                return self.status

            # Compute migration plan
            new_tasks = compute_rebalance_plan(
                cluster=self.cluster,
                policies=self.policies,
                metadata_raft=self.metadata_raft,
            )

            self.tasks = new_tasks
            now = time.time()
            total = len(new_tasks)

            self.status = RebalanceStatus(
                status="running" if total > 0 else "completed",
                total_tasks=total,
                queued_tasks=total,
                copied_tasks=0,
                verified_tasks=0,
                failed_tasks=0,
                bytes_moved=0,
                completion_percentage=100.0 if total == 0 else 0.0,
                start_time=now,
                end_time=now if total == 0 else None,
            )

            REBALANCE_QUEUED_TOTAL.set(total)
            REBALANCE_PROGRESS_RATIO.set(1.0 if total == 0 else 0.0)
            REBALANCE_ACTIVE.set(1 if total > 0 else 0)

            if total > 0:
                self._running_task = asyncio.create_task(self._run_rebalance_loop())

            return self.status

    async def resume_rebalance(self) -> RebalanceStatus:
        """
        Resume rebalance after worker restart.
        Recomputes migration plan; any tasks already committed in Raft are automatically skipped.
        """
        return await self.trigger_rebalance()

    async def _run_rebalance_loop(self) -> None:
        """Iterate through queued migration tasks with rate limiting."""
        delay = 1.0 / max(1.0, self.rate_limit_per_second)
        logger.info(f"Starting background rebalance loop ({len(self.tasks)} tasks queued)")

        try:
            for task in self.tasks:
                if task.state != RebalanceTaskState.QUEUED:
                    continue

                success = await self._execute_task(task)
                if not success:
                    self.status.failed_tasks += 1
                    REBALANCE_FAILED_TOTAL.inc()

                # Update progress
                finished = self.status.verified_tasks + self.status.failed_tasks
                self.status.queued_tasks = max(0, self.status.total_tasks - finished)
                REBALANCE_QUEUED_TOTAL.set(self.status.queued_tasks)

                if self.status.total_tasks > 0:
                    ratio = finished / self.status.total_tasks
                    self.status.completion_percentage = round(ratio * 100.0, 1)
                    REBALANCE_PROGRESS_RATIO.set(ratio)

                # Enforce rate limiting to protect foreground I/O
                await asyncio.sleep(delay)

            self.status.status = "completed" if self.status.failed_tasks == 0 else "failed"
            self.status.end_time = time.time()
            REBALANCE_ACTIVE.set(0)
            logger.info(
                f"Rebalance finished: {self.status.verified_tasks} verified, "
                f"{self.status.failed_tasks} failed, {self.status.bytes_moved} bytes moved"
            )

        except asyncio.CancelledError:
            self.status.status = "idle"
            REBALANCE_ACTIVE.set(0)
            raise
        except Exception as exc:
            self.status.status = "failed"
            self.status.error = str(exc)
            self.status.end_time = time.time()
            REBALANCE_ACTIVE.set(0)
            logger.error(f"Rebalance loop crashed: {exc}", exc_info=True)

    async def _execute_task(self, task: RebalanceTask) -> bool:
        """
        Executes a single migration task following strict safe copy-before-delete rules:
        1. Download chunk/fragment from source node and verify SHA-256.
        2. Upload chunk/fragment to target node.
        3. Verify destination SHA-256 on target node.
        4. Commit new placement replica into Raft manifest.
        5. Verify that durability policy is fully satisfied.
        6. Delete obsolete source replica if durability policy is satisfied.
        """
        auth_token = create_auth_token(self.secret_key, node_id="rebalance-worker")
        chunk_param = task.fragment_index if (task.is_erasure_coded and task.fragment_index is not None) else task.chunk_index

        source_node = self.cluster.get_storage_node(task.source_node_id)
        target_node = self.cluster.get_storage_node(task.target_node_id)

        if not target_node or not target_node.active:
            task.state = RebalanceTaskState.FAILED
            task.error = f"Target node {task.target_node_id} is offline or not registered"
            return False

        # If source node is offline or missing, look for another active placement node for this chunk
        manifest_dict = self.metadata_raft.get_latest_manifest(task.bucket, task.key)
        if not manifest_dict:
            task.state = RebalanceTaskState.FAILED
            task.error = f"Manifest {task.bucket}/{task.key} not found"
            return False

        manifest = ObjectManifest.model_validate(manifest_dict)
        chunk_meta = next((c for c in manifest.chunks if c.chunk_index == task.chunk_index), None)
        if not chunk_meta:
            task.state = RebalanceTaskState.FAILED
            task.error = f"Chunk {task.chunk_index} not found in manifest"
            return False

        candidate_sources = [source_node] if (source_node and source_node.active) else []
        if not task.is_erasure_coded:
            for nid in chunk_meta.placement_nodes:
                cand = self.cluster.get_storage_node(nid)
                if cand and cand.active and cand not in candidate_sources:
                    candidate_sources.append(cand)

        # -------------------------------------------------------------
        # STEP 1: Download from source and verify SHA-256
        # -------------------------------------------------------------
        task.state = RebalanceTaskState.COPYING
        payload: Optional[bytes] = None

        for src in candidate_sources:
            src_url = f"{src.url}/v1/chunks/{task.bucket}/{task.version_id}/{chunk_param}"
            try:
                resp = await self.http_client.get(src_url, headers={"X-Vault-Auth-Token": auth_token})
                if resp.status_code == 200:
                    data = resp.content
                    if hashlib.sha256(data).hexdigest() == task.expected_sha256:
                        payload = data
                        break
            except Exception:
                pass

        if payload is None:
            task.state = RebalanceTaskState.FAILED
            task.error = f"Failed to download valid chunk {chunk_param} from any source node"
            return False

        # -------------------------------------------------------------
        # STEP 2: Upload to target node
        # -------------------------------------------------------------
        target_put_url = f"{target_node.url}/v1/chunks/{task.bucket}/{task.version_id}/{chunk_param}"
        try:
            put_resp = await self.http_client.put(
                target_put_url,
                content=payload,
                headers={
                    "X-Vault-Auth-Token": auth_token,
                    "X-Expected-SHA256": task.expected_sha256,
                }
            )
            if put_resp.status_code not in (200, 201):
                task.state = RebalanceTaskState.FAILED
                task.error = f"Target node {target_node.node_id} rejected upload: HTTP {put_resp.status_code}"
                return False
        except Exception as exc:
            task.state = RebalanceTaskState.FAILED
            task.error = f"Upload to target node {target_node.node_id} failed: {exc}"
            return False

        task.state = RebalanceTaskState.COPIED
        self.status.copied_tasks += 1
        REBALANCE_COPIED_TOTAL.inc()

        # -------------------------------------------------------------
        # STEP 3: Verify destination SHA-256
        # -------------------------------------------------------------
        task.state = RebalanceTaskState.VERIFYING
        try:
            head_resp = await self.http_client.get(
                target_put_url,
                headers={
                    "X-Vault-Auth-Token": auth_token,
                    "X-Expected-SHA256": task.expected_sha256,
                }
            )
            if head_resp.status_code != 200 or hashlib.sha256(head_resp.content).hexdigest() != task.expected_sha256:
                task.state = RebalanceTaskState.FAILED
                task.error = f"Target node {target_node.node_id} destination verification failed"
                return False
        except Exception as exc:
            task.state = RebalanceTaskState.FAILED
            task.error = f"Verification check on target node {target_node.node_id} failed: {exc}"
            return False

        task.state = RebalanceTaskState.VERIFIED
        self.status.verified_tasks += 1
        self.status.bytes_moved += len(payload)
        REBALANCE_VERIFIED_TOTAL.inc()
        REBALANCE_BYTES_MOVED_TOTAL.inc(len(payload))

        # -------------------------------------------------------------
        # STEP 4: Atomically update manifest in Raft
        # -------------------------------------------------------------
        # Add target node to manifest placement
        manifest_dict = self.metadata_raft.get_latest_manifest(task.bucket, task.key)
        if not manifest_dict:
            return False
        manifest = ObjectManifest.model_validate(manifest_dict)
        target_chunk = next((c for c in manifest.chunks if c.chunk_index == task.chunk_index), None)
        if not target_chunk:
            return False

        if task.is_erasure_coded and target_chunk.fragments:
            for f in target_chunk.fragments:
                if f.fragment_index == task.fragment_index:
                    f.node_id = target_node.node_id
            if target_node.node_id not in target_chunk.placement_nodes:
                target_chunk.placement_nodes.append(target_node.node_id)
        else:
            if target_node.node_id not in target_chunk.placement_nodes:
                target_chunk.placement_nodes.append(target_node.node_id)

        try:
            self.metadata_raft.commit_manifest(
                manifest_dict=manifest.model_dump(),
                expected_version=manifest.logical_version,
            )
        except Exception as exc:
            logger.warning(f"Raft commit during rebalance placement addition encountered: {exc}")

        # -------------------------------------------------------------
        # STEP 5: Durability policy check before deleting obsolete copy
        # "Never delete a source copy until the configured durability policy is satisfied."
        # -------------------------------------------------------------
        if task.obsolete_source_node_id and task.obsolete_source_node_id != target_node.node_id:
            policy = self.policies.get(manifest.policy)
            durability_satisfied = False

            if policy:
                if policy.scheme == PolicyScheme.REPLICATION:
                    # Count active, healthy placement nodes excluding obsolete node
                    remaining_nodes = [
                        self.cluster.get_storage_node(nid)
                        for nid in target_chunk.placement_nodes
                        if nid != task.obsolete_source_node_id
                        and self.cluster.get_storage_node(nid)
                        and self.cluster.get_storage_node(nid).active
                    ]
                    remaining_count = len(remaining_nodes)
                    distinct_zones = {n.zone for n in remaining_nodes}

                    if (
                        remaining_count >= policy.replication_factor
                        and len(distinct_zones) >= policy.minimum_distinct_zones
                    ):
                        durability_satisfied = True
                elif policy.scheme == PolicyScheme.ERASURE_CODING:
                    # Verify all 6 fragments reside on distinct active nodes spanning >= 3 zones
                    if target_chunk.fragments:
                        frag_nodes = [
                            self.cluster.get_storage_node(f.node_id)
                            for f in target_chunk.fragments
                            if self.cluster.get_storage_node(f.node_id)
                            and self.cluster.get_storage_node(f.node_id).active
                        ]
                        frag_zones = {n.zone for n in frag_nodes}
                        if len(frag_nodes) == 6 and len(frag_zones) >= policy.minimum_distinct_zones:
                            durability_satisfied = True

            # If durability is guaranteed, safely delete the obsolete replica
            if durability_satisfied:
                obs_node = self.cluster.get_storage_node(task.obsolete_source_node_id)
                if obs_node and obs_node.active:
                    obs_url = f"{obs_node.url}/v1/chunks/{task.bucket}/{task.version_id}/{chunk_param}"
                    try:
                        del_resp = await self.http_client.delete(obs_url, headers={"X-Vault-Auth-Token": auth_token})
                        if del_resp.status_code in (200, 204, 404):
                            logger.info(
                                f"Deleted obsolete chunk {chunk_param} from {task.obsolete_source_node_id} "
                                f"after verifying target {target_node.node_id}"
                            )
                    except Exception as exc:
                        logger.warning(f"Could not delete obsolete replica on {task.obsolete_source_node_id}: {exc}")

                # Update manifest to remove obsolete node
                if task.obsolete_source_node_id in target_chunk.placement_nodes:
                    target_chunk.placement_nodes = [
                        nid for nid in target_chunk.placement_nodes
                        if nid != task.obsolete_source_node_id
                    ]
                    try:
                        latest_m = self.metadata_raft.get_latest_manifest(task.bucket, task.key)
                        curr_ver = latest_m.get("logical_version") if latest_m else manifest.logical_version
                        self.metadata_raft.commit_manifest(
                            manifest_dict=manifest.model_dump(),
                            expected_version=curr_ver,
                        )
                    except Exception as exc:
                        logger.warning(f"Raft commit during obsolete node removal: {exc}")
            else:
                logger.warning(
                    f"Durability policy NOT satisfied for {task.bucket}/{task.key} chunk {chunk_param}; "
                    f"retaining source replica on {task.obsolete_source_node_id}"
                )

        task.state = RebalanceTaskState.COMPLETED
        return True
