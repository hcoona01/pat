"""Background and read-repair worker for replica convergence and bitrot restoration."""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
import httpx

from vault_core.auth import create_auth_token
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.erasure_coding import ErasureCodec
from vault_core.logging_config import logger
from vault_core.manifest import ObjectManifest, ReplicaState
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.metrics import (
    REPAIR_BACKLOG,
    REPAIR_DURATION_SECONDS,
    REPAIR_FAILURE_TOTAL,
    REPAIR_SUCCESS_TOTAL,
)


@dataclass(order=True)
class PrioritizedRepairTask:
    """
    Repair task prioritized by urgency.
    Lower priority value = processed FIRST.
    Priority is set to the count of surviving healthy replicas:
    - 0 surviving replicas: priority 0 (CRITICAL)
    - 1 surviving replica: priority 1 (HIGH urgency)
    - 2 surviving replicas: priority 2 (NORMAL urgency)
    """
    priority: int
    enqueued_at: float = field(compare=False)
    bucket: str = field(compare=False)
    key: str = field(compare=False)
    version_id: str = field(compare=False)
    chunk_index: int = field(compare=False)
    failed_node_id: str = field(compare=False)
    expected_sha256: str = field(compare=False)
    state: str = field(compare=False, default="corrupt")


class RepairWorker:
    """
    Autonomous repair worker that detects and repairs missing, stale, unreachable,
    and corrupt replicas across storage nodes.
    Features:
    - Rate-limited chunk migration to protect cluster network bandwidth.
    - Urgency-based prioritization: objects with the fewest surviving replicas are repaired first.
    - End-to-end cryptographic verification: checks source SHA-256 and destination SHA-256 before completion.
    - Integrated with Gateway read-repair and scheduled cluster audits.
    """

    def __init__(
        self,
        cluster: ClusterConfig,
        metadata_raft: RaftMetadataStateMachine,
        http_client: httpx.AsyncClient,
        rate_limit_per_second: float = 20.0,
        secret_key: str = "vault-insecure-secret-key-change-in-production",
    ) -> None:
        self.cluster = cluster
        self.metadata_raft = metadata_raft
        self.http_client = http_client
        self.rate_limit_per_second = rate_limit_per_second
        self.secret_key = secret_key

        self._queue: asyncio.PriorityQueue[PrioritizedRepairTask] = asyncio.PriorityQueue()
        self._queued_signatures: Set[Tuple[str, str, str, int, str]] = set()
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None

    def queue_size(self) -> int:
        """Return the current number of pending repair tasks in the queue."""
        return self._queue.qsize()

    def enqueue_repair(
        self,
        bucket: str,
        key: str,
        version_id: str,
        chunk_index: int,
        failed_node_id: str,
        expected_sha256: str,
        surviving_healthy_replicas: int = 1,
        state: str = "corrupt",
    ) -> bool:
        """
        Enqueue a degraded chunk replica for repair.
        Deduplicates against currently queued tasks.
        """
        sig = (bucket, key, version_id, chunk_index, failed_node_id)
        if sig in self._queued_signatures:
            return False

        task = PrioritizedRepairTask(
            priority=surviving_healthy_replicas,
            enqueued_at=time.time(),
            bucket=bucket,
            key=key,
            version_id=version_id,
            chunk_index=chunk_index,
            failed_node_id=failed_node_id,
            expected_sha256=expected_sha256,
            state=state,
        )
        self._queued_signatures.add(sig)
        self._queue.put_nowait(task)
        REPAIR_BACKLOG.set(self._queue.qsize())
        logger.info(
            f"Enqueued repair task for {bucket}/{key} chunk {chunk_index} on {failed_node_id} "
            f"(state={state}, surviving={surviving_healthy_replicas}, backlog={self._queue.qsize()})"
        )
        return True

    async def repair_single_task(self, task: PrioritizedRepairTask) -> bool:
        """
        Execute repair of a single chunk replica:
        1. Verify task is still relevant against latest committed Raft manifest.
        2. Identify and fetch chunk data from a healthy source node.
        3. Verify source SHA-256 against committed manifest.
        4. Upload chunk to target destination node.
        5. Verify destination SHA-256 matches expected checksum.
        """
        start_time = time.time()
        sig = (task.bucket, task.key, task.version_id, task.chunk_index, task.failed_node_id)

        try:
            # 1. Check committed manifest ground truth
            manifest_dict = self.metadata_raft.get_latest_manifest(task.bucket, task.key)
            if not manifest_dict or manifest_dict.get("is_tombstone"):
                logger.info(f"Skipping repair for {task.bucket}/{task.key}: object deleted or tombstoned")
                return True

            if manifest_dict.get("version_id") != task.version_id:
                logger.info(f"Skipping repair for {task.bucket}/{task.key}: superseded by newer version")
                return True

            manifest = ObjectManifest.model_validate(manifest_dict)
            if not manifest.chunks:
                return False

            auth_token = create_auth_token(self.secret_key, node_id="repair-worker")
            source_data: Optional[bytes] = None

            # Handle Erasure-Coded Object Repair
            if manifest.policy == "archive" or any(c.is_erasure_coded for c in manifest.chunks):
                # For archive policy, task.chunk_index is the fragment index (0..5)
                chunk_meta = manifest.chunks[0]
                fragments_info = chunk_meta.fragments or []
                target_frag = next((f for f in fragments_info if f.fragment_index == task.chunk_index), None)
                if not target_frag:
                    return False

                expected_sha = target_frag.sha256
                target_node = self.cluster.get_storage_node(task.failed_node_id)
                if not target_node or not target_node.active:
                    target_node = self.cluster.get_storage_node(target_frag.node_id)
                if not target_node:
                    return False

                # Gather surviving fragments (need at least K=4)
                surviving_frags: Dict[int, bytes] = {}
                for f_meta in fragments_info:
                    if f_meta.fragment_index == task.chunk_index:
                        continue
                    src_node = self.cluster.get_storage_node(f_meta.node_id)
                    if not src_node or not src_node.active:
                        continue

                    get_url = f"{src_node.url}/v1/chunks/{task.bucket}/{task.version_id}/{f_meta.fragment_index}"
                    try:
                        resp = await self.http_client.get(get_url, headers={"X-Vault-Auth-Token": auth_token})
                        if resp.status_code == 200:
                            f_data = resp.content
                            if hashlib.sha256(f_data).hexdigest() == f_meta.sha256:
                                surviving_frags[f_meta.fragment_index] = f_data
                                if len(surviving_frags) >= 4:
                                    break
                    except Exception:
                        pass

                if len(surviving_frags) < 4:
                    logger.error(f"Cannot repair EC fragment {task.chunk_index}: only {len(surviving_frags)} valid fragments")
                    REPAIR_FAILURE_TOTAL.inc()
                    return False

                codec = ErasureCodec(4, 2)
                orig_size = chunk_meta.original_chunk_size or chunk_meta.size_bytes
                reconstructed_frag, frag_sha = codec.reconstruct_fragment(
                    surviving_fragments=surviving_frags,
                    original_size=orig_size,
                    target_fragment_index=task.chunk_index,
                )
                if frag_sha != expected_sha:
                    logger.error(f"Regenerated EC fragment hash mismatch: expected {expected_sha}, got {frag_sha}")
                    REPAIR_FAILURE_TOTAL.inc()
                    return False
                source_data = reconstructed_frag

            else:
                # Handle Replicated Object Repair
                if task.chunk_index >= len(manifest.chunks):
                    return False
                chunk_meta = manifest.chunks[task.chunk_index]
                expected_sha = chunk_meta.sha256

                target_node = self.cluster.get_storage_node(task.failed_node_id)
                if not target_node or not target_node.active:
                    active_nodes = self.cluster.get_active_storage_nodes()
                    used_nodes = set(chunk_meta.placement_nodes)
                    candidate_alts = [n for n in active_nodes if n.node_id not in used_nodes]
                    if not candidate_alts:
                        logger.error(f"Cannot repair {task.bucket}/{task.key}: no available alternative nodes")
                        REPAIR_FAILURE_TOTAL.inc()
                        return False
                    target_node = candidate_alts[0]

                for node_id in chunk_meta.placement_nodes:
                    if node_id == task.failed_node_id:
                        continue
                    node = self.cluster.get_storage_node(node_id)
                    if not node or not node.active:
                        continue

                    get_url = f"{node.url}/v1/chunks/{task.bucket}/{task.version_id}/{task.chunk_index}"
                    try:
                        resp = await self.http_client.get(get_url, headers={"X-Vault-Auth-Token": auth_token})
                        if resp.status_code == 200:
                            downloaded = resp.content
                            if hashlib.sha256(downloaded).hexdigest() == expected_sha:
                                source_data = downloaded
                                break
                    except Exception:
                        pass

                if source_data is None:
                    logger.error(f"Repair impossible for {task.bucket}/{task.key} chunk {task.chunk_index}: no healthy source")
                    REPAIR_FAILURE_TOTAL.inc()
                    return False

            # 4. Upload verified chunk to target node
            put_url = f"{target_node.url}/v1/chunks/{task.bucket}/{task.version_id}/{task.chunk_index}"
            put_resp = await self.http_client.put(
                put_url,
                content=source_data,
                headers={
                    "X-Vault-Auth-Token": auth_token,
                    "X-Expected-SHA256": expected_sha,
                }
            )

            if put_resp.status_code not in (200, 201):
                logger.error(f"Target node {target_node.node_id} rejected repaired chunk: HTTP {put_resp.status_code}")
                REPAIR_FAILURE_TOTAL.inc()
                return False

            # 5. Post-repair verification: verify destination hash
            head_url = f"{target_node.url}/v1/chunks/{task.bucket}/{task.version_id}/{task.chunk_index}"
            head_resp = await self.http_client.head(head_url, headers={"X-Vault-Auth-Token": auth_token})
            dest_sha = head_resp.headers.get("x-chunk-sha256")

            if dest_sha and dest_sha != expected_sha:
                logger.error(f"Post-repair verification failed on {target_node.node_id}: expected {expected_sha}, got {dest_sha}")
                REPAIR_FAILURE_TOTAL.inc()
                return False

            elapsed = time.time() - start_time
            REPAIR_SUCCESS_TOTAL.inc()
            REPAIR_DURATION_SECONDS.observe(elapsed)
            logger.info(
                f"Successfully repaired {task.bucket}/{task.key} chunk {task.chunk_index} "
                f"on {target_node.node_id} ({elapsed:.3f}s)"
            )
            return True

        except Exception as exc:
            logger.error(f"Unexpected error during repair of {task.bucket}/{task.key}: {exc}")
            REPAIR_FAILURE_TOTAL.inc()
            return False
        finally:
            self._queued_signatures.discard(sig)
            REPAIR_BACKLOG.set(self._queue.qsize())

    async def run_repair_cycle(self, max_tasks: Optional[int] = None) -> int:
        """Process tasks from priority queue with rate-limiting."""
        processed = 0
        delay = 1.0 / max(1.0, self.rate_limit_per_second)

        while not self._queue.empty():
            if max_tasks is not None and processed >= max_tasks:
                break

            task = await self._queue.get()
            await self.repair_single_task(task)
            self._queue.task_done()
            processed += 1
            await asyncio.sleep(delay)

        return processed

    repair_all = run_repair_cycle

    async def scan_and_enqueue_degraded_objects(self) -> int:
        """
        Audit all active objects in Raft metadata cluster against physical storage nodes.
        Enqueues repairs for missing, stale, corrupt, or unreachable replicas,
        prioritizing objects with the lowest surviving replica counts.
        """
        enqueued_count = 0
        all_objects = getattr(self.metadata_raft, "_current_versions", {})

        for obj_key, curr in list(all_objects.items()):
            if curr.get("is_tombstone"):
                continue

            parts = obj_key.split("/", 1)
            if len(parts) != 2:
                continue
            bucket, key = parts[0], parts[1]

            manifest_dict = self.metadata_raft.get_latest_manifest(bucket, key)
            if not manifest_dict:
                continue

            manifest = ObjectManifest.model_validate(manifest_dict)
            auth_token = create_auth_token(self.secret_key, node_id="repair-worker")

            for chunk_meta in manifest.chunks:
                healthy_nodes = []
                degraded_nodes = []

                if chunk_meta.is_erasure_coded:
                    fragments_info = chunk_meta.fragments or []
                    for f_info in fragments_info:
                        node = self.cluster.get_storage_node(f_info.node_id)
                        if not node or not node.active:
                            degraded_nodes.append((f_info.fragment_index, f_info.node_id, f_info.sha256, "unreachable"))
                            continue

                        url = f"{node.url}/v1/chunks/{bucket}/{manifest.version_id}/{f_info.fragment_index}"
                        try:
                            resp = await self.http_client.get(url, headers={"X-Vault-Auth-Token": auth_token})
                            if resp.status_code == 200:
                                actual_sha = hashlib.sha256(resp.content).hexdigest()
                                if actual_sha == f_info.sha256:
                                    healthy_nodes.append(f_info.node_id)
                                else:
                                    degraded_nodes.append((f_info.fragment_index, f_info.node_id, f_info.sha256, "corrupt"))
                            elif resp.status_code == 404:
                                degraded_nodes.append((f_info.fragment_index, f_info.node_id, f_info.sha256, "missing"))
                            elif resp.status_code == 410:
                                degraded_nodes.append((f_info.fragment_index, f_info.node_id, f_info.sha256, "corrupt"))
                            else:
                                degraded_nodes.append((f_info.fragment_index, f_info.node_id, f_info.sha256, "unreachable"))
                        except Exception:
                            degraded_nodes.append((f_info.fragment_index, f_info.node_id, f_info.sha256, "unreachable"))

                    surviving_count = len(healthy_nodes)
                    for f_idx, node_id, f_sha, state in degraded_nodes:
                        enqueued = self.enqueue_repair(
                            bucket=bucket,
                            key=key,
                            version_id=manifest.version_id,
                            chunk_index=f_idx,
                            failed_node_id=node_id,
                            expected_sha256=f_sha,
                            surviving_healthy_replicas=surviving_count,
                            state=state,
                        )
                        if enqueued:
                            enqueued_count += 1

                else:
                    for node_id in chunk_meta.placement_nodes:
                        node = self.cluster.get_storage_node(node_id)
                        if not node or not node.active:
                            degraded_nodes.append((chunk_meta.chunk_index, node_id, chunk_meta.sha256, "unreachable"))
                            continue

                        url = f"{node.url}/v1/chunks/{bucket}/{manifest.version_id}/{chunk_meta.chunk_index}"
                        try:
                            resp = await self.http_client.get(url, headers={"X-Vault-Auth-Token": auth_token})
                            if resp.status_code == 200:
                                actual_sha = hashlib.sha256(resp.content).hexdigest()
                                if actual_sha == chunk_meta.sha256:
                                    healthy_nodes.append(node_id)
                                else:
                                    degraded_nodes.append((chunk_meta.chunk_index, node_id, chunk_meta.sha256, "corrupt"))
                            elif resp.status_code == 404:
                                degraded_nodes.append((chunk_meta.chunk_index, node_id, chunk_meta.sha256, "missing"))
                            elif resp.status_code == 410:
                                degraded_nodes.append((chunk_meta.chunk_index, node_id, chunk_meta.sha256, "corrupt"))
                            else:
                                degraded_nodes.append((chunk_meta.chunk_index, node_id, chunk_meta.sha256, "unreachable"))
                        except Exception:
                            degraded_nodes.append((chunk_meta.chunk_index, node_id, chunk_meta.sha256, "unreachable"))

                    surviving_count = len(healthy_nodes)
                    for c_idx, node_id, c_sha, state in degraded_nodes:
                        enqueued = self.enqueue_repair(
                            bucket=bucket,
                            key=key,
                            version_id=manifest.version_id,
                            chunk_index=c_idx,
                            failed_node_id=node_id,
                            expected_sha256=c_sha,
                            surviving_healthy_replicas=surviving_count,
                            state=state,
                        )
                        if enqueued:
                            enqueued_count += 1

        return enqueued_count

    async def _worker_loop(self) -> None:
        """Background continuous worker loop."""
        while self._running:
            try:
                # Process any pending repairs
                if not self._queue.empty():
                    await self.run_repair_cycle(max_tasks=10)
                else:
                    await asyncio.sleep(0.5)
            except Exception as exc:
                logger.error(f"Error in repair worker loop: {exc}")
                await asyncio.sleep(1.0)

    def start(self) -> None:
        """Start the background repair worker task."""
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker_loop())
            logger.info("RepairWorker background task started")

    def stop(self) -> None:
        """Stop the background repair worker task."""
        self._running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            logger.info("RepairWorker background task stopped")
