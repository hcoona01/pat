import asyncio
from contextlib import asynccontextmanager
import hashlib
import time
import uuid
from typing import AsyncGenerator, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response as PlainResponse, StreamingResponse
import httpx

from apps.workers.rebalance_worker import RebalanceWorker
from apps.workers.repair_worker import RepairWorker
from vault_core.auth import create_auth_token
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.erasure_coding import ErasureCodec, InsufficientFragmentsError
from vault_core.logging_config import StructuredLoggingMiddleware, logger
from vault_core.rebalance import RebalanceStatus
from vault_core.manifest import (
    ChunkInfo,
    FragmentInfo,
    ObjectManifest,
    ReplicaAudit,
    ReplicaAuditReport,
    ReplicaState,
)
from vault_core.metadata_raft import (
    CASConflictError,
    ManifestNotFoundError,
    NotLeaderError,
    RaftMetadataStateMachine,
    RaftQuorumError,
)
from vault_core.metrics import (
    HTTP_REQUESTS_TOTAL,
    IDEMPOTENT_HITS_TOTAL,
    get_latest_metrics,
)
from vault_core.placement import select_placement_nodes
from vault_core.quorum import (
    DurabilityPolicy,
    PolicyScheme,
    PolicyValidationError,
    load_policies_from_yaml,
)
from vault_core.settings import VaultSettings, settings


class GatewayService:
    """Orchestrates multi-node streaming writes/reads, quorums, and metadata consensus."""

    def __init__(
        self,
        cluster: ClusterConfig,
        policies: Dict[str, DurabilityPolicy],
        metadata_raft: Optional[RaftMetadataStateMachine] = None,
        custom_settings: Optional[VaultSettings] = None,
        storage_http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.cluster = cluster
        self.policies = policies
        self.metadata_raft = metadata_raft
        self.settings = custom_settings or settings
        self._http_client = storage_http_client
        self.orphan_candidates: List[dict] = []
        self.repair_backlog: List[dict] = []

        if self.metadata_raft:
            self.repair_worker: Optional[RepairWorker] = RepairWorker(
                cluster=self.cluster,
                metadata_raft=self.metadata_raft,
                http_client=self.get_http_client(),
                secret_key=self.settings.secret_key,
            )
            self.rebalance_worker: Optional[RebalanceWorker] = RebalanceWorker(
                cluster=self.cluster,
                metadata_raft=self.metadata_raft,
                policies=self.policies,
                http_client=self.get_http_client(),
                secret_key=self.settings.secret_key,
            )
            self.sync_cluster_membership()
        else:
            self.repair_worker = None
            self.rebalance_worker = None

    def sync_cluster_membership(self) -> None:
        """Synchronize gateway cluster configuration with Raft-committed membership."""
        if not self.metadata_raft:
            return
        raft_nodes = self.metadata_raft.get_all_storage_nodes()
        if raft_nodes:
            self.cluster.storage_nodes = [
                StorageNodeConfig.model_validate(n) for n in raft_nodes.values()
            ]
        elif self.metadata_raft.is_leader():
            # Seed Raft membership with existing cluster configuration if empty
            self.metadata_raft.seed_initial_membership(
                [n.model_dump() for n in self.cluster.storage_nodes]
            )

    def get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0),
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )
        return self._http_client

    async def upload_chunk_to_node(
        self,
        node: StorageNodeConfig,
        bucket: str,
        version_id: str,
        chunk_index: int,
        data: bytes,
        expected_sha256: str,
    ) -> Optional[dict]:
        """Upload a single chunk to a specific storage node via HTTP."""
        client = self.get_http_client()
        url = f"{node.url}/v1/chunks/{bucket}/{version_id}/{chunk_index}"
        auth_token = create_auth_token(self.settings.secret_key, node_id="gateway")

        try:
            resp = await client.put(
                url,
                content=data,
                headers={
                    "X-Expected-SHA256": expected_sha256,
                    "X-Vault-Auth-Token": auth_token,
                    "Content-Type": "application/octet-stream",
                }
            )
            if resp.status_code in (200, 201):
                body = resp.json()
                if body.get("sha256") == expected_sha256:
                    return {"node_id": node.node_id, "sha256": body.get("sha256")}
            logger.warning(f"Upload to node {node.node_id} returned status {resp.status_code}: {resp.text}")
        except Exception as exc:
            logger.warning(f"Failed to upload chunk {chunk_index} to node {node.node_id} ({url}): {exc}")

        return None

    async def fetch_chunk_from_node(
        self,
        node: StorageNodeConfig,
        bucket: str,
        version_id: str,
        chunk_index: int,
        expected_sha256: str,
    ) -> Optional[bytes]:
        """Download chunk bytes from a storage node and verify SHA-256."""
        client = self.get_http_client()
        url = f"{node.url}/v1/chunks/{bucket}/{version_id}/{chunk_index}"
        auth_token = create_auth_token(self.settings.secret_key, node_id="gateway")

        try:
            resp = await client.get(
                url,
                headers={
                    "X-Expected-SHA256": expected_sha256,
                    "X-Vault-Auth-Token": auth_token,
                }
            )
            if resp.status_code == 200:
                data = resp.content
                actual_sha = hashlib.sha256(data).hexdigest()
                if actual_sha == expected_sha256:
                    return data
                logger.error(f"Node {node.node_id} served corrupted chunk {chunk_index} (mismatched hash)")
            elif resp.status_code == 410:
                logger.error(f"Node {node.node_id} reported chunk {chunk_index} quarantined/corrupt")
        except Exception as exc:
            logger.warning(f"Error reading chunk {chunk_index} from node {node.node_id}: {exc}")

        return None


def create_gateway_app(gateway_service: GatewayService) -> FastAPI:
    """Create FastAPI application for the distributed API Gateway."""
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if gateway_service.repair_worker:
            gateway_service.repair_worker.start()
        yield
        if gateway_service.repair_worker:
            gateway_service.repair_worker.stop()

    app = FastAPI(
        title="Vault Object Storage Gateway",
        version="0.1.0",
        description="Distributed API Gateway with Raft metadata consensus and multi-node placement",
        lifespan=lifespan,
    )

    app.add_middleware(StructuredLoggingMiddleware)
    app.state.gateway = gateway_service

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        """Prometheus metrics endpoint."""
        body, content_type = get_latest_metrics()
        return Response(content=body, media_type=content_type)

    @app.post("/v1/admin/repair/scan")
    async def trigger_repair_scan() -> dict:
        """Scan all active objects in Raft and enqueue any missing/corrupt/unreachable replicas."""
        gw: GatewayService = app.state.gateway
        if not gw.repair_worker:
            raise HTTPException(status_code=503, detail="Repair worker uninitialized")
        count = await gw.repair_worker.scan_and_enqueue_degraded_objects()
        return {"enqueued_repairs": count, "backlog_size": gw.repair_worker._queue.qsize()}

    @app.post("/v1/admin/repair/run")
    async def run_repair_cycle(max_tasks: Optional[int] = None) -> dict:
        """Execute pending repairs from the priority queue."""
        gw: GatewayService = app.state.gateway
        if not gw.repair_worker:
            raise HTTPException(status_code=503, detail="Repair worker uninitialized")
        processed = await gw.repair_worker.run_repair_cycle(max_tasks=max_tasks)
        return {"processed_repairs": processed, "remaining_backlog": gw.repair_worker._queue.qsize()}

    @app.get("/v1/admin/repair/status")
    async def repair_status() -> dict:
        """Inspect repair worker backlog and status."""
        gw: GatewayService = app.state.gateway
        backlog = gw.repair_worker._queue.qsize() if gw.repair_worker else 0
        return {
            "backlog_size": backlog,
            "running": gw.repair_worker._running if gw.repair_worker else False,
        }

    # =========================================================================
    # ADMIN STORAGE MEMBERSHIP & REBALANCE APIS
    # =========================================================================

    @app.get("/v1/admin/nodes")
    async def list_nodes() -> dict:
        """List all storage nodes registered in the cluster."""
        gw: GatewayService = app.state.gateway
        gw.sync_cluster_membership()
        return {
            "cluster_id": gw.cluster.cluster_id,
            "total_nodes": len(gw.cluster.storage_nodes),
            "active_nodes": len(gw.cluster.get_active_storage_nodes()),
            "distinct_zones": list(gw.cluster.distinct_active_zones()),
            "nodes": [n.model_dump() for n in gw.cluster.storage_nodes],
        }

    @app.post("/v1/admin/nodes", status_code=status.HTTP_201_CREATED)
    async def add_node(node_data: StorageNodeConfig) -> dict:
        """Add and register a new storage node, committing membership change via Raft."""
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        if not node_data.node_id or not node_data.url or not node_data.zone:
            raise HTTPException(status_code=400, detail="node_id, url, and zone are required")

        try:
            gw.metadata_raft.add_storage_node(node_data.model_dump())
        except NotLeaderError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to commit node addition: {exc}")

        gw.cluster.add_storage_node(node_data)
        logger.info(f"Admin added storage node: {node_data.node_id} ({node_data.url}, zone={node_data.zone})")
        return node_data.model_dump()

    @app.post("/v1/admin/nodes/{node_id}/activate")
    async def activate_node(node_id: str) -> dict:
        """Activate a storage node for traffic, committing change via Raft."""
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        node = gw.cluster.get_storage_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail=f"Storage node {node_id} not found")

        try:
            gw.metadata_raft.update_storage_node(node_id, {"active": True})
        except NotLeaderError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to commit node activation: {exc}")

        gw.cluster.update_storage_node(node_id, active=True)
        return {"node_id": node_id, "active": True, "status": "activated"}

    @app.post("/v1/admin/nodes/{node_id}/deactivate")
    async def deactivate_node(node_id: str) -> dict:
        """Deactivate a storage node, committing change via Raft."""
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        node = gw.cluster.get_storage_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail=f"Storage node {node_id} not found")

        try:
            gw.metadata_raft.update_storage_node(node_id, {"active": False})
        except NotLeaderError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to commit node deactivation: {exc}")

        gw.cluster.update_storage_node(node_id, active=False)
        return {"node_id": node_id, "active": False, "status": "deactivated"}

    @app.delete("/v1/admin/nodes/{node_id}")
    async def remove_node(node_id: str, force: bool = False) -> dict:
        """
        Safely remove a storage node from cluster membership via Raft.
        Validates that removing the node does not leave the cluster with fewer than 3 active zones.
        """
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        node = gw.cluster.get_storage_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail=f"Storage node {node_id} not found")

        if not force:
            remaining_active = [n for n in gw.cluster.get_active_storage_nodes() if n.node_id != node_id]
            remaining_zones = {n.zone for n in remaining_active}
            if len(remaining_zones) < 3 and len(gw.cluster.get_active_storage_nodes()) > 3:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot remove node {node_id}: would leave cluster with only {len(remaining_zones)} zones (minimum 3 required)"
                )

        try:
            gw.metadata_raft.remove_storage_node(node_id)
        except NotLeaderError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to commit node removal: {exc}")

        gw.cluster.remove_storage_node(node_id)
        return {"node_id": node_id, "status": "removed"}

    @app.post("/v1/admin/rebalance", status_code=status.HTTP_202_ACCEPTED)
    async def trigger_rebalance(rate_limit: Optional[float] = None) -> dict:
        """Trigger background rebalancing to align data placement with current topology."""
        gw: GatewayService = app.state.gateway
        if not gw.rebalance_worker:
            raise HTTPException(status_code=503, detail="Rebalance worker unavailable")

        status_obj = await gw.rebalance_worker.trigger_rebalance(rate_limit=rate_limit)
        return status_obj.model_dump()

    @app.get("/v1/admin/rebalance")
    @app.get("/v1/admin/rebalance/status")
    async def get_rebalance_status() -> dict:
        """Retrieve progress and completion status of background rebalancing."""
        gw: GatewayService = app.state.gateway
        if not gw.rebalance_worker:
            return RebalanceStatus().model_dump()
        return gw.rebalance_worker.get_status().model_dump()

    @app.get("/v1/cluster/health")
    async def cluster_health() -> dict:
        """Return end-to-end cluster health across metadata and storage nodes."""
        gw = app.state.gateway
        meta_health = gw.metadata_raft.get_cluster_health() if gw.metadata_raft else {}

        active_nodes = gw.cluster.get_active_storage_nodes()
        distinct_zones = gw.cluster.distinct_active_zones()
        distinct_regions = gw.cluster.distinct_active_regions()

        return {
            "cluster_id": gw.cluster.cluster_id,
            "status": "healthy" if len(active_nodes) >= 3 else "degraded",
            "metadata_raft": meta_health,
            "active_storage_nodes": len(active_nodes),
            "total_storage_nodes": len(gw.cluster.storage_nodes),
            "distinct_zones": list(distinct_zones),
            "distinct_regions": list(distinct_regions),
            "orphan_candidates_count": len(gw.orphan_candidates),
            "repair_backlog_count": len(gw.repair_backlog),
        }

    @app.get("/v1/objects/{bucket}/{key:path}/health")
    async def object_health(bucket: str, key: str) -> dict:
        """Inspect durability and replica health for a specific object."""
        gw = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        manifest_dict = gw.metadata_raft.get_latest_manifest(bucket, key)
        if not manifest_dict:
            raise HTTPException(status_code=404, detail="Object not found or tombstoned")

        manifest = ObjectManifest.model_validate(manifest_dict)
        policy = gw.policies.get(manifest.policy)

        chunk_statuses = []
        for c in manifest.chunks:
            chunk_statuses.append({
                "chunk_index": c.chunk_index,
                "sha256": c.sha256,
                "placement_nodes": c.placement_nodes,
                "target_replicas": len(c.placement_nodes),
            })

        return {
            "bucket": bucket,
            "key": key,
            "version_id": manifest.version_id,
            "logical_version": manifest.logical_version,
            "policy": manifest.policy,
            "storage_amplification": 1.5 if manifest.policy == "archive" else (4.0 if manifest.policy == "durable" else 3.0),
            "size_bytes": manifest.size_bytes,
            "content_hash": manifest.content_hash,
            "chunks": chunk_statuses,
        }

    @app.get("/v1/objects/{bucket}/{key:path}/audit")
    async def audit_object(bucket: str, key: str) -> dict:
        """
        Audit physical replica inventory across storage nodes against the latest committed Raft manifest.
        Detects HEALTHY, STALE, CORRUPT, MISSING, and UNREACHABLE states.
        Supports both replicated and erasure-coded policies.
        """
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        manifest_dict = gw.metadata_raft.get_latest_manifest(bucket, key)
        if not manifest_dict:
            raise HTTPException(status_code=404, detail="Object not found or tombstoned")

        manifest = ObjectManifest.model_validate(manifest_dict)
        audits: List[ReplicaAudit] = []

        for chunk_meta in manifest.chunks:
            if chunk_meta.is_erasure_coded and chunk_meta.fragments:
                targets = [
                    (f.node_id, f.fragment_index, f.sha256)
                    for f in chunk_meta.fragments
                ]
            else:
                targets = [
                    (node_id, chunk_meta.chunk_index, chunk_meta.sha256)
                    for node_id in chunk_meta.placement_nodes
                ]

            for node_id, c_idx, exp_sha in targets:
                node = gw.cluster.get_storage_node(node_id)
                if not node or not node.active:
                    audits.append(ReplicaAudit(
                        node_id=node_id,
                        chunk_index=c_idx,
                        expected_version_id=manifest.version_id,
                        expected_sha256=exp_sha,
                        state=ReplicaState.UNREACHABLE,
                        details="Node not registered or marked inactive"
                    ))
                    continue

                client = gw.get_http_client()
                url = f"{node.url}/v1/chunks/{bucket}/{manifest.version_id}/{c_idx}"
                auth_token = create_auth_token(gw.settings.secret_key, node_id="gateway")

                try:
                    resp = await client.get(url, headers={"X-Vault-Auth-Token": auth_token})
                    if resp.status_code == 200:
                        actual_sha = hashlib.sha256(resp.content).hexdigest()
                        if actual_sha == exp_sha:
                            audits.append(ReplicaAudit(
                                node_id=node_id,
                                chunk_index=c_idx,
                                expected_version_id=manifest.version_id,
                                expected_sha256=exp_sha,
                                state=ReplicaState.HEALTHY,
                                actual_sha256=actual_sha,
                            ))
                        else:
                            audits.append(ReplicaAudit(
                                node_id=node_id,
                                chunk_index=c_idx,
                                expected_version_id=manifest.version_id,
                                expected_sha256=exp_sha,
                                state=ReplicaState.CORRUPT,
                                actual_sha256=actual_sha,
                                details="Checksum mismatch on disk"
                            ))
                    elif resp.status_code == 404:
                        all_versions = gw.metadata_raft.get_all_versions(bucket, key)
                        older_versions = [
                            v for v in all_versions
                            if v.get("version_id") != manifest.version_id and not v.get("is_tombstone")
                        ]
                        has_stale = False
                        for old_v in older_versions:
                            old_vid = old_v.get("version_id")
                            old_url = f"{node.url}/v1/chunks/{bucket}/{old_vid}/{c_idx}"
                            try:
                                old_resp = await client.head(old_url, headers={"X-Vault-Auth-Token": auth_token})
                                if old_resp.status_code == 200:
                                    audits.append(ReplicaAudit(
                                        node_id=node_id,
                                        chunk_index=c_idx,
                                        expected_version_id=manifest.version_id,
                                        expected_sha256=exp_sha,
                                        state=ReplicaState.STALE,
                                        details=f"Node holds superseded version {old_vid} instead of {manifest.version_id}"
                                    ))
                                    has_stale = True
                                    break
                            except Exception:
                                pass

                        if not has_stale:
                            audits.append(ReplicaAudit(
                                node_id=node_id,
                                chunk_index=c_idx,
                                expected_version_id=manifest.version_id,
                                expected_sha256=exp_sha,
                                state=ReplicaState.MISSING,
                                details="Chunk not found on node"
                            ))
                    elif resp.status_code == 410:
                        audits.append(ReplicaAudit(
                            node_id=node_id,
                            chunk_index=c_idx,
                            expected_version_id=manifest.version_id,
                            expected_sha256=exp_sha,
                            state=ReplicaState.CORRUPT,
                            details="Node reported chunk quarantined"
                        ))
                    else:
                        audits.append(ReplicaAudit(
                            node_id=node_id,
                            chunk_index=c_idx,
                            expected_version_id=manifest.version_id,
                            expected_sha256=exp_sha,
                            state=ReplicaState.UNREACHABLE,
                            details=f"Node returned HTTP {resp.status_code}"
                        ))
                except Exception as exc:
                    audits.append(ReplicaAudit(
                        node_id=node_id,
                        chunk_index=c_idx,
                        expected_version_id=manifest.version_id,
                        expected_sha256=exp_sha,
                        state=ReplicaState.UNREACHABLE,
                        details=str(exc)
                    ))

        report = ReplicaAuditReport(
            bucket=bucket,
            key=key,
            version_id=manifest.version_id,
            logical_version=manifest.logical_version,
            replicas=audits,
            healthy_count=sum(1 for a in audits if a.state == ReplicaState.HEALTHY),
            stale_count=sum(1 for a in audits if a.state == ReplicaState.STALE),
            corrupt_count=sum(1 for a in audits if a.state == ReplicaState.CORRUPT),
            missing_count=sum(1 for a in audits if a.state == ReplicaState.MISSING),
            unreachable_count=sum(1 for a in audits if a.state == ReplicaState.UNREACHABLE),
        )
        return report.model_dump()

    @app.put("/v1/objects/{bucket}/{key:path}", status_code=status.HTTP_201_CREATED)
    async def put_object(
        bucket: str,
        key: str,
        request: Request,
        policy_name: str = Header("hot", alias="X-Vault-Policy"),
        idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
        expected_version: Optional[int] = Header(None, alias="X-Expected-Version"),
        content_type: str = Header("application/octet-stream", alias="Content-Type"),
    ) -> Response:
        """
        Coordinated distributed write flow:
        1. Resolve policy and validate against active cluster nodes/zones.
        2. Stream incoming body into 8 MiB chunks.
        3. Concurrently upload each chunk to target placement nodes.
        4. Require configured data write quorum (W) before proceeding.
        5. Verify returned chunk hashes.
        6. Commit manifest through Raft metadata cluster only after all chunks meet data quorum.
        7. If quorum or commit fails, record orphan candidates for garbage collection.
        """
        gw: GatewayService = app.state.gateway

        idemp_key = idempotency_key or request.headers.get("Idempotency-Key")
        if not idemp_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required Idempotency-Key (or X-Idempotency-Key) header"
            )

        if policy_name not in gw.policies:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown durability policy '{policy_name}'. Available: {list(gw.policies.keys())}"
            )

        policy = gw.policies[policy_name]
        version_id = str(uuid.uuid4())
        chunk_limit = gw.settings.chunk_size_bytes

        # Stream ingestion buffer
        chunks_info: List[ChunkInfo] = []
        overall_hasher = hashlib.sha256()
        total_size = 0
        chunk_index = 0
        current_chunk_buffer = bytearray()
        all_written_chunks: List[dict] = []

        async def write_single_chunk(data_bytes: bytes, idx: int) -> ChunkInfo:
            chunk_sha = hashlib.sha256(data_bytes).hexdigest()

            # 1. Resolve deterministic placement
            try:
                target_nodes = select_placement_nodes(
                    bucket=bucket,
                    key=key,
                    version_id=version_id,
                    chunk_index=idx,
                    policy=policy,
                    nodes_or_cluster=gw.cluster,
                )
            except PolicyValidationError as exc:
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

            # Handle Reed-Solomon Erasure Coding (e.g. 4+2 archive policy)
            if policy.scheme == PolicyScheme.ERASURE_CODING:
                codec = ErasureCodec(
                    data_fragments=policy.data_fragments or 4,
                    parity_fragments=policy.parity_fragments or 2,
                )
                encoded_frags = codec.encode(data_bytes)
                fragments_info: List[FragmentInfo] = []
                upload_tasks = []

                for frag_idx, frag_bytes, frag_sha in encoded_frags:
                    assigned_node = target_nodes[frag_idx]
                    fragments_info.append(FragmentInfo(
                        fragment_index=frag_idx,
                        sha256=frag_sha,
                        size_bytes=len(frag_bytes),
                        node_id=assigned_node.node_id,
                        is_parity=(frag_idx >= codec.k),
                    ))
                    upload_tasks.append(
                        gw.upload_chunk_to_node(
                            node=assigned_node,
                            bucket=bucket,
                            version_id=version_id,
                            chunk_index=frag_idx,
                            data=frag_bytes,
                            expected_sha256=frag_sha,
                        )
                    )

                results = await asyncio.gather(*upload_tasks)
                successful_nodes = [res["node_id"] for res in results if res is not None]

                required_quorum = policy.write_quorum
                if len(successful_nodes) < required_quorum:
                    for n_id in successful_nodes:
                        gw.orphan_candidates.append({
                            "node_id": n_id,
                            "bucket": bucket,
                            "version_id": version_id,
                            "chunk_index": idx,
                            "created_at": time.time(),
                        })
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=(
                            f"Archive erasure coding write quorum unavailable for chunk {idx}: "
                            f"required {required_quorum} fragments, received {len(successful_nodes)} "
                            f"(targets: {[n.node_id for n in target_nodes]})"
                        )
                    )

                for n_id in successful_nodes:
                    all_written_chunks.append({
                        "node_id": n_id,
                        "bucket": bucket,
                        "version_id": version_id,
                        "chunk_index": idx,
                    })

                return ChunkInfo(
                    chunk_index=idx,
                    chunk_id=f"{version_id}_{idx}",
                    sha256=chunk_sha,
                    size_bytes=len(data_bytes),
                    placement_nodes=[n.node_id for n in target_nodes],
                    is_erasure_coded=True,
                    fragments=fragments_info,
                    original_chunk_size=len(data_bytes),
                )

            # 2. Concurrently upload to target nodes (Replication policy)
            upload_tasks = [
                gw.upload_chunk_to_node(
                    node=node,
                    bucket=bucket,
                    version_id=version_id,
                    chunk_index=idx,
                    data=data_bytes,
                    expected_sha256=chunk_sha,
                )
                for node in target_nodes
            ]

            results = await asyncio.gather(*upload_tasks)
            successful_nodes = [res["node_id"] for res in results if res is not None]

            # 3. Enforce data write quorum
            required_quorum = policy.write_quorum
            if len(successful_nodes) < required_quorum:
                # Mark successful writes as orphans for future GC sweep
                for n_id in successful_nodes:
                    gw.orphan_candidates.append({
                        "node_id": n_id,
                        "bucket": bucket,
                        "version_id": version_id,
                        "chunk_index": idx,
                        "created_at": time.time(),
                    })
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=(
                        f"Data write quorum unavailable for chunk {idx}: "
                        f"required {required_quorum} acknowledgments, received {len(successful_nodes)} "
                        f"(targets: {[n.node_id for n in target_nodes]})"
                    )
                )

            # Record successfully written chunk
            for n_id in successful_nodes:
                all_written_chunks.append({
                    "node_id": n_id,
                    "bucket": bucket,
                    "version_id": version_id,
                    "chunk_index": idx,
                })

            return ChunkInfo(
                chunk_index=idx,
                chunk_id=f"{version_id}_{idx}",
                sha256=chunk_sha,
                size_bytes=len(data_bytes),
                placement_nodes=successful_nodes,
            )

        # Stream request body
        try:
            async for byte_chunk in request.stream():
                if not byte_chunk:
                    continue

                current_chunk_buffer.extend(byte_chunk)
                overall_hasher.update(byte_chunk)
                total_size += len(byte_chunk)

                while len(current_chunk_buffer) >= chunk_limit:
                    slice_data = bytes(current_chunk_buffer[:chunk_limit])
                    current_chunk_buffer = current_chunk_buffer[chunk_limit:]

                    c_info = await write_single_chunk(slice_data, chunk_index)
                    chunks_info.append(c_info)
                    chunk_index += 1

            # Final leftover chunk
            if len(current_chunk_buffer) > 0 or chunk_index == 0:
                final_data = bytes(current_chunk_buffer)
                c_info = await write_single_chunk(final_data, chunk_index)
                chunks_info.append(c_info)
        except HTTPException:
            # Re-raise explicit HTTP exceptions (e.g. 503 Quorum Unavailable)
            raise
        except Exception as exc:
            # Mark all written chunks as orphans on unexpected failure
            for item in all_written_chunks:
                gw.orphan_candidates.append({**item, "created_at": time.time()})
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

        # 4. Prepare manifest
        content_hash = overall_hasher.hexdigest()
        manifest = ObjectManifest(
            bucket=bucket,
            key=key,
            version_id=version_id,
            size_bytes=total_size,
            content_hash=content_hash,
            content_type=content_type,
            policy=policy_name,
            chunks=chunks_info,
            is_tombstone=False,
            idempotency_key=idemp_key,
            created_at=time.time(),
        )

        manifest_dict = manifest.model_dump()
        response_payload = manifest.to_summary_dict()

        # 5. Commit through Raft Metadata Consensus
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Raft metadata consensus unavailable")

        try:
            res = gw.metadata_raft.commit_manifest(
                manifest_dict=manifest_dict,
                expected_version=expected_version,
                idempotency_key=idemp_key,
                response_payload=response_payload,
            )
        except CASConflictError as exc:
            # Mark data as orphan since commit was rejected
            for item in all_written_chunks:
                gw.orphan_candidates.append({**item, "created_at": time.time()})
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
        except (NotLeaderError, RaftQuorumError) as exc:
            for item in all_written_chunks:
                gw.orphan_candidates.append({**item, "created_at": time.time()})
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
        except Exception as exc:
            for item in all_written_chunks:
                gw.orphan_candidates.append({**item, "created_at": time.time()})
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Raft commit error: {exc}")

        logical_version = res.get("logical_version", 1)

        # If duplicate idempotency hit, return HTTP 200 OK
        if res.get("idempotent_hit"):
            IDEMPOTENT_HITS_TOTAL.inc()
            return JSONResponse(
                content=res.get("response", response_payload),
                status_code=status.HTTP_200_OK,
                headers={
                    "ETag": f'"{content_hash}"',
                    "X-Vault-Version-Id": res.get("version_id", version_id),
                    "X-Vault-Logical-Version": str(logical_version),
                    "X-Vault-Idempotent-Hit": "true",
                }
            )

        return JSONResponse(
            content=response_payload,
            status_code=status.HTTP_201_CREATED,
            headers={
                "ETag": f'"{content_hash}"',
                "X-Vault-Version-Id": version_id,
                "X-Vault-Logical-Version": str(logical_version),
                "X-Vault-Policy": policy_name,
            }
        )

    @app.get("/v1/objects/{bucket}/{key:path}")
    async def get_object(bucket: str, key: str) -> Response:
        """
        Coordinated distributed read flow:
        1. Resolve latest committed manifest through Raft metadata cluster.
        2. Concurrently read candidate replicas/fragments for each chunk.
        3. Verify cryptographic SHA-256 hash.
        4. For archive policy, decode from >= 4 valid fragments and verify final hash.
        5. Serve only verified data.
        6. Record failed, missing, or corrupt replicas/fragments for repair.
        """
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        manifest_dict = gw.metadata_raft.get_latest_manifest(bucket, key)
        if not manifest_dict or manifest_dict.get("is_tombstone"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

        manifest = ObjectManifest.model_validate(manifest_dict)

        # -------------------------------------------------------------
        # Erasure-Coded Object Reconstruction (Archive Policy)
        # -------------------------------------------------------------
        if manifest.policy == "archive" or any(c.is_erasure_coded for c in manifest.chunks):
            reconstructed_chunks: List[bytes] = []
            overall_hasher = hashlib.sha256()

            for chunk_meta in manifest.chunks:
                fragments_info = chunk_meta.fragments or []
                if not fragments_info:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Corrupt manifest: missing fragment metadata for chunk {chunk_meta.chunk_index}"
                    )

                async def fetch_fragment(f_info: FragmentInfo):
                    node = gw.cluster.get_storage_node(f_info.node_id)
                    if not node or not node.active:
                        return f_info.fragment_index, f_info.node_id, None
                    data = await gw.fetch_chunk_from_node(
                        node=node,
                        bucket=bucket,
                        version_id=manifest.version_id,
                        chunk_index=f_info.fragment_index,
                        expected_sha256=f_info.sha256,
                    )
                    return f_info.fragment_index, f_info.node_id, data

                fetch_tasks = [fetch_fragment(f) for f in fragments_info]
                results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

                valid_fragments: Dict[int, bytes] = {}
                failed_fragments: List[tuple] = []  # (frag_index, node_id, expected_sha)

                for r in results:
                    if isinstance(r, Exception):
                        continue
                    frag_idx, node_id, data = r
                    if data is not None:
                        valid_fragments[frag_idx] = data
                    else:
                        target_f = next(f for f in fragments_info if f.fragment_index == frag_idx)
                        failed_fragments.append((frag_idx, node_id, target_f.sha256))

                # Check if we have at least K=4 valid fragments
                codec = ErasureCodec(4, 2)
                if len(valid_fragments) < codec.k:
                    # Enqueue read repair if repair worker is active
                    if gw.repair_worker:
                        for f_idx, node_id, f_sha in failed_fragments:
                            gw.repair_worker.enqueue_repair(
                                bucket=bucket,
                                key=key,
                                version_id=manifest.version_id,
                                chunk_index=f_idx,
                                failed_node_id=node_id,
                                expected_sha256=f_sha,
                                surviving_healthy_replicas=len(valid_fragments),
                                state="corrupt_or_missing",
                            )
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=(
                            f"Archive read failed for chunk {chunk_meta.chunk_index}: "
                            f"only {len(valid_fragments)} valid fragments available, required at least {codec.k}"
                        )
                    )

                # Decode / Reconstruct original chunk data
                orig_size = chunk_meta.original_chunk_size if chunk_meta.original_chunk_size is not None else chunk_meta.size_bytes
                try:
                    reconstructed_chunk = codec.decode(valid_fragments, orig_size)
                except Exception as exc:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Erasure decoding failed for chunk {chunk_meta.chunk_index}: {exc}"
                    )

                # Verify reconstructed chunk hash
                chunk_sha = hashlib.sha256(reconstructed_chunk).hexdigest()
                if chunk_sha != chunk_meta.sha256:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Reconstructed chunk {chunk_meta.chunk_index} checksum mismatch: expected {chunk_meta.sha256}, got {chunk_sha}"
                    )

                reconstructed_chunks.append(reconstructed_chunk)
                overall_hasher.update(reconstructed_chunk)

                # Enqueue missing or corrupt fragments for background read-repair
                if failed_fragments and gw.repair_worker:
                    surviving_count = len(valid_fragments)
                    for f_idx, node_id, f_sha in failed_fragments:
                        gw.repair_worker.enqueue_repair(
                            bucket=bucket,
                            key=key,
                            version_id=manifest.version_id,
                            chunk_index=f_idx,
                            failed_node_id=node_id,
                            expected_sha256=f_sha,
                            surviving_healthy_replicas=surviving_count,
                            state="corrupt_or_missing",
                        )

            # Do not serve reconstructed data unless the final object hash validates
            final_hash = overall_hasher.hexdigest()
            if final_hash != manifest.content_hash:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Reconstructed object checksum mismatch: expected {manifest.content_hash}, calculated {final_hash}"
                )

            headers = {
                "Content-Length": str(manifest.size_bytes),
                "Content-Type": manifest.content_type,
                "ETag": f'"{manifest.content_hash}"',
                "X-Vault-Version-Id": manifest.version_id,
                "X-Vault-Logical-Version": str(manifest.logical_version),
                "X-Vault-Policy": manifest.policy,
            }

            async def ec_streamer():
                for c_bytes in reconstructed_chunks:
                    yield c_bytes

            return StreamingResponse(ec_streamer(), media_type=manifest.content_type, headers=headers)

        # -------------------------------------------------------------
        # Replicated Object Streaming (Hot & Durable Policies)
        # -------------------------------------------------------------
        async def chunk_streamer() -> AsyncGenerator[bytes, None]:
            for chunk_meta in manifest.chunks:
                # Find candidate nodes
                candidate_node_ids = chunk_meta.placement_nodes
                candidate_nodes = [
                    gw.cluster.get_storage_node(nid)
                    for nid in candidate_node_ids
                    if gw.cluster.get_storage_node(nid) and gw.cluster.get_storage_node(nid).active
                ]

                if not candidate_nodes:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"All replica storage nodes for chunk {chunk_meta.chunk_index} are offline"
                    )

                verified_bytes: Optional[bytes] = None

                # Query candidate replicas concurrently
                node_task_map = {
                    node: asyncio.create_task(gw.fetch_chunk_from_node(
                        node=node,
                        bucket=bucket,
                        version_id=manifest.version_id,
                        chunk_index=chunk_meta.chunk_index,
                        expected_sha256=chunk_meta.sha256,
                    ))
                    for node in candidate_nodes
                }

                # We iterate through completed reads or take the first valid replica
                for fut in asyncio.as_completed(node_task_map.values()):
                    data = await fut
                    if data is not None:
                        verified_bytes = data
                        break

                # Enqueue any degraded/failed replica for read repair
                if gw.repair_worker:
                    async def evaluate_read_repairs():
                        for n, t in node_task_map.items():
                            try:
                                res = await t
                                if res is None:
                                    gw.repair_worker.enqueue_repair(
                                        bucket=bucket,
                                        key=key,
                                        version_id=manifest.version_id,
                                        chunk_index=chunk_meta.chunk_index,
                                        failed_node_id=n.node_id,
                                        expected_sha256=chunk_meta.sha256,
                                        surviving_healthy_replicas=1 if verified_bytes else 0,
                                        state="corrupt_or_missing",
                                    )
                            except Exception:
                                gw.repair_worker.enqueue_repair(
                                    bucket=bucket,
                                    key=key,
                                    version_id=manifest.version_id,
                                    chunk_index=chunk_meta.chunk_index,
                                    failed_node_id=n.node_id,
                                    expected_sha256=chunk_meta.sha256,
                                    surviving_healthy_replicas=1 if verified_bytes else 0,
                                    state="unreachable",
                                )
                    asyncio.create_task(evaluate_read_repairs())

                if verified_bytes is None:
                    gw.repair_backlog.append({
                        "bucket": bucket,
                        "key": key,
                        "version_id": manifest.version_id,
                        "chunk_index": chunk_meta.chunk_index,
                        "reason": "all_replicas_failed_checksum_or_offline",
                    })
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Data unavailable: no healthy replica reachable for chunk {chunk_meta.chunk_index}"
                    )

                yield verified_bytes

        headers = {
            "Content-Length": str(manifest.size_bytes),
            "Content-Type": manifest.content_type,
            "ETag": f'"{manifest.content_hash}"',
            "X-Vault-Version-Id": manifest.version_id,
            "X-Vault-Logical-Version": str(manifest.logical_version),
            "X-Vault-Policy": manifest.policy,
        }

        return StreamingResponse(chunk_streamer(), media_type=manifest.content_type, headers=headers)

    @app.head("/v1/objects/{bucket}/{key:path}")
    async def head_object(bucket: str, key: str) -> Response:
        """Retrieve object metadata headers without transferring body."""
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        manifest_dict = gw.metadata_raft.get_latest_manifest(bucket, key)
        if not manifest_dict or manifest_dict.get("is_tombstone"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

        headers = {
            "Content-Length": str(manifest_dict.get("size_bytes", 0)),
            "Content-Type": manifest_dict.get("content_type", "application/octet-stream"),
            "ETag": f'"{manifest_dict.get("content_hash", "")}"',
            "X-Vault-Version-Id": str(manifest_dict.get("version_id", "")),
            "X-Vault-Logical-Version": str(manifest_dict.get("logical_version", 1)),
            "X-Vault-Policy": str(manifest_dict.get("policy", "hot")),
        }
        return PlainResponse(content=b"", headers=headers)

    @app.delete("/v1/objects/{bucket}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_object(
        bucket: str,
        key: str,
        expected_version: Optional[int] = Header(None, alias="X-Expected-Version"),
        idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    ) -> Response:
        """Commit an object tombstone through the Raft metadata cluster."""
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        try:
            res = gw.metadata_raft.create_tombstone(
                bucket=bucket,
                key=key,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )
        except ManifestNotFoundError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")
        except CASConflictError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
        except (NotLeaderError, RaftQuorumError) as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

        return PlainResponse(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={
                "X-Vault-Version-Id": str(res.get("version_id", "")),
                "X-Vault-Logical-Version": str(res.get("logical_version", 1)),
                "X-Vault-Tombstone": "true",
            }
        )

    return app
