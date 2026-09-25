"""API Gateway service orchestrating placement, storage replication quorums, and Raft metadata."""

import asyncio
import hashlib
import time
import uuid
from typing import AsyncGenerator, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response as PlainResponse, StreamingResponse
import httpx

from vault_core.auth import create_auth_token
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.logging_config import StructuredLoggingMiddleware, logger
from vault_core.manifest import (
    ChunkInfo,
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
    app = FastAPI(
        title="Vault Object Storage Gateway",
        version="0.1.0",
        description="Distributed API Gateway with Raft metadata consensus and multi-node placement",
    )

    app.add_middleware(StructuredLoggingMiddleware)
    app.state.gateway = gateway_service

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        """Prometheus metrics endpoint."""
        body, content_type = get_latest_metrics()
        return Response(content=body, media_type=content_type)

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
            "size_bytes": manifest.size_bytes,
            "content_hash": manifest.content_hash,
            "chunks": chunk_statuses,
        }

    @app.get("/v1/objects/{bucket}/{key:path}/audit")
    async def audit_object(bucket: str, key: str) -> dict:
        """
        Audit physical replica inventory across storage nodes against the latest committed Raft manifest.
        Detects HEALTHY, STALE, CORRUPT, MISSING, and UNREACHABLE states.
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
            for node_id in chunk_meta.placement_nodes:
                node = gw.cluster.get_storage_node(node_id)
                if not node or not node.active:
                    audits.append(ReplicaAudit(
                        node_id=node_id,
                        chunk_index=chunk_meta.chunk_index,
                        expected_version_id=manifest.version_id,
                        expected_sha256=chunk_meta.sha256,
                        state=ReplicaState.UNREACHABLE,
                        details="Node not registered or marked inactive"
                    ))
                    continue

                client = gw.get_http_client()
                url = f"{node.url}/v1/chunks/{bucket}/{manifest.version_id}/{chunk_meta.chunk_index}"
                auth_token = create_auth_token(gw.settings.secret_key, node_id="gateway")

                try:
                    resp = await client.get(url, headers={"X-Vault-Auth-Token": auth_token})
                    if resp.status_code == 200:
                        actual_sha = hashlib.sha256(resp.content).hexdigest()
                        if actual_sha == chunk_meta.sha256:
                            audits.append(ReplicaAudit(
                                node_id=node_id,
                                chunk_index=chunk_meta.chunk_index,
                                expected_version_id=manifest.version_id,
                                expected_sha256=chunk_meta.sha256,
                                state=ReplicaState.HEALTHY,
                                actual_sha256=actual_sha,
                            ))
                        else:
                            audits.append(ReplicaAudit(
                                node_id=node_id,
                                chunk_index=chunk_meta.chunk_index,
                                expected_version_id=manifest.version_id,
                                expected_sha256=chunk_meta.sha256,
                                state=ReplicaState.CORRUPT,
                                actual_sha256=actual_sha,
                                details="Checksum mismatch on disk"
                            ))
                    elif resp.status_code == 404:
                        # Check if this node holds an older superseded (stale) version of this chunk
                        all_versions = gw.metadata_raft.get_all_versions(bucket, key)
                        older_versions = [
                            v for v in all_versions
                            if v.get("version_id") != manifest.version_id and not v.get("is_tombstone")
                        ]
                        has_stale = False
                        for old_v in older_versions:
                            old_vid = old_v.get("version_id")
                            old_url = f"{node.url}/v1/chunks/{bucket}/{old_vid}/{chunk_meta.chunk_index}"
                            try:
                                old_resp = await client.head(old_url, headers={"X-Vault-Auth-Token": auth_token})
                                if old_resp.status_code == 200:
                                    audits.append(ReplicaAudit(
                                        node_id=node_id,
                                        chunk_index=chunk_meta.chunk_index,
                                        expected_version_id=manifest.version_id,
                                        expected_sha256=chunk_meta.sha256,
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
                                chunk_index=chunk_meta.chunk_index,
                                expected_version_id=manifest.version_id,
                                expected_sha256=chunk_meta.sha256,
                                state=ReplicaState.MISSING,
                                details="Chunk not found on node"
                            ))
                    elif resp.status_code == 410:
                        audits.append(ReplicaAudit(
                            node_id=node_id,
                            chunk_index=chunk_meta.chunk_index,
                            expected_version_id=manifest.version_id,
                            expected_sha256=chunk_meta.sha256,
                            state=ReplicaState.CORRUPT,
                            details="Node reported chunk quarantined"
                        ))
                    else:
                        audits.append(ReplicaAudit(
                            node_id=node_id,
                            chunk_index=chunk_meta.chunk_index,
                            expected_version_id=manifest.version_id,
                            expected_sha256=chunk_meta.sha256,
                            state=ReplicaState.UNREACHABLE,
                            details=f"Node returned HTTP {resp.status_code}"
                        ))
                except Exception as exc:
                    audits.append(ReplicaAudit(
                        node_id=node_id,
                        chunk_index=chunk_meta.chunk_index,
                        expected_version_id=manifest.version_id,
                        expected_sha256=chunk_meta.sha256,
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

            # 2. Concurrently upload to target nodes
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
        2. Concurrently read candidate replicas for each chunk.
        3. Verify cryptographic SHA-256 hash.
        4. Serve only verified bytes.
        5. Record failed, missing, or corrupt replicas for repair.
        """
        gw: GatewayService = app.state.gateway
        if not gw.metadata_raft:
            raise HTTPException(status_code=503, detail="Metadata consensus unavailable")

        manifest_dict = gw.metadata_raft.get_latest_manifest(bucket, key)
        if not manifest_dict or manifest_dict.get("is_tombstone"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

        manifest = ObjectManifest.model_validate(manifest_dict)

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
                read_tasks = [
                    gw.fetch_chunk_from_node(
                        node=node,
                        bucket=bucket,
                        version_id=manifest.version_id,
                        chunk_index=chunk_meta.chunk_index,
                        expected_sha256=chunk_meta.sha256,
                    )
                    for node in candidate_nodes
                ]

                # We iterate through completed reads or take the first valid replica
                for coro in asyncio.as_completed(read_tasks):
                    data = await coro
                    if data is not None:
                        verified_bytes = data
                        break

                if verified_bytes is None:
                    # Enqueue for repair
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
