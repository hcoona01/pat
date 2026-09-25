import asyncio
import hashlib
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response as PlainResponse, StreamingResponse

from vault_core.hashing import sha256_bytes
from vault_core.logging_config import StructuredLoggingMiddleware, logger
from vault_core.manifest import ChunkInfo, ObjectManifest
from vault_core.metadata_db import MetadataRepository
from vault_core.metrics import (
    IDEMPOTENT_HITS_TOTAL,
    get_latest_metrics,
)
from vault_core.scanner import IntegrityScanner
from vault_core.settings import VaultSettings, settings
from vault_core.storage import (
    ChecksumMismatchError,
    ChunkNotFoundError,
    CorruptedChunkError,
    LocalChunkStorage,
)


def create_vault_app(custom_settings: Optional[VaultSettings] = None) -> FastAPI:
    """Create and configure the FastAPI application for single-node Vault storage."""
    cfg = custom_settings or settings

    # Initialize storage and metadata components
    storage = LocalChunkStorage(cfg.data_dir)
    metadata = MetadataRepository(cfg.data_dir / "vault_metadata.db")
    scanner = IntegrityScanner(storage, scan_interval_seconds=60.0)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await metadata.initialize()
        scanner.start()
        logger.info(
            f"Vault storage service initialized with data_dir={cfg.data_dir}, "
            f"chunk_size={cfg.chunk_size_bytes} bytes"
        )
        yield
        scanner.stop()

    app = FastAPI(
        title="Vault Object Storage (Single Node Prototype)",
        version="0.1.0",
        description="Single-node reference implementation of Vault distributed object storage",
        lifespan=lifespan,
    )

    # Attach middleware
    app.add_middleware(StructuredLoggingMiddleware)

    # Expose components on app.state for testing / direct inspection
    app.state.storage = storage
    app.state.metadata = metadata
    app.state.scanner = scanner
    app.state.settings = cfg

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        """Prometheus metrics endpoint."""
        body, content_type = get_latest_metrics()
        return Response(content=body, media_type=content_type)

    @app.post("/v1/admin/scan")
    async def trigger_full_scan() -> dict:
        """Trigger an immediate full integrity scan across all chunks on this storage node."""
        return await asyncio.to_thread(scanner.scan_once)

    @app.get("/v1/admin/scan/status")
    async def scan_status() -> dict:
        """Inspect last integrity scan outcome."""
        return {
            "running": scanner._running,
            "last_scan": scanner.last_scan_result,
        }

    @app.put("/v1/objects/{bucket}/{key:path}", status_code=status.HTTP_201_CREATED)
    async def put_object(
        bucket: str,
        key: str,
        request: Request,
        idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
        content_type: str = Header("application/octet-stream", alias="Content-Type"),
    ) -> Response:
        """
        Stream upload an object, splitting into configurable 8 MiB chunks,
        computing SHA-256 on the fly, and persisting immutable version manifest.
        Requires an idempotency key.
        """
        # Fall back to alternative header name if X-Idempotency-Key is absent
        idemp_key = idempotency_key or request.headers.get("Idempotency-Key")
        if not idemp_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required Idempotency-Key (or X-Idempotency-Key) header"
            )

        # Check for idempotent retry
        existing = await metadata.get_idempotent_result(idemp_key)
        if existing:
            IDEMPOTENT_HITS_TOTAL.inc()
            logger.info(
                f"Idempotent hit for key {idemp_key}",
                extra={"bucket": bucket, "key": key}
            )
            return JSONResponse(
                content=existing,
                status_code=status.HTTP_200_OK,
                headers={
                    "ETag": f'"{existing.get("content_hash", "")}"',
                    "X-Vault-Version-Id": existing.get("version_id", ""),
                    "X-Vault-Logical-Version": str(existing.get("logical_version", 1)),
                    "X-Vault-Idempotent-Hit": "true",
                }
            )

        version_id = str(uuid.uuid4())
        chunks: list[ChunkInfo] = []
        overall_hasher = hashlib.sha256()
        total_size = 0
        chunk_index = 0
        current_chunk_buffer = bytearray()
        chunk_limit = cfg.chunk_size_bytes

        # Stream the request body
        async for byte_chunk in request.stream():
            if not byte_chunk:
                continue

            current_chunk_buffer.extend(byte_chunk)
            overall_hasher.update(byte_chunk)
            total_size += len(byte_chunk)

            while len(current_chunk_buffer) >= chunk_limit:
                slice_to_write = bytes(current_chunk_buffer[:chunk_limit])
                current_chunk_buffer = current_chunk_buffer[chunk_limit:]

                chunk_info = storage.write_chunk(
                    bucket=bucket,
                    version_id=version_id,
                    chunk_index=chunk_index,
                    data=slice_to_write,
                )
                chunks.append(chunk_info)
                chunk_index += 1

        # Write final leftover chunk (or empty chunk if 0-byte object)
        if len(current_chunk_buffer) > 0 or chunk_index == 0:
            final_data = bytes(current_chunk_buffer)
            chunk_info = storage.write_chunk(
                bucket=bucket,
                version_id=version_id,
                chunk_index=chunk_index,
                data=final_data,
            )
            chunks.append(chunk_info)

        content_hash = overall_hasher.hexdigest()
        next_logical_version, prev_version_id = await metadata.get_next_logical_version(bucket, key)

        manifest = ObjectManifest(
            bucket=bucket,
            key=key,
            version_id=version_id,
            logical_version=next_logical_version,
            previous_version_id=prev_version_id,
            size_bytes=total_size,
            content_hash=content_hash,
            content_type=content_type,
            policy="single_node",
            chunks=chunks,
            is_tombstone=False,
            idempotency_key=idemp_key,
            created_at=time.time(),
        )

        response_payload = manifest.to_summary_dict()
        await metadata.commit_manifest(manifest, response_payload=response_payload)

        return JSONResponse(
            content=response_payload,
            status_code=status.HTTP_201_CREATED,
            headers={
                "ETag": f'"{content_hash}"',
                "X-Vault-Version-Id": version_id,
                "X-Vault-Logical-Version": str(next_logical_version),
            }
        )

    # =========================================================================
    # INTERNAL CHUNK-LEVEL STORAGE APIS (USED BY GATEWAY & REPAIR WORKERS)
    # =========================================================================

    @app.get("/v1/node/health")
    async def node_health() -> dict:
        """Return storage node identity and health status."""
        return {
            "node_id": os.getenv("NODE_ID", "store-local"),
            "region": os.getenv("REGION", "us-east-1"),
            "zone": os.getenv("ZONE", "us-east-1a"),
            "status": "active",
            "data_dir": str(cfg.data_dir),
            "chunks_dir_exists": storage.chunks_dir.is_dir(),
        }

    @app.put("/v1/chunks/{bucket}/{version_id}/{chunk_index:int}", status_code=status.HTTP_201_CREATED)
    async def put_chunk(
        bucket: str,
        version_id: str,
        chunk_index: int,
        request: Request,
        expected_sha256: Optional[str] = Header(None, alias="X-Expected-SHA256"),
    ) -> dict:
        """Atomically persist a single chunk on this storage node."""
        data = await request.body()
        try:
            chunk_info = storage.write_chunk(
                bucket=bucket,
                version_id=version_id,
                chunk_index=chunk_index,
                data=data,
                expected_sha256=expected_sha256,
            )
        except ChecksumMismatchError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        return {
            "chunk_id": chunk_info.chunk_id,
            "chunk_index": chunk_info.chunk_index,
            "sha256": chunk_info.sha256,
            "size_bytes": chunk_info.size_bytes,
        }

    @app.get("/v1/chunks/{bucket}/{version_id}/{chunk_index:int}")
    async def get_chunk(
        bucket: str,
        version_id: str,
        chunk_index: int,
        expected_sha256: Optional[str] = Header(None, alias="X-Expected-SHA256"),
    ) -> Response:
        """Retrieve chunk bytes after verifying SHA-256 integrity."""
        # If expected_sha256 is not sent in header, try reading without expected check or verify against file
        chunk_path = storage._chunk_path(bucket, version_id, chunk_index)
        if not chunk_path.is_file():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chunk not found")

        try:
            if expected_sha256:
                data = storage.read_chunk(bucket, version_id, chunk_index, expected_sha256)
            else:
                with open(chunk_path, "rb") as f:
                    data = f.read()
        except CorruptedChunkError as exc:
            raise HTTPException(status_code=status.HTTP_410_GONE, detail=str(exc))
        except ChunkNotFoundError:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chunk not found")

        return PlainResponse(content=data, media_type="application/octet-stream")

    @app.head("/v1/chunks/{bucket}/{version_id}/{chunk_index:int}")
    async def head_chunk(bucket: str, version_id: str, chunk_index: int) -> Response:
        """Check if chunk exists on this storage node."""
        chunk_path = storage._chunk_path(bucket, version_id, chunk_index)
        if not chunk_path.is_file():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return PlainResponse(headers={"Content-Length": str(chunk_path.stat().st_size)})

    @app.delete("/v1/chunks/{bucket}/{version_id}/{chunk_index:int}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_chunk(bucket: str, version_id: str, chunk_index: int) -> Response:
        """Remove chunk from disk (used by GC and rebalance)."""
        chunk_path = storage._chunk_path(bucket, version_id, chunk_index)
        if chunk_path.is_file():
            chunk_path.unlink()
        return PlainResponse(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/v1/objects/{bucket}/{key:path}")
    async def get_object(bucket: str, key: str) -> Response:
        """
        Stream download an object, verifying chunk SHA-256 checksums before serving bytes.
        Returns 404 if not found or tombstoned.
        """
        manifest = await metadata.get_latest_manifest(bucket, key)
        if not manifest or manifest.is_tombstone:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

        # Stream generator with on-the-fly checksum verification
        async def chunk_streamer() -> AsyncGenerator[bytes, None]:
            for chunk_meta in manifest.chunks:
                try:
                    data = storage.read_chunk(
                        bucket=manifest.bucket,
                        version_id=manifest.version_id,
                        chunk_index=chunk_meta.chunk_index,
                        expected_sha256=chunk_meta.sha256,
                    )
                except CorruptedChunkError as exc:
                    logger.error(f"Integrity check failed while serving {bucket}/{key}: {exc}")
                    raise HTTPException(
                        status_code=status.HTTP_410_GONE,
                        detail=f"Data corruption detected on chunk {chunk_meta.chunk_index}"
                    )
                yield data

        headers = {
            "Content-Length": str(manifest.size_bytes),
            "Content-Type": manifest.content_type,
            "ETag": f'"{manifest.content_hash}"',
            "X-Vault-Version-Id": manifest.version_id,
            "X-Vault-Logical-Version": str(manifest.logical_version),
        }

        return StreamingResponse(chunk_streamer(), media_type=manifest.content_type, headers=headers)

    @app.head("/v1/objects/{bucket}/{key:path}")
    async def head_object(bucket: str, key: str) -> Response:
        """Retrieve object metadata headers without transferring the body."""
        manifest = await metadata.get_latest_manifest(bucket, key)
        if not manifest or manifest.is_tombstone:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

        headers = {
            "Content-Length": str(manifest.size_bytes),
            "Content-Type": manifest.content_type,
            "ETag": f'"{manifest.content_hash}"',
            "X-Vault-Version-Id": manifest.version_id,
            "X-Vault-Logical-Version": str(manifest.logical_version),
            "X-Vault-Policy": manifest.policy,
        }
        return PlainResponse(content=b"", headers=headers)

    @app.delete("/v1/objects/{bucket}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_object(
        bucket: str,
        key: str,
        idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key")
    ) -> Response:
        """
        Record an immutable tombstone version for an object.
        Returns 204 No Content. Subsequent GET/HEAD requests immediately return 404.
        """
        tombstone = await metadata.create_tombstone(bucket, key, idempotency_key=idempotency_key)
        if not tombstone:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

        return PlainResponse(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={
                "X-Vault-Version-Id": tombstone.version_id,
                "X-Vault-Logical-Version": str(tombstone.logical_version),
                "X-Vault-Tombstone": "true",
            }
        )

    return app


app = create_vault_app()
