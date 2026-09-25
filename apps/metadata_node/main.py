"""FastAPI REST service for Raft metadata cluster node."""

import os
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response as PlainResponse

from vault_core.logging_config import StructuredLoggingMiddleware, logger
from vault_core.metadata_raft import (
    CASConflictError,
    ManifestNotFoundError,
    NotLeaderError,
    RaftError,
    RaftMetadataStateMachine,
    RaftQuorumError,
)
from vault_core.metrics import get_latest_metrics

# Configurable environment settings for container and standalone execution
NODE_ID = os.getenv("NODE_ID", "meta-1")
BIND_ADDR = os.getenv("BIND_ADDR", "127.0.0.1:9002")
PARTNERS = [p.strip() for p in os.getenv("PARTNERS", "").split(",") if p.strip()]
DATA_DIR = os.getenv("DATA_DIR", f"./data/{NODE_ID}")

raft_sm: Optional[RaftMetadataStateMachine] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global raft_sm
    logger.info(f"Starting Raft metadata state machine on {BIND_ADDR} with partners {PARTNERS}")
    raft_sm = RaftMetadataStateMachine(
        self_address=BIND_ADDR,
        partner_addresses=PARTNERS,
        data_dir=DATA_DIR,
    )
    yield
    if raft_sm:
        logger.info(f"Shutting down Raft node {NODE_ID}")
        raft_sm.destroy()


app = FastAPI(
    title=f"Vault Raft Metadata Node ({NODE_ID})",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(StructuredLoggingMiddleware)


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    """Prometheus metrics exposition."""
    body, content_type = get_latest_metrics()
    return Response(content=body, media_type=content_type)


@app.get("/v1/metadata/cluster/health")
async def cluster_health() -> dict:
    """Return health and consensus status of this Raft node."""
    if not raft_sm:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    health = raft_sm.get_cluster_health()
    health["node_id"] = NODE_ID
    return health


@app.post("/v1/metadata/manifests/{bucket}/{key:path}", status_code=status.HTTP_201_CREATED)
async def commit_manifest(
    bucket: str,
    key: str,
    request: Request,
    expected_version: Optional[int] = Header(None, alias="X-Expected-Version"),
    idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
) -> Response:
    """
    Commit an object manifest through Raft consensus.
    Requires that this node is the Raft leader.
    Enforces CAS precondition and idempotency deduplication.
    """
    if not raft_sm:
        raise HTTPException(status_code=503, detail="Raft state machine uninitialized")

    manifest_dict = await request.json()
    manifest_dict["bucket"] = bucket
    manifest_dict["key"] = key

    try:
        result = raft_sm.commit_manifest(
            manifest_dict=manifest_dict,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            response_payload=manifest_dict,
        )
    except NotLeaderError as exc:
        raise HTTPException(
            status_code=status.HTTP_421_MISDIRECTED_REQUEST,
            detail=str(exc),
            headers={"X-Vault-Raft-Leader": exc.leader_address or "unknown"}
        )
    except CASConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc)
        )
    except RaftQuorumError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc)
        )
    except RaftError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc)
        )

    return JSONResponse(
        content=result,
        status_code=status.HTTP_201_CREATED if not result.get("idempotent_hit") else status.HTTP_200_OK,
        headers={
            "X-Vault-Version-Id": str(result.get("version_id", "")),
            "X-Vault-Logical-Version": str(result.get("logical_version", 1)),
        }
    )


@app.get("/v1/metadata/manifests/{bucket}/{key:path}")
async def get_latest_manifest(bucket: str, key: str) -> Response:
    """Retrieve latest committed object manifest."""
    if not raft_sm:
        raise HTTPException(status_code=503, detail="Raft state machine uninitialized")

    manifest = raft_sm.get_latest_manifest(bucket, key)
    if not manifest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object manifest not found or tombstoned")

    return JSONResponse(content=manifest)


@app.delete("/v1/metadata/manifests/{bucket}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
async def tombstone_manifest(
    bucket: str,
    key: str,
    expected_version: Optional[int] = Header(None, alias="X-Expected-Version"),
    idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
) -> Response:
    """Commit an object tombstone through Raft consensus."""
    if not raft_sm:
        raise HTTPException(status_code=503, detail="Raft state machine uninitialized")

    try:
        raft_sm.create_tombstone(
            bucket=bucket,
            key=key,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
        )
    except NotLeaderError as exc:
        raise HTTPException(
            status_code=status.HTTP_421_MISDIRECTED_REQUEST,
            detail=str(exc),
            headers={"X-Vault-Raft-Leader": exc.leader_address or "unknown"}
        )
    except CASConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except ManifestNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found or already deleted")
    except RaftQuorumError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    return PlainResponse(status_code=status.HTTP_204_NO_CONTENT)
