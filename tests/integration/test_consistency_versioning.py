"""Integration tests for object versioning, CAS consistency, tombstones, and replica state convergence."""

import asyncio
import hashlib
import time
import uuid
from pathlib import Path
from typing import Dict, List
import httpx
import pytest

from apps.gateway.service import GatewayService, create_gateway_app
from apps.storage_node.service import create_vault_app
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.manifest import (
    ObjectManifest,
    ReplicaState,
    evaluate_gc_eligibility,
)
from vault_core.metadata_raft import RaftMetadataStateMachine, RaftQuorumError
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.settings import VaultSettings


class MultiNodeHttpTransport(httpx.AsyncBaseTransport):
    """Simulates inter-node network routing with dynamic node stop/start."""

    def __init__(self, node_apps: Dict[str, httpx.ASGITransport]) -> None:
        self.node_transports = node_apps
        self.offline_nodes: set[str] = set()

    def stop_node(self, node_id: str) -> None:
        self.offline_nodes.add(node_id)

    def start_node(self, node_id: str) -> None:
        self.offline_nodes.discard(node_id)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        for node_id, transport in self.node_transports.items():
            if f"://{node_id}:" in url_str:
                if node_id in self.offline_nodes:
                    raise httpx.ConnectError(f"Connection refused: storage node {node_id} is offline")
                return await transport.handle_async_request(request)
        raise httpx.ConnectError(f"Host unreachable: {request.url}")


@pytest.fixture
def cluster_env(tmp_path: Path):
    """Sets up a 6-node storage cluster and Raft metadata consensus node."""
    storage_configs = [
        StorageNodeConfig(node_id="storage-1", url="http://storage-1:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-2", url="http://storage-2:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-3", url="http://storage-3:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-4", url="http://storage-4:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-5", url="http://storage-5:8001", region="us-east-1", zone="us-east-1c", active=True),
        StorageNodeConfig(node_id="storage-6", url="http://storage-6:8001", region="us-east-1", zone="us-east-1c", active=True),
    ]

    cluster = ClusterConfig(cluster_id="vault-test-cluster", storage_nodes=storage_configs)

    policies = {
        "hot": DurabilityPolicy(
            name="hot",
            scheme=PolicyScheme.REPLICATION,
            replication_factor=3,
            data_write_quorum=2,
            data_read_quorum=1,
            minimum_distinct_zones=3,
        ),
    }

    storage_apps = {}
    storage_dirs = {}
    for node in storage_configs:
        node_dir = tmp_path / node.node_id
        storage_dirs[node.node_id] = node_dir
        node_settings = VaultSettings(data_dir=node_dir, chunk_size_bytes=8 * 1024 * 1024)
        storage_app = create_vault_app(node_settings)
        storage_apps[node.node_id] = httpx.ASGITransport(app=storage_app)

    transport = MultiNodeHttpTransport(storage_apps)
    storage_http_client = httpx.AsyncClient(transport=transport, timeout=3.0)

    raft_dir = tmp_path / "raft_meta"
    raft_port = 26000 + (int(time.time() * 100) % 3000)
    raft_addr = f"127.0.0.1:{raft_port}"
    raft_sm = RaftMetadataStateMachine(self_address=raft_addr, partner_addresses=[], data_dir=raft_dir)

    start = time.time()
    while time.time() - start < 4.0:
        if raft_sm.is_leader():
            break
        time.sleep(0.05)

    assert raft_sm.is_leader(), "Raft metadata leader failed to elect"

    gateway_service = GatewayService(
        cluster=cluster,
        policies=policies,
        metadata_raft=raft_sm,
        storage_http_client=storage_http_client,
    )
    gateway_app = create_gateway_app(gateway_service)
    gateway_transport = httpx.ASGITransport(app=gateway_app)

    try:
        yield {
            "gateway_app": gateway_app,
            "gateway_transport": gateway_transport,
            "transport": transport,
            "raft_sm": raft_sm,
            "service": gateway_service,
            "cluster": cluster,
            "storage_dirs": storage_dirs,
        }
    finally:
        raft_sm.destroy()


# =============================================================================
# 1. CAS CONCURRENT WRITERS & STALE WRITES
# =============================================================================

@pytest.mark.asyncio
async def test_concurrent_writers_cas_exactly_one_succeeds(cluster_env):
    """
    Two concurrent writers competing to create the same key with X-Expected-Version: 0.
    Exactly one must succeed (HTTP 201), and the other must be rejected with HTTP 409 Conflict.
    """
    gw_transport = cluster_env["gateway_transport"]
    bucket = "test-cas-bucket"
    key = "documents/contract.pdf"

    payload_a = b"WRITER_A_DATA_PREMIUM_CONTRACT"
    payload_b = b"WRITER_B_DATA_ALTERNATIVE_CONTRACT"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # Launch concurrent PUT requests expecting version 0
        task_a = client.put(
            f"/v1/objects/{bucket}/{key}",
            content=payload_a,
            headers={
                "X-Idempotency-Key": f"cas-writer-a-{uuid.uuid4()}",
                "X-Expected-Version": "0",
                "X-Vault-Policy": "hot",
            }
        )
        task_b = client.put(
            f"/v1/objects/{bucket}/{key}",
            content=payload_b,
            headers={
                "X-Idempotency-Key": f"cas-writer-b-{uuid.uuid4()}",
                "X-Expected-Version": "0",
                "X-Vault-Policy": "hot",
            }
        )

        resp_a, resp_b = await asyncio.gather(task_a, task_b)

        statuses = [resp_a.status_code, resp_b.status_code]
        assert 201 in statuses, f"Expected at least one 201, got {statuses}"
        assert 409 in statuses, f"Expected exactly one 409 conflict, got {statuses}"
        assert statuses.count(201) == 1, "Exactly one write must succeed"
        assert statuses.count(409) == 1, "The competing write must receive HTTP 409"

        # Verify winning object is readable and uncorrupted
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 200
        assert get_resp.headers["X-Vault-Logical-Version"] == "1"

        winning_body = get_resp.content
        assert winning_body in [payload_a, payload_b]


@pytest.mark.asyncio
async def test_stale_writer_receiving_http_409(cluster_env):
    """
    A writer attempting to update an object using a stale expected version must receive HTTP 409.
    """
    gw_transport = cluster_env["gateway_transport"]
    bucket = "test-stale-bucket"
    key = "data/stats.json"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Create initial version 1
        put1 = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b'{"version": 1}',
            headers={
                "X-Idempotency-Key": "writer-step-1",
                "X-Expected-Version": "0",
            }
        )
        assert put1.status_code == 201
        assert put1.headers["X-Vault-Logical-Version"] == "1"

        # 2. Advance to version 2
        put2 = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b'{"version": 2}',
            headers={
                "X-Idempotency-Key": "writer-step-2",
                "X-Expected-Version": "1",
            }
        )
        assert put2.status_code == 201
        assert put2.headers["X-Vault-Logical-Version"] == "2"

        # 3. Stale writer submits with old X-Expected-Version: 1
        stale_put = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b'{"version": "stale_write"}',
            headers={
                "X-Idempotency-Key": "writer-stale-step-3",
                "X-Expected-Version": "1",
            }
        )
        assert stale_put.status_code == 409
        error_detail = stale_put.json().get("detail", "")
        assert "expected version 1" in error_detail and "current logical version is 2" in error_detail


# =============================================================================
# 2. VERSIONED TOMBSTONE LIFECYCLE
# =============================================================================

@pytest.mark.asyncio
async def test_versioned_tombstone_behavior(cluster_env):
    """
    DELETE creates an immutable tombstone version in Raft.
    Immediate GET/HEAD returns 404.
    Re-creating the object increments logical_version past the tombstone.
    """
    gw_transport = cluster_env["gateway_transport"]
    bucket = "tombstone-bucket"
    key = "users/profile.png"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Create object (logical_version 1)
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"PROFILE_PICTURE_BYTES",
            headers={"X-Idempotency-Key": "tombstone-init"}
        )
        assert put_resp.status_code == 201
        v1_id = put_resp.headers["X-Vault-Version-Id"]
        assert put_resp.headers["X-Vault-Logical-Version"] == "1"

        # 2. Delete object (creates tombstone, logical_version 2)
        del_resp = await client.delete(
            f"/v1/objects/{bucket}/{key}",
            headers={"X-Expected-Version": "1"}
        )
        assert del_resp.status_code == 204
        tombstone_vid = del_resp.headers["X-Vault-Version-Id"]
        assert tombstone_vid != v1_id
        assert del_resp.headers["X-Vault-Logical-Version"] == "2"
        assert del_resp.headers["X-Vault-Tombstone"] == "true"

        # 3. GET and HEAD immediately return 404
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 404

        head_resp = await client.head(f"/v1/objects/{bucket}/{key}")
        assert head_resp.status_code == 404

        # 4. Duplicate delete returns 404
        del2_resp = await client.delete(f"/v1/objects/{bucket}/{key}")
        assert del2_resp.status_code == 404

        # 5. Re-creating the object succeeds and assigns logical_version 3
        recreate_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"RECREATED_PROFILE_BYTES",
            headers={
                "X-Idempotency-Key": "tombstone-recreate",
                "X-Expected-Version": "2",
            }
        )
        assert recreate_resp.status_code == 201
        assert recreate_resp.headers["X-Vault-Logical-Version"] == "3"

        # 6. Verify newly active object
        get_active = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_active.status_code == 200
        assert get_active.content == b"RECREATED_PROFILE_BYTES"


# =============================================================================
# 3. OLD REPLICA INVISIBILITY
# =============================================================================

@pytest.mark.asyncio
async def test_old_replica_not_visible_after_newer_committed_version(cluster_env):
    """
    Ensure that physical chunks belonging to an older version (even if modified or corrupt)
    never become visible or served after a newer version is committed in Raft.
    """
    gw_transport = cluster_env["gateway_transport"]
    bucket = "version-isolation-bucket"
    key = "release/app-update.bin"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # Write Version 1
        put_v1 = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"PAYLOAD_VERSION_1_LEGACY",
            headers={"X-Idempotency-Key": "version-test-v1"}
        )
        assert put_v1.status_code == 201
        v1_id = put_v1.headers["X-Vault-Version-Id"]

        # Write Version 2
        put_v2 = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"PAYLOAD_VERSION_2_MODERN",
            headers={
                "X-Idempotency-Key": "version-test-v2",
                "X-Expected-Version": "1",
            }
        )
        assert put_v2.status_code == 201
        v2_id = put_v2.headers["X-Vault-Version-Id"]
        assert v1_id != v2_id

        # Directly tamper with / mutate Version 1 chunk files on storage nodes
        storage_dirs = cluster_env["storage_dirs"]
        for node_id, node_dir in storage_dirs.items():
            v1_chunk_file = node_dir / "chunks" / bucket / v1_id / "chunk_0.dat"
            if v1_chunk_file.is_file():
                # Write garbage to v1 chunk
                v1_chunk_file.write_bytes(b"CORRUPTED_V1_GARBAGE_BYTES")

        # GET must target and serve Version 2 without any contamination from Version 1
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 200
        assert get_resp.headers["X-Vault-Version-Id"] == v2_id
        assert get_resp.headers["X-Vault-Logical-Version"] == "2"
        assert get_resp.content == b"PAYLOAD_VERSION_2_MODERN"


# =============================================================================
# 4. FAILED RAFT COMMIT LEAVES NO VISIBLE OBJECT
# =============================================================================

@pytest.mark.asyncio
async def test_failed_raft_commit_leaves_no_visible_object_version(cluster_env):
    """
    If data write quorum succeeds but the subsequent Raft manifest commit fails,
    the gateway must return an error and leave NO visible object version.
    All uploaded chunks must be safely registered as orphan candidates.
    """
    gw_transport = cluster_env["gateway_transport"]
    gw_service: GatewayService = cluster_env["service"]
    raft_sm: RaftMetadataStateMachine = cluster_env["raft_sm"]

    bucket = "abort-bucket"
    key = "critical/database_backup.sql"

    # Monkeypatch commit_manifest to simulate Raft consensus quorum failure
    original_commit = raft_sm.commit_manifest

    def failing_commit(*args, **kwargs):
        raise RaftQuorumError("Consensus quorum partition during commit simulation")

    raft_sm.commit_manifest = failing_commit

    initial_orphan_count = len(gw_service.orphan_candidates)

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"DATABASE_DUMP_BYTES_12345",
            headers={"X-Idempotency-Key": "failed-commit-test"}
        )
        # Gateway returns HTTP 503 Service Unavailable when Raft commit quorum fails
        assert put_resp.status_code == 503
        assert "Consensus quorum partition" in put_resp.json().get("detail", "")

        # Verify object does NOT exist in Raft or Gateway
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 404

        head_resp = await client.head(f"/v1/objects/{bucket}/{key}")
        assert head_resp.status_code == 404

        # Verify all uploaded data chunks were safely registered as orphan candidates for GC
        assert len(gw_service.orphan_candidates) > initial_orphan_count
        orphans = [o for o in gw_service.orphan_candidates if o.get("bucket") == bucket]
        assert len(orphans) >= 2  # Hot policy requires at least write quorum 2 acknowledged

    # Restore original method
    raft_sm.commit_manifest = original_commit


# =============================================================================
# 5. REPLICA INVENTORY AUDIT & CONVERGENCE STATES
# =============================================================================

@pytest.mark.asyncio
async def test_replica_inventory_audit_convergence_states(cluster_env):
    """
    Test replica inventory audit endpoint comparing physical storage nodes against Raft manifest.
    Verifies detection of:
    - HEALTHY: current version on disk matches checksum.
    - STALE: node holds superseded historical version.
    - CORRUPT: node holds corrupted chunk bytes.
    - UNREACHABLE: node is offline.
    """
    gw_transport = cluster_env["gateway_transport"]
    net_transport: MultiNodeHttpTransport = cluster_env["transport"]
    storage_dirs = cluster_env["storage_dirs"]
    bucket = "audit-bucket"
    key = "assets/logo.png"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # Step 1: Upload version 1
        put_v1 = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"LOGO_V1_INITIAL_ASSET",
            headers={"X-Idempotency-Key": "audit-v1"}
        )
        assert put_v1.status_code == 201
        v1_id = put_v1.headers["X-Vault-Version-Id"]

        # Step 2: Upload version 2
        put_v2 = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=b"LOGO_V2_REDESIGNED_ASSET",
            headers={
                "X-Idempotency-Key": "audit-v2",
                "X-Expected-Version": "1",
            }
        )
        assert put_v2.status_code == 201
        v2_id = put_v2.headers["X-Vault-Version-Id"]

        # Step 3: Run baseline audit; all target replicas should be HEALTHY
        audit_resp1 = await client.get(f"/v1/objects/{bucket}/{key}/audit")
        assert audit_resp1.status_code == 200
        data1 = audit_resp1.json()
        assert data1["healthy_count"] == 3
        assert data1["stale_count"] == 0
        assert data1["corrupt_count"] == 0
        assert data1["unreachable_count"] == 0

        # Step 4: Induce heterogeneous replica states
        target_audits = data1["replicas"]
        node_stale = target_audits[0]["node_id"]
        node_corrupt = target_audits[1]["node_id"]
        node_offline = target_audits[2]["node_id"]

        # Ensure node_stale holds the superseded v1 chunk, and does not have the current v2 chunk
        v1_chunk_stale = storage_dirs[node_stale] / "chunks" / bucket / v1_id / "chunk_0.dat"
        v1_chunk_stale.parent.mkdir(parents=True, exist_ok=True)
        v1_chunk_stale.write_bytes(b"LOGO_V1_INITIAL_ASSET")

        v2_chunk_stale = storage_dirs[node_stale] / "chunks" / bucket / v2_id / "chunk_0.dat"
        if v2_chunk_stale.is_file():
            v2_chunk_stale.unlink()

        # Make node_corrupt hold corrupt bytes for v2
        v2_chunk_corrupt = storage_dirs[node_corrupt] / "chunks" / bucket / v2_id / "chunk_0.dat"
        v2_chunk_corrupt.write_bytes(b"CORRUPTED_DISK_SECTOR_BYTES")

        # Make node_offline unreachable
        net_transport.stop_node(node_offline)

        # Step 5: Execute audit again and assert convergence states
        audit_resp2 = await client.get(f"/v1/objects/{bucket}/{key}/audit")
        assert audit_resp2.status_code == 200
        data2 = audit_resp2.json()

        replica_states = {r["node_id"]: r["state"] for r in data2["replicas"]}

        assert replica_states[node_stale] == ReplicaState.STALE
        assert replica_states[node_corrupt] == ReplicaState.CORRUPT
        assert replica_states[node_offline] == ReplicaState.UNREACHABLE

        assert data2["stale_count"] == 1
        assert data2["corrupt_count"] == 1
        assert data2["unreachable_count"] == 1
        assert data2["healthy_count"] == 0

        # Restore offline node
        net_transport.start_node(node_offline)


# =============================================================================
# 6. SAFE OLD-VERSION GARBAGE COLLECTION ELIGIBILITY
# =============================================================================

def test_gc_eligibility_rules():
    """
    Test evaluate_gc_eligibility logic:
    - Active current version is NEVER eligible.
    - Tombstone within grace period is NOT eligible.
    - Tombstone past retention period IS eligible.
    - Superseded version within grace period is NOT eligible.
    - Superseded version past retention period IS eligible.
    """
    now = 100_000.0
    retention_grace = 3600  # 1 hour grace period

    active_manifest = ObjectManifest(
        bucket="b",
        key="k",
        version_id="active-uuid",
        size_bytes=100,
        content_hash="abc",
        content_type="text/plain",
        policy="hot",
        chunks=[],
        is_tombstone=False,
        created_at=now - 50000,
    )

    # 1. Active current version cannot be collected
    eligible, reason = evaluate_gc_eligibility(
        manifest=active_manifest,
        current_manifest=active_manifest,
        retention_period_seconds=retention_grace,
        current_time=now,
    )
    assert not eligible
    assert "Active current version" in reason

    # 2. Tombstone within retention grace period
    recent_tombstone = ObjectManifest(
        bucket="b",
        key="k",
        version_id="tomb-1",
        size_bytes=0,
        content_hash="",
        content_type="",
        policy="hot",
        chunks=[],
        is_tombstone=True,
        created_at=now - 1000,
        deleted_at=now - 1000,  # 1000s < 3600s
    )
    eligible, reason = evaluate_gc_eligibility(
        manifest=recent_tombstone,
        current_manifest=None,
        retention_period_seconds=retention_grace,
        current_time=now,
    )
    assert not eligible
    assert "within retention grace period" in reason

    # 3. Tombstone past retention grace period
    expired_tombstone = ObjectManifest(
        bucket="b",
        key="k",
        version_id="tomb-2",
        size_bytes=0,
        content_hash="",
        content_type="",
        policy="hot",
        chunks=[],
        is_tombstone=True,
        created_at=now - 10000,
        deleted_at=now - 5000,  # 5000s >= 3600s
    )
    eligible, reason = evaluate_gc_eligibility(
        manifest=expired_tombstone,
        current_manifest=None,
        retention_period_seconds=retention_grace,
        current_time=now,
    )
    assert eligible
    assert "expired retention" in reason

    # 4. Superseded historical version within grace period
    recent_superseded = ObjectManifest(
        bucket="b",
        key="k",
        version_id="old-v1",
        size_bytes=100,
        content_hash="abc",
        content_type="text/plain",
        policy="hot",
        chunks=[],
        is_tombstone=False,
        created_at=now - 1200,  # 1200s < 3600s
    )
    eligible, reason = evaluate_gc_eligibility(
        manifest=recent_superseded,
        current_manifest=active_manifest,
        retention_period_seconds=retention_grace,
        current_time=now,
    )
    assert not eligible
    assert "within retention grace period" in reason

    # 5. Superseded historical version past retention period
    expired_superseded = ObjectManifest(
        bucket="b",
        key="k",
        version_id="old-v0",
        size_bytes=100,
        content_hash="abc",
        content_type="text/plain",
        policy="hot",
        chunks=[],
        is_tombstone=False,
        created_at=now - 7200,  # 7200s >= 3600s
    )
    eligible, reason = evaluate_gc_eligibility(
        manifest=expired_superseded,
        current_manifest=active_manifest,
        retention_period_seconds=retention_grace,
        current_time=now,
    )
    assert eligible
    assert "expired retention" in reason
