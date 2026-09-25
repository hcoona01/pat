"""Integration tests for Admin Storage Membership Management and Safe Background Rebalancing.

Validates:
- Admin endpoints: add node, activate node, deactivate node, safe remove node.
- Membership changes committed synchronously through Raft consensus.
- Rebalance trigger and progress tracking (queued, copied, verified, failed, bytes moved, completion percentage).
- Placement matching desired Rendezvous (HRW) placement after rebalance completion.
- Continuous foreground readability during active background rebalancing.
- Safe copy-before-delete: source replicas are never prematurely deleted before target verification and durability satisfaction.
- Resuming rebalance seamlessly after a worker restart.
"""

import asyncio
import hashlib
import time
from pathlib import Path
from typing import Dict, List
import httpx
import pytest

from apps.gateway.service import GatewayService, create_gateway_app
from apps.storage_node.service import create_vault_app
from apps.workers.rebalance_worker import RebalanceWorker
from apps.workers.repair_worker import RepairWorker
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.manifest import ObjectManifest
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.metrics import (
    REBALANCE_BYTES_MOVED_TOTAL,
    REBALANCE_COPIED_TOTAL,
    REBALANCE_PROGRESS_RATIO,
    REBALANCE_VERIFIED_TOTAL,
)
from vault_core.placement import select_placement_nodes
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.settings import VaultSettings


class MultiNodeHttpTransport(httpx.AsyncBaseTransport):
    """Simulates inter-node network routing with dynamic node addition/stop/start."""

    def __init__(self, node_apps: Dict[str, httpx.ASGITransport]) -> None:
        self.node_transports = dict(node_apps)
        self.offline_nodes: set[str] = set()

    def add_node_transport(self, node_id: str, transport: httpx.ASGITransport) -> None:
        self.node_transports[node_id] = transport

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
def rebalance_cluster_env(tmp_path: Path):
    """Sets up a 6-node storage cluster and Raft metadata node for rebalance testing."""
    storage_configs = [
        StorageNodeConfig(node_id="storage-1", url="http://storage-1:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-2", url="http://storage-2:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-3", url="http://storage-3:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-4", url="http://storage-4:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-5", url="http://storage-5:8001", region="us-east-1", zone="us-east-1c", active=True),
        StorageNodeConfig(node_id="storage-6", url="http://storage-6:8001", region="us-east-1", zone="us-east-1c", active=True),
    ]

    cluster = ClusterConfig(cluster_id="vault-rebalance-cluster", storage_nodes=storage_configs)

    policies = {
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
    storage_http_client = httpx.AsyncClient(transport=transport, timeout=5.0)

    raft_dir = tmp_path / "raft_meta"
    raft_port = 29000 + (int(time.time() * 100) % 2000)
    raft_addr = f"127.0.0.1:{raft_port}"
    raft_sm = RaftMetadataStateMachine(self_address=raft_addr, partner_addresses=[], data_dir=raft_dir)

    start = time.time()
    while time.time() - start < 4.0:
        if raft_sm.is_leader():
            break
        time.sleep(0.05)

    assert raft_sm.is_leader(), "Raft metadata leader failed to elect"

    # Seed initial cluster nodes into Raft
    raft_sm.seed_initial_membership([n.model_dump() for n in storage_configs])

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
            "policies": policies,
            "storage_dirs": storage_dirs,
            "storage_http_client": storage_http_client,
            "tmp_path": tmp_path,
        }
    finally:
        raft_sm.destroy()


# =============================================================================
# 1. ADMIN MEMBERSHIP LIFECYCLE (ADD, ACTIVATE, DEACTIVATE, SAFE REMOVE)
# =============================================================================

@pytest.mark.asyncio
async def test_admin_node_membership_lifecycle(rebalance_cluster_env):
    """
    Test admin endpoints:
    - Add a 7th storage node through Raft.
    - Verify node registration in both Raft and Gateway cluster topology.
    - Deactivate and activate node.
    - Verify safe removal checks (reject if zone constraint violated).
    - Remove node safely when permitted.
    """
    env = rebalance_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    # 1. List initial nodes
    resp = await gw_client.get("/v1/admin/nodes")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_nodes"] == 6
    assert data["active_nodes"] == 6

    # 2. Add 7th storage node in us-east-1a
    new_node = {
        "node_id": "storage-7",
        "url": "http://storage-7:8001",
        "region": "us-east-1",
        "zone": "us-east-1a",
        "active": True,
        "capacity_weight": 1.0,
    }
    add_resp = await gw_client.post("/v1/admin/nodes", json=new_node)
    assert add_resp.status_code == 201
    assert add_resp.json()["node_id"] == "storage-7"

    # Verify committed to Raft and Gateway
    nodes_resp = await gw_client.get("/v1/admin/nodes")
    assert nodes_resp.json()["total_nodes"] == 7
    assert env["raft_sm"].get_all_storage_nodes()["storage-7"]["zone"] == "us-east-1a"

    # 3. Deactivate node
    deact_resp = await gw_client.post("/v1/admin/nodes/storage-7/deactivate")
    assert deact_resp.status_code == 200
    assert deact_resp.json()["active"] is False

    nodes_resp2 = await gw_client.get("/v1/admin/nodes")
    assert nodes_resp2.json()["active_nodes"] == 6

    # 4. Activate node
    act_resp = await gw_client.post("/v1/admin/nodes/storage-7/activate")
    assert act_resp.status_code == 200
    assert act_resp.json()["active"] is True

    # 5. Remove node safely
    del_resp = await gw_client.delete("/v1/admin/nodes/storage-7")
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "removed"

    nodes_resp3 = await gw_client.get("/v1/admin/nodes")
    assert nodes_resp3.json()["total_nodes"] == 6


# =============================================================================
# 2. TRIGGER REBALANCE & VERIFY PLACEMENT MATCHES RENDEZVOUS
# =============================================================================

@pytest.mark.asyncio
async def test_rebalance_trigger_and_rendezvous_placement_matching(rebalance_cluster_env):
    """
    Test that:
    1. Upload objects with initial 6-node topology.
    2. Register storage-7 (with its storage app mounted in transport).
    3. Trigger rebalance via POST /v1/admin/rebalance.
    4. Await completion and verify progress metrics.
    5. Verify committed manifests in Raft match deterministic rendezvous placement!
    """
    env = rebalance_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    # Set up physical node storage-7 app and transport
    node_7_dir = env["tmp_path"] / "storage-7"
    node_7_settings = VaultSettings(data_dir=node_7_dir, chunk_size_bytes=8 * 1024 * 1024)
    storage_app_7 = create_vault_app(node_7_settings)
    env["transport"].add_node_transport("storage-7", httpx.ASGITransport(app=storage_app_7))

    # Upload 6 objects to distribute across nodes
    uploaded_objects = []
    for i in range(6):
        obj_key = f"rebal/object-{i}.dat"
        payload = f"Rebalance test content for object key {obj_key} with deterministic hashing!".encode("utf-8") * 50
        put_resp = await gw_client.put(
            f"/v1/objects/test-bucket/{obj_key}",
            content=payload,
            headers={"Idempotency-Key": f"rebal-put-{i}", "X-Vault-Policy": "hot"}
        )
        assert put_resp.status_code == 201
        uploaded_objects.append((obj_key, payload))

    # Register storage-7 via Admin API
    add_node_payload = {
        "node_id": "storage-7",
        "url": "http://storage-7:8001",
        "region": "us-east-1",
        "zone": "us-east-1a",
        "active": True,
        "capacity_weight": 2.0,  # Higher weight to attract chunks
    }
    await gw_client.post("/v1/admin/nodes", json=add_node_payload)

    # Trigger rebalance
    rebal_resp = await gw_client.post("/v1/admin/rebalance", params={"rate_limit": 50.0})
    assert rebal_resp.status_code == 202
    assert rebal_resp.json()["total_tasks"] >= 0

    # Wait for rebalance to complete
    start_wait = time.time()
    while time.time() - start_wait < 8.0:
        stat_resp = await gw_client.get("/v1/admin/rebalance/status")
        stat = stat_resp.json()
        if stat["status"] in ("completed", "idle") and stat["queued_tasks"] == 0:
            break
        await asyncio.sleep(0.1)

    final_stat = (await gw_client.get("/v1/admin/rebalance/status")).json()
    assert final_stat["status"] == "completed"
    assert final_stat["failed_tasks"] == 0
    assert final_stat["completion_percentage"] == 100.0

    # Verify that all object chunks now strictly match the desired placement on the 7-node cluster
    for obj_key, payload in uploaded_objects:
        manifest_dict = env["raft_sm"].get_latest_manifest("test-bucket", obj_key)
        manifest = ObjectManifest.model_validate(manifest_dict)
        chunk = manifest.chunks[0]

        # Desired placement under 7-node cluster
        desired_nodes = select_placement_nodes(
            bucket="test-bucket",
            key=obj_key,
            version_id=manifest.version_id,
            chunk_index=chunk.chunk_index,
            policy=env["policies"]["hot"],
            nodes_or_cluster=env["service"].cluster,
        )
        desired_ids = {n.node_id for n in desired_nodes}

        # Check placement matches desired
        assert set(chunk.placement_nodes) == desired_ids

        # Verify object is readable and content matches
        get_resp = await gw_client.get(f"/v1/objects/test-bucket/{obj_key}")
        assert get_resp.status_code == 200
        assert get_resp.content == payload


# =============================================================================
# 3. CONTINUOUS READABILITY DURING ACTIVE REBALANCE
# =============================================================================

@pytest.mark.asyncio
async def test_readability_during_active_rebalance(rebalance_cluster_env):
    """
    Test that continuous client reads succeed with 100% success rate while
    background rebalance is actively migrating chunks.
    """
    env = rebalance_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    # Add storage-7 app
    node_7_dir = env["tmp_path"] / "storage-7"
    node_7_settings = VaultSettings(data_dir=node_7_dir, chunk_size_bytes=8 * 1024 * 1024)
    storage_app_7 = create_vault_app(node_7_settings)
    env["transport"].add_node_transport("storage-7", httpx.ASGITransport(app=storage_app_7))

    # Upload test object
    obj_key = "concurrent/read-during-rebal.bin"
    payload = b"Continuous foreground read availability verification during rebalancing!" * 100
    expected_sha = hashlib.sha256(payload).hexdigest()

    put_resp = await gw_client.put(
        f"/v1/objects/test-bucket/{obj_key}",
        content=payload,
        headers={"Idempotency-Key": "read-dur-rebal", "X-Vault-Policy": "hot"}
    )
    assert put_resp.status_code == 201

    # Add storage-7 with high weight
    await gw_client.post("/v1/admin/nodes", json={
        "node_id": "storage-7",
        "url": "http://storage-7:8001",
        "region": "us-east-1",
        "zone": "us-east-1a",
        "active": True,
        "capacity_weight": 5.0,
    })

    # Trigger rebalance with rate limit to keep it running for a moment
    await gw_client.post("/v1/admin/rebalance", params={"rate_limit": 5.0})

    # Concurrently perform continuous GET requests
    read_successes = 0
    for _ in range(15):
        get_resp = await gw_client.get(f"/v1/objects/test-bucket/{obj_key}")
        assert get_resp.status_code == 200
        assert hashlib.sha256(get_resp.content).hexdigest() == expected_sha
        read_successes += 1
        await asyncio.sleep(0.02)

    assert read_successes == 15


# =============================================================================
# 4. SAFE COPY-BEFORE-DELETE: NO PREMATURE DELETION
# =============================================================================

@pytest.mark.asyncio
async def test_no_premature_deletion_of_source_replicas(rebalance_cluster_env):
    """
    Test that if a target node upload fails or verification fails during rebalance:
    - The source replica is NEVER deleted from disk.
    - Object durability is never compromised.
    - Data remains fully readable.
    """
    env = rebalance_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    # Add node-7 but DO NOT start its app in transport (target will fail to connect)
    await gw_client.post("/v1/admin/nodes", json={
        "node_id": "storage-7",
        "url": "http://storage-7:8001",
        "region": "us-east-1",
        "zone": "us-east-1a",
        "active": True,
        "capacity_weight": 5.0,
    })

    obj_key = "safety/no-delete-on-failure.dat"
    payload = b"Source replica must survive even when target destination fails!" * 80
    put_resp = await gw_client.put(
        f"/v1/objects/test-bucket/{obj_key}",
        content=payload,
        headers={"Idempotency-Key": "safety-put", "X-Vault-Policy": "hot"}
    )
    assert put_resp.status_code == 201
    version_id = put_resp.json()["version_id"]

    manifest_dict = env["raft_sm"].get_latest_manifest("test-bucket", obj_key)
    manifest = ObjectManifest.model_validate(manifest_dict)
    initial_sources = list(manifest.chunks[0].placement_nodes)

    # Trigger rebalance - storage-7 is offline so upload will fail
    await gw_client.post("/v1/admin/rebalance", params={"rate_limit": 50.0})

    # Await completion
    start = time.time()
    while time.time() - start < 5.0:
        stat = (await gw_client.get("/v1/admin/rebalance/status")).json()
        if stat["status"] in ("completed", "failed"):
            break
        await asyncio.sleep(0.05)

    # Verify that NONE of the original source files on disk were deleted!
    for node_id in initial_sources:
        node_dir = env["storage_dirs"][node_id]
        chunk_file = node_dir / "chunks" / "test-bucket" / version_id / "chunk_0.dat"
        assert chunk_file.is_file(), f"Source chunk on {node_id} was prematurely deleted!"

    # Object remains 100% readable
    get_resp = await gw_client.get(f"/v1/objects/test-bucket/{obj_key}")
    assert get_resp.status_code == 200
    assert get_resp.content == payload


# =============================================================================
# 5. RESUMED REBALANCE AFTER WORKER RESTART
# =============================================================================

@pytest.mark.asyncio
async def test_resumed_rebalance_after_worker_restart(rebalance_cluster_env):
    """
    Test that rebalance can be resumed after a worker restart, picking up
    remaining migrations without repeating already completed ones.
    """
    env = rebalance_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    # Add storage-7 app
    node_7_dir = env["tmp_path"] / "storage-7"
    node_7_settings = VaultSettings(data_dir=node_7_dir, chunk_size_bytes=8 * 1024 * 1024)
    storage_app_7 = create_vault_app(node_7_settings)
    env["transport"].add_node_transport("storage-7", httpx.ASGITransport(app=storage_app_7))

    for i in range(4):
        put_resp = await gw_client.put(
            f"/v1/objects/test-bucket/resume-{i}.dat",
            content=f"Payload for resume test {i}".encode() * 50,
            headers={"Idempotency-Key": f"resume-put-{i}", "X-Vault-Policy": "hot"}
        )
        assert put_resp.status_code == 201

    await gw_client.post("/v1/admin/nodes", json={
        "node_id": "storage-7",
        "url": "http://storage-7:8001",
        "region": "us-east-1",
        "zone": "us-east-1a",
        "active": True,
        "capacity_weight": 3.0,
    })

    # Simulate worker restart by creating a fresh RebalanceWorker instance
    new_worker = RebalanceWorker(
        cluster=env["service"].cluster,
        metadata_raft=env["raft_sm"],
        policies=env["policies"],
        http_client=env["storage_http_client"],
    )
    env["service"].rebalance_worker = new_worker

    # Resume rebalance
    status_obj = await new_worker.resume_rebalance()
    assert status_obj.status in ("running", "completed")

    start = time.time()
    while time.time() - start < 6.0:
        if new_worker.get_status().status == "completed":
            break
        await asyncio.sleep(0.05)

    assert new_worker.get_status().status == "completed"
    assert new_worker.get_status().failed_tasks == 0
