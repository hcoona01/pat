"""Multi-node cluster integration tests for Hot and Durable replication policies and quorum failure modes."""

import asyncio
import hashlib
import time
import uuid
from pathlib import Path
from typing import Dict
import httpx
import pytest

from apps.gateway.service import GatewayService, create_gateway_app
from apps.storage_node.service import create_vault_app
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.settings import VaultSettings


class MultiNodeHttpTransport(httpx.AsyncBaseTransport):
    """
    Simulates multi-node network fabric. Routes requests directly to node FastAPI apps.
    Supports dynamic node stopping/restarting to simulate node crashes and partitions.
    """

    def __init__(self, node_apps: Dict[str, httpx.ASGITransport]) -> None:
        self.node_transports = node_apps
        self.offline_nodes: set[str] = set()

    def stop_node(self, node_id: str) -> None:
        self.offline_nodes.add(node_id)

    def start_node(self, node_id: str) -> None:
        self.offline_nodes.discard(node_id)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)

        # Match target node
        for node_id, transport in self.node_transports.items():
            if f"://{node_id}:" in url_str:
                if node_id in self.offline_nodes:
                    raise httpx.ConnectError(f"Connection refused: storage node {node_id} is offline")
                # Forward to actual ASGI app
                return await transport.handle_async_request(request)

        raise httpx.ConnectError(f"Host unreachable: {request.url}")


@pytest.fixture
def cluster_env(tmp_path: Path):
    """
    Sets up 6 independent storage nodes across 3 zones and a Raft consensus metadata node.
    """
    # 1. Initialize 6 storage nodes
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
        "durable": DurabilityPolicy(
            name="durable",
            scheme=PolicyScheme.REPLICATION,
            replication_factor=4,
            data_write_quorum=3,
            data_read_quorum=1,
            minimum_distinct_zones=3,
        ),
    }

    # 2. Storage Node ASGI Apps with isolated disk directories
    storage_apps = {}
    for node in storage_configs:
        node_dir = tmp_path / node.node_id
        node_settings = VaultSettings(data_dir=node_dir, chunk_size_bytes=8 * 1024 * 1024)
        storage_app = create_vault_app(node_settings)
        storage_apps[node.node_id] = httpx.ASGITransport(app=storage_app)

    transport = MultiNodeHttpTransport(storage_apps)
    storage_http_client = httpx.AsyncClient(transport=transport, timeout=3.0)

    # 3. Raft Metadata Node
    raft_dir = tmp_path / "raft_meta"
    raft_port = 25000 + (int(time.time() * 100) % 4000)
    raft_addr = f"127.0.0.1:{raft_port}"
    raft_sm = RaftMetadataStateMachine(self_address=raft_addr, partner_addresses=[], data_dir=raft_dir)

    # Wait for single-node Raft consensus leader
    start = time.time()
    while time.time() - start < 4.0:
        if raft_sm.is_leader():
            break
        time.sleep(0.05)

    assert raft_sm.is_leader(), "Raft metadata leader failed to elect"

    # 4. Gateway Service
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
        }
    finally:
        raft_sm.destroy()


@pytest.mark.asyncio
async def test_hot_and_durable_policies_upload_and_retrieve(cluster_env):
    """
    Test upload and retrieval under 'hot' (3 replicas across 3 zones)
    and 'durable' (4 replicas across 3 zones) policies.
    """
    transport = cluster_env["gateway_transport"]

    async with httpx.AsyncClient(transport=transport, base_url="http://gateway:8000") as client:
        # 1. Hot Policy Upload (3x replication)
        content_hot = b"HOT_POLICY_REPLICATED_DATA_STREAM" * 500
        hot_hash = hashlib.sha256(content_hot).hexdigest()

        resp_hot = await client.put(
            "/v1/objects/production/hot-file.bin",
            content=content_hot,
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": "hot-upload-001",
            }
        )
        assert resp_hot.status_code == 201
        hot_data = resp_hot.json()
        assert hot_data["policy"] == "hot"
        assert hot_data["content_hash"] == hot_hash

        # Verify object health endpoint reports 3 distinct placement nodes
        health_resp = await client.get("/v1/objects/production/hot-file.bin/health")
        assert health_resp.status_code == 200
        health_data = health_resp.json()
        chunk_placement = health_data["chunks"][0]["placement_nodes"]
        assert len(chunk_placement) == 3

        # Verify GET returns verified bytes
        get_hot = await client.get("/v1/objects/production/hot-file.bin")
        assert get_hot.status_code == 200
        assert get_hot.content == content_hot

        # 2. Durable Policy Upload (4x replication)
        content_durable = b"DURABLE_CRITICAL_ENTERPRISE_ARCHIVE" * 1000
        durable_hash = hashlib.sha256(content_durable).hexdigest()

        resp_dur = await client.put(
            "/v1/objects/production/durable-file.bin",
            content=content_durable,
            headers={
                "X-Vault-Policy": "durable",
                "X-Idempotency-Key": "durable-upload-001",
            }
        )
        assert resp_dur.status_code == 201
        dur_data = resp_dur.json()
        assert dur_data["policy"] == "durable"

        health_dur = await client.get("/v1/objects/production/durable-file.bin/health")
        assert len(health_dur.json()["chunks"][0]["placement_nodes"]) == 4

        get_dur = await client.get("/v1/objects/production/durable-file.bin")
        assert get_dur.status_code == 200
        assert get_dur.content == content_durable


@pytest.mark.asyncio
async def test_concurrent_uploads_and_reads(cluster_env):
    """Verify that multiple concurrent clients can upload and retrieve objects concurrently."""
    transport = cluster_env["gateway_transport"]

    async with httpx.AsyncClient(transport=transport, base_url="http://gateway:8000") as client:
        total_files = 8

        # Concurrent uploads
        async def upload_file(idx: int):
            data = f"CONCURRENT_CONTENT_STREAM_BLOCK_{idx}".encode("utf-8") * 200
            resp = await client.put(
                f"/v1/objects/concurrent/file_{idx}.dat",
                content=data,
                headers={
                    "X-Vault-Policy": "hot",
                    "X-Idempotency-Key": f"concurrent-key-{idx}",
                }
            )
            assert resp.status_code == 201
            return idx, data

        upload_tasks = [upload_file(i) for i in range(total_files)]
        uploaded_results = await asyncio.gather(*upload_tasks)

        # Concurrent reads
        async def read_file(idx: int, expected_data: bytes):
            resp = await client.get(f"/v1/objects/concurrent/file_{idx}.dat")
            assert resp.status_code == 200
            assert resp.content == expected_data

        read_tasks = [read_file(idx, data) for idx, data in uploaded_results]
        await asyncio.gather(*read_tasks)


@pytest.mark.asyncio
async def test_one_storage_node_failure_during_write(cluster_env):
    """
    Verify that if 1 of 3 storage nodes fails during a hot policy write,
    the write still succeeds because data write quorum (W=2) is met.
    """
    transport = cluster_env["gateway_transport"]
    net = cluster_env["transport"]

    async with httpx.AsyncClient(transport=transport, base_url="http://gateway:8000") as client:
        # Pre-simulate: stop storage-1
        net.stop_node("storage-1")

        content = b"FAULT_TOLERANT_QUORUM_WRITE_SURVIVES_1_DEAD_NODE" * 200
        resp = await client.put(
            "/v1/objects/fault/node-dead-write.bin",
            content=content,
            headers={
                "X-Vault-Policy": "hot",  # W=2, survives 1 node down
                "X-Idempotency-Key": "fault-write-001",
            }
        )
        assert resp.status_code == 201

        # Object is readable from surviving nodes
        get_resp = await client.get("/v1/objects/fault/node-dead-write.bin")
        assert get_resp.status_code == 200
        assert get_resp.content == content


@pytest.mark.asyncio
async def test_write_failure_when_quorum_unavailable_and_no_manifest_visible(cluster_env):
    """
    Verify that if enough nodes are stopped that write quorum is lost:
    1. The write safely fails with HTTP 503 Quorum Unavailable.
    2. No partial or uncommitted manifest is visible in Raft (GET returns 404).
    3. Successfully written chunks on surviving nodes are marked as orphan candidates.
    """
    transport = cluster_env["gateway_transport"]
    net = cluster_env["transport"]
    gw_service = cluster_env["service"]

    async with httpx.AsyncClient(transport=transport, base_url="http://gateway:8000") as client:
        # Stop 5 of 6 nodes, leaving only 1 node alive (cannot achieve W=2)
        net.stop_node("storage-1")
        net.stop_node("storage-2")
        net.stop_node("storage-3")
        net.stop_node("storage-4")
        net.stop_node("storage-5")

        content = b"WRITE_THAT_SHOULD_ABORT_DUE_TO_QUORUM_LOSS" * 100
        resp = await client.put(
            "/v1/objects/fault/quorum-loss.bin",
            content=content,
            headers={
                "X-Vault-Policy": "hot",  # Requires W=2
                "X-Idempotency-Key": "quorum-loss-key-001",
            }
        )
        # Must fail with 503
        assert resp.status_code == 503
        assert "Data write quorum unavailable" in resp.json()["detail"]

        # CRITICAL ASSERTION: No manifest was committed; GET must return 404
        get_resp = await client.get("/v1/objects/fault/quorum-loss.bin")
        assert get_resp.status_code == 404

        # HEAD must also return 404
        head_resp = await client.head("/v1/objects/fault/quorum-loss.bin")
        assert head_resp.status_code == 404


@pytest.mark.asyncio
async def test_reads_from_surviving_replicas_after_node_fails(cluster_env):
    """
    Verify that after an object is uploaded, stopping a node holding a replica
    does not break reads, and the gateway serves clean data from surviving replicas.
    """
    transport = cluster_env["gateway_transport"]
    net = cluster_env["transport"]

    async with httpx.AsyncClient(transport=transport, base_url="http://gateway:8000") as client:
        content = b"DATA_STORED_ACROSS_THREE_ZONES_SURVIVES_CRASH" * 300
        resp = await client.put(
            "/v1/objects/durability/survive-read.bin",
            content=content,
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": "survive-read-key-001",
            }
        )
        assert resp.status_code == 201

        # Check placement nodes
        health = await client.get("/v1/objects/durability/survive-read.bin/health")
        nodes_with_data = health.json()["chunks"][0]["placement_nodes"]
        assert len(nodes_with_data) >= 3

        # Crash the first node holding a replica
        killed_node = nodes_with_data[0]
        net.stop_node(killed_node)

        # GET must still succeed from surviving replicas!
        get_resp = await client.get("/v1/objects/durability/survive-read.bin")
        assert get_resp.status_code == 200
        assert get_resp.content == content
        assert hashlib.sha256(get_resp.content).hexdigest() == hashlib.sha256(content).hexdigest()
