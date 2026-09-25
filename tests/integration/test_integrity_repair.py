"""Integration tests for disk integrity scanning, corruption quarantine, read-repair, and automatic background replica repair."""

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
from vault_core.manifest import ObjectManifest, ReplicaState
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.metrics import (
    CHUNKS_VERIFIED_TOTAL,
    CORRUPT_CHUNKS_TOTAL,
    LAST_FULL_SCAN_AT,
    QUARANTINED_CHUNKS_TOTAL,
    REPAIR_BACKLOG,
    REPAIR_DURATION_SECONDS,
    REPAIR_FAILURE_TOTAL,
    REPAIR_SUCCESS_TOTAL,
)
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.scanner import IntegrityScanner
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
    storage_scanners = {}
    for node in storage_configs:
        node_dir = tmp_path / node.node_id
        storage_dirs[node.node_id] = node_dir
        node_settings = VaultSettings(data_dir=node_dir, chunk_size_bytes=8 * 1024 * 1024)
        storage_app = create_vault_app(node_settings)
        storage_apps[node.node_id] = httpx.ASGITransport(app=storage_app)
        storage_scanners[node.node_id] = storage_app.state.scanner

    transport = MultiNodeHttpTransport(storage_apps)
    storage_http_client = httpx.AsyncClient(transport=transport, timeout=3.0)

    raft_dir = tmp_path / "raft_meta"
    raft_port = 27000 + (int(time.time() * 100) % 2000)
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
            "storage_scanners": storage_scanners,
            "storage_apps": storage_apps,
        }
    finally:
        raft_sm.destroy()


# =============================================================================
# 1. DISK CORRUPTION, SCANNER DETECTION & QUARANTINE
# =============================================================================

@pytest.mark.asyncio
async def test_disk_corruption_scanner_detection_and_quarantine(cluster_env):
    """
    Test that bit rot directly induced on disk is detected by the storage node's
    integrity scanner, the corrupt chunk is quarantined, and the node refuses
    to serve the corrupted chunk.
    """
    gw_transport = cluster_env["gateway_transport"]
    storage_dirs = cluster_env["storage_dirs"]
    storage_scanners = cluster_env["storage_scanners"]

    bucket = "integrity-test"
    key = "critical-data.bin"
    payload = b"CRITICAL_HEALTHCARE_RECORD_DATA_12345"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Upload object
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=payload,
            headers={"X-Idempotency-Key": "integrity-put-1"}
        )
        assert put_resp.status_code == 201
        v_id = put_resp.headers["X-Vault-Version-Id"]

        # 2. Audit to find target placement node
        audit_resp = await client.get(f"/v1/objects/{bucket}/{key}/audit")
        assert audit_resp.status_code == 200
        target_node = audit_resp.json()["replicas"][0]["node_id"]

        # 3. Verify chunk exists on target node before corruption
        chunk_path = storage_dirs[target_node] / "chunks" / bucket / v_id / "chunk_0.dat"
        sha_path = storage_dirs[target_node] / "chunks" / bucket / v_id / "chunk_0.sha256"
        assert chunk_path.is_file()
        assert sha_path.is_file()

        # 4. Inject bit rot directly on disk
        chunk_path.write_bytes(b"CORRUPTED_BIT_ROT_SECTOR_DATA")

        # 5. Run full integrity scan on target node
        scanner: IntegrityScanner = storage_scanners[target_node]
        scan_res = scanner.scan_once()

        assert scan_res["corrupt_detected"] >= 1
        assert scan_res["quarantined"] >= 1
        assert scan_res["duration_seconds"] >= 0

        # 6. Verify corrupt chunk was quarantined and removed from active chunks directory
        assert not chunk_path.is_file(), "Corrupted chunk must be removed from active chunks dir"

        quarantine_dir = storage_dirs[target_node] / "quarantine"
        quarantined_files = list(quarantine_dir.glob("*_corrupt_*.dat"))
        assert len(quarantined_files) >= 1, "Quarantine directory must contain the quarantined chunk"


# =============================================================================
# 2. READ-REPAIR ENQUEUING AND REPLICA RESTORATION
# =============================================================================

@pytest.mark.asyncio
async def test_read_repair_enqueues_and_restores_replica(cluster_env):
    """
    Test read-repair workflow:
    1. Object uploaded under Hot policy (RF=3).
    2. One replica is corrupted on disk.
    3. Client GET succeeds by reading surviving replica and enqueues corrupt replica.
    4. Repair worker executes repair: verifies source hash, writes chunk to target, verifies destination hash.
    5. Restored replica count returns to full replication factor (healthy_count == 3).
    """
    gw_transport = cluster_env["gateway_transport"]
    storage_dirs = cluster_env["storage_dirs"]
    gw_service: GatewayService = cluster_env["service"]

    bucket = "repair-test"
    key = "documents/tax_filing.pdf"
    payload = b"TAX_FILING_PAYLOAD_WITH_IMMUTABLE_HASH"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Upload object
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=payload,
            headers={"X-Idempotency-Key": "read-repair-init"}
        )
        assert put_resp.status_code == 201
        v_id = put_resp.headers["X-Vault-Version-Id"]

        # 2. Audit initial healthy count
        audit1 = await client.get(f"/v1/objects/{bucket}/{key}/audit")
        assert audit1.json()["healthy_count"] == 3
        corrupt_node = audit1.json()["replicas"][0]["node_id"]

        # 3. Corrupt one replica on disk
        chunk_file = storage_dirs[corrupt_node] / "chunks" / bucket / v_id / "chunk_0.dat"
        chunk_file.write_bytes(b"CORRUPTED_SILENT_ERROR")

        # 4. Client GET reads object (read repair triggers in background)
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 200
        assert get_resp.content == payload

        # Allow background read repair task to enqueue
        await asyncio.sleep(0.05)

        # 5. Verify repair worker has queued task and execute repair
        assert gw_service.repair_worker is not None
        assert gw_service.repair_worker._queue.qsize() >= 1

        processed = await gw_service.repair_worker.run_repair_cycle()
        assert processed >= 1

        # 6. Verify restored replica count
        audit2 = await client.get(f"/v1/objects/{bucket}/{key}/audit")
        report = audit2.json()
        assert report["healthy_count"] == 3
        assert report["corrupt_count"] == 0
        assert report["missing_count"] == 0

        # Verify disk chunk on corrupt_node now has exact verified payload
        repaired_file = storage_dirs[corrupt_node] / "chunks" / bucket / v_id / "chunk_0.dat"
        assert repaired_file.is_file()
        assert repaired_file.read_bytes() == payload


# =============================================================================
# 3. CLUSTER AUDIT & RATE-LIMITED PRIORITIZED REPAIR
# =============================================================================

@pytest.mark.asyncio
async def test_cluster_audit_and_prioritized_repair_workflow(cluster_env):
    """
    Test cluster-wide audit and priority ordering:
    - Object A has 1 corrupt replica (surviving = 2, priority 2).
    - Object B has 2 missing/corrupt replicas (surviving = 1, priority 1 - CRITICAL).
    - Audit enqueues all degraded replicas.
    - Object B is processed before Object A.
    - Full replica counts are restored across all objects.
    """
    gw_transport = cluster_env["gateway_transport"]
    storage_dirs = cluster_env["storage_dirs"]
    gw_service: GatewayService = cluster_env["service"]

    bucket = "prioritized-repair"
    key_a = "dataset-a.bin"
    key_b = "dataset-b.bin"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # Upload Object A and Object B
        put_a = await client.put(f"/v1/objects/{bucket}/{key_a}", content=b"DATA_SET_A_CONTENT", headers={"X-Idempotency-Key": "put-a"})
        put_b = await client.put(f"/v1/objects/{bucket}/{key_b}", content=b"DATA_SET_B_CONTENT", headers={"X-Idempotency-Key": "put-b"})
        assert put_a.status_code == 201 and put_b.status_code == 201

        v_a = put_a.headers["X-Vault-Version-Id"]
        v_b = put_b.headers["X-Vault-Version-Id"]

        audit_a = (await client.get(f"/v1/objects/{bucket}/{key_a}/audit")).json()
        audit_b = (await client.get(f"/v1/objects/{bucket}/{key_b}/audit")).json()

        # Object A: corrupt 1 node
        node_a_1 = audit_a["replicas"][0]["node_id"]
        (storage_dirs[node_a_1] / "chunks" / bucket / v_a / "chunk_0.dat").write_bytes(b"BAD_A")

        # Object B: corrupt 2 nodes (surviving = 1, highest priority!)
        node_b_1 = audit_b["replicas"][0]["node_id"]
        node_b_2 = audit_b["replicas"][1]["node_id"]
        (storage_dirs[node_b_1] / "chunks" / bucket / v_b / "chunk_0.dat").write_bytes(b"BAD_B1")
        (storage_dirs[node_b_2] / "chunks" / bucket / v_b / "chunk_0.dat").unlink()

        # Trigger cluster audit via admin endpoint
        scan_resp = await client.post("/v1/admin/repair/scan")
        assert scan_resp.status_code == 200
        assert scan_resp.json()["enqueued_repairs"] >= 3

        # Verify priority queue order: Object B items must pop before Object A
        q = gw_service.repair_worker._queue
        first_task = await q.get()
        assert first_task.priority == 1, "Under-replicated Object B (surviving=1) must pop first!"
        assert first_task.key == key_b

        # Put first task back and run complete repair cycle
        await q.put(first_task)
        processed = await gw_service.repair_worker.run_repair_cycle()
        assert processed >= 3

        # Verify all objects restored to 100% healthy replication
        audit_a_after = (await client.get(f"/v1/objects/{bucket}/{key_a}/audit")).json()
        audit_b_after = (await client.get(f"/v1/objects/{bucket}/{key_b}/audit")).json()

        assert audit_a_after["healthy_count"] == 3
        assert audit_b_after["healthy_count"] == 3


# =============================================================================
# 4. METRICS EXPOSITION VERIFICATION
# =============================================================================

@pytest.mark.asyncio
async def test_metrics_exposition_integrity_and_repair(cluster_env):
    """
    Verify all required metrics are exposed in Prometheus format:
    - last_full_scan_at
    - chunks_verified_total
    - corrupt_chunks_total
    - quarantined_chunks_total
    - repair_backlog
    - repair_success_total
    - repair_failure_total
    - repair_duration_seconds
    """
    gw_transport = cluster_env["gateway_transport"]

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        text = resp.text

        expected_metrics = [
            "last_full_scan_at",
            "chunks_verified_total",
            "corrupt_chunks_total",
            "quarantined_chunks_total",
            "repair_backlog",
            "repair_success_total",
            "repair_failure_total",
            "repair_duration_seconds",
        ]

        for m in expected_metrics:
            assert m in text, f"Expected metric '{m}' to be present in /metrics output"
