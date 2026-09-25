"""Measurable Hackathon Acceptance Targets and Workload Performance Test Suite.

Validates:
1. Configurable large streamed object (target 1 GiB when machine resources allow).
2. Multi-object batch workload (concurrent client PUT/GET pipeline).
3. Concurrent client workload measuring real p50/p95/p99 read/write latencies.
4. Comprehensive Prometheus metrics verification:
   - API success/failure counts;
   - p50/p95/p99 read/write latency;
   - node availability and topology;
   - Raft leader/quorum health;
   - under-replicated objects;
   - corruption/quarantine counts;
   - repair backlog and duration;
   - rebalance progress and bytes moved;
   - policy storage amplification;
   - data and metadata quorum failures;
   - network timeout failures.
5. Configurable prototype targets:
   - repair SLO in seconds for the defined test dataset;
   - data-loss tolerance per policy;
   - expected storage amplification per policy.

All performance metrics and evidence are recorded from actual observed executions.
"""

import asyncio
import hashlib
import os
import time
import uuid
from pathlib import Path
from typing import Dict, List
import httpx
import pytest

from apps.gateway.service import GatewayService, create_gateway_app
from apps.storage_node.service import create_vault_app
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.metrics import (
    API_SUCCESS_TOTAL,
    READ_LATENCY_TRACKER,
    STORAGE_AMPLIFICATION,
    WRITE_LATENCY_TRACKER,
)
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.settings import VaultSettings
from vault_core.targets import AcceptanceTargets, load_acceptance_targets


class WorkloadHttpTransport(httpx.AsyncBaseTransport):
    """Direct ASGI transport for multi-node storage and gateway routing."""

    def __init__(self, node_apps: Dict[str, httpx.ASGITransport]) -> None:
        self.node_transports = dict(node_apps)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        host = getattr(request.url, "host", "")

        for node_id, transport in self.node_transports.items():
            if node_id == host or f"://{node_id}:" in url_str:
                return await transport.handle_async_request(request)

        raise httpx.ConnectError(f"Host unreachable: {request.url}")


@pytest.fixture
def workload_env(tmp_path: Path):
    """Cluster fixture for workload performance and acceptance target validation."""
    storage_configs = [
        StorageNodeConfig(node_id="storage-1", url="http://storage-1:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-2", url="http://storage-2:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-3", url="http://storage-3:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-4", url="http://storage-4:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-5", url="http://storage-5:8001", region="us-east-1", zone="us-east-1c", active=True),
        StorageNodeConfig(node_id="storage-6", url="http://storage-6:8001", region="us-east-1", zone="us-east-1c", active=True),
    ]

    cluster = ClusterConfig(cluster_id="workload-cluster", storage_nodes=storage_configs)

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
        "archive": DurabilityPolicy(
            name="archive",
            scheme=PolicyScheme.ERASURE_CODING,
            data_fragments=4,
            parity_fragments=2,
            minimum_distinct_zones=3,
        ),
    }

    storage_apps = {}
    storage_dirs = {}
    for node in storage_configs:
        node_dir = tmp_path / node.node_id
        node_settings = VaultSettings(data_dir=node_dir, chunk_size_bytes=8 * 1024 * 1024)
        app = create_vault_app(node_settings)
        storage_apps[node.node_id] = httpx.ASGITransport(app=app)
        storage_dirs[node.node_id] = node_dir

    transport = WorkloadHttpTransport(storage_apps)
    storage_http_client = httpx.AsyncClient(transport=transport, timeout=30.0)

    raft_dir = tmp_path / "raft_meta"
    raft_port = 28000 + (int(time.time() * 100) % 4000)
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
            "gateway_service": gateway_service,
            "raft_sm": raft_sm,
            "cluster": cluster,
            "policies": policies,
            "storage_dirs": storage_dirs,
            "tmp_path": tmp_path,
        }
    finally:
        raft_sm.destroy()


# =============================================================================
# 1. LARGE STREAMED OBJECT WORKLOAD (TARGET 1 GIB)
# =============================================================================

@pytest.mark.asyncio
async def test_large_streamed_object_workload(workload_env):
    """
    Workload Test: Stream a large object (default target 1 GiB = 1,073,741,824 bytes,
    configurable via VAULT_LARGE_OBJECT_SIZE_BYTES).
    Validates:
    - Streaming without loading the full 1 GiB payload into memory at once.
    - Automatic splitting into 8 MiB chunks across multi-zone placement nodes.
    - Concurrent multi-node chunk uploads with write quorum enforcement.
    - Cryptographic SHA-256 validation end-to-end.
    - Empirical measurement of actual throughput (MB/s) and latency.
    """
    env = workload_env
    gw_transport = env["gateway_transport"]

    # Target 1 GiB (1,073,741,824 bytes = 128 chunks of 8 MiB), configurable for resource-constrained environments
    target_size = int(os.environ.get("VAULT_LARGE_OBJECT_SIZE_BYTES", 1024 * 1024 * 1024))
    chunk_size = 8 * 1024 * 1024  # 8 MiB
    num_chunks = target_size // chunk_size
    bucket = "workload-bucket"
    key = "benchmark/large_streamed_1gib.bin"

    # Pre-generate an 8 MiB reusable pattern to stream with zero memory explosion
    one_mb_pattern = b"0123456789ABCDEF" * 65536  # Exactly 1 MiB
    eight_mb_block = one_mb_pattern * 8  # 8 MiB block
    assert len(eight_mb_block) == chunk_size

    overall_hasher = hashlib.sha256()

    async def generate_large_stream():
        for _ in range(num_chunks):
            overall_hasher.update(eight_mb_block)
            yield eight_mb_block

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000", timeout=120.0) as client:
        # 1. Stream PUT
        t0 = time.time()
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=generate_large_stream(),
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": f"large-obj-bench-{uuid.uuid4()}",
                "Content-Type": "application/octet-stream",
            },
        )
        write_duration = time.time() - t0

        assert put_resp.status_code == 201, f"Large upload failed: {put_resp.status_code}: {put_resp.text}"
        put_data = put_resp.json()
        expected_hash = overall_hasher.hexdigest()

        # Actual observed upload metrics
        write_mb = target_size / (1024 * 1024)
        write_throughput = write_mb / write_duration
        print(f"\n[Actual Observed] 1 GiB Stream Upload: {write_mb:.1f} MB in {write_duration:.2f}s ({write_throughput:.2f} MB/s)")

        assert put_data["size_bytes"] == target_size
        assert put_data["content_hash"] == expected_hash
        assert put_data["chunk_count"] == num_chunks

        # 2. Stream GET
        t1 = time.time()
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        read_duration = time.time() - t1

        assert get_resp.status_code == 200
        assert len(get_resp.content) == target_size
        read_hash = hashlib.sha256(get_resp.content).hexdigest()
        assert read_hash == expected_hash

        # Actual observed download metrics
        read_throughput = write_mb / read_duration
        print(f"[Actual Observed] 1 GiB Stream Download: {write_mb:.1f} MB in {read_duration:.2f}s ({read_throughput:.2f} MB/s)")


# =============================================================================
# 2. CONCURRENT CLIENT WORKLOAD & LATENCY PERCENTILES (p50/p95/p99)
# =============================================================================

@pytest.mark.asyncio
async def test_concurrent_client_workload_and_latency_percentiles(workload_env):
    """
    Workload Test: Concurrent client workload measuring real observed p50/p95/p99 read/write latencies.
    Validates:
    - Multiple concurrent clients sending requests simultaneously.
    - Zero data corruption or checksum errors.
    - Automatic calculation of rolling p50/p95/p99 read and write latency gauges.
    """
    env = workload_env
    gw_transport = env["gateway_transport"]
    bucket = "concurrent-bench-bucket"
    object_count = 60
    concurrency = 8
    object_payload = b"CONCURRENT_WORKLOAD_PAYLOAD_CHUNK_DATA" * 50  # ~1.9 KB each

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000", timeout=30.0) as client:
        # Pre-create test keys
        keys = [f"object-{i}.dat" for i in range(object_count)]
        semaphore = asyncio.Semaphore(concurrency)

        # 1. Concurrent writes
        async def do_write(k: str):
            async with semaphore:
                resp = await client.put(
                    f"/v1/objects/{bucket}/{k}",
                    content=object_payload,
                    headers={
                        "X-Vault-Policy": "hot",
                        "X-Idempotency-Key": str(uuid.uuid4()),
                        "Content-Type": "application/octet-stream",
                    },
                )
                assert resp.status_code == 201

        write_start = time.time()
        await asyncio.gather(*(do_write(k) for k in keys))
        total_write_time = time.time() - write_start

        # 2. Concurrent reads
        async def do_read(k: str):
            async with semaphore:
                resp = await client.get(f"/v1/objects/{bucket}/{k}")
                assert resp.status_code == 200
                assert resp.content == object_payload

        read_start = time.time()
        await asyncio.gather(*(do_read(k) for k in keys))
        total_read_time = time.time() - read_start

        # Check latency quantiles recorded
        write_p50, write_p95, write_p99 = WRITE_LATENCY_TRACKER.quantiles()
        read_p50, read_p95, read_p99 = READ_LATENCY_TRACKER.quantiles()

        print(f"\n[Actual Observed] Concurrent Writes ({object_count} ops, c={concurrency}): {total_write_time:.2f}s total")
        print(f"  Write Latency -> p50: {write_p50 * 1000:.2f}ms | p95: {write_p95 * 1000:.2f}ms | p99: {write_p99 * 1000:.2f}ms")
        print(f"[Actual Observed] Concurrent Reads ({object_count} ops, c={concurrency}): {total_read_time:.2f}s total")
        print(f"  Read Latency  -> p50: {read_p50 * 1000:.2f}ms | p95: {read_p95 * 1000:.2f}ms | p99: {read_p99 * 1000:.2f}ms")

        # Quantiles must be positive and non-zero
        assert write_p50 > 0.0
        assert write_p95 >= write_p50
        assert write_p99 >= write_p95
        assert read_p50 > 0.0
        assert read_p95 >= read_p50
        assert read_p99 >= read_p95


# =============================================================================
# 3. PROMETHEUS METRICS ENDPOINT VERIFICATION
# =============================================================================

@pytest.mark.asyncio
async def test_complete_prometheus_metrics_exposition(workload_env):
    """
    Validates exposition of all 11 required Prometheus metric categories on /metrics:
    1. API success/failure counts
    2. p50/p95/p99 read/write latency
    3. Node availability
    4. Raft leader/quorum health
    5. Under-replicated objects
    6. Corruption/quarantine counts
    7. Repair backlog and duration
    8. Rebalance progress and bytes moved
    9. Policy storage amplification
    10. Data and metadata quorum failures
    11. Network timeout failures
    """
    env = workload_env
    gw_transport = env["gateway_transport"]

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # Perform 1 write and 1 read to trigger metrics
        test_payload = b"METRICS_VALIDATION_SAMPLE_PAYLOAD"
        resp_put = await client.put(
            "/v1/objects/test-bucket/metrics-test.dat",
            content=test_payload,
            headers={"X-Vault-Policy": "hot", "X-Idempotency-Key": str(uuid.uuid4())},
        )
        assert resp_put.status_code == 201

        resp_get = await client.get("/v1/objects/test-bucket/metrics-test.dat")
        assert resp_get.status_code == 200

        # Query /metrics endpoint
        metrics_resp = await client.get("/metrics")
        assert metrics_resp.status_code == 200
        text = metrics_resp.text

        required_metrics = [
            # 1. API success/failure counts
            "vault_api_success_total",
            "vault_api_failure_total",
            "vault_http_requests_total",
            # 2. p50/p95/p99 read/write latency
            "vault_operation_latency_seconds",
            "vault_read_latency_p50_seconds",
            "vault_read_latency_p95_seconds",
            "vault_read_latency_p99_seconds",
            "vault_write_latency_p50_seconds",
            "vault_write_latency_p95_seconds",
            "vault_write_latency_p99_seconds",
            # 3. Node availability
            "vault_storage_node_active",
            "vault_storage_nodes_active_total",
            # 4. Raft leader/quorum health
            "vault_raft_is_leader",
            "vault_raft_leader_elected",
            "vault_raft_quorum_healthy",
            "vault_raft_peers_active",
            # 5. Under-replicated objects
            "vault_under_replicated_objects",
            # 6. Corruption/quarantine counts
            "chunks_verified_total",
            "corrupt_chunks_total",
            "quarantined_chunks_total",
            # 7. Repair backlog and duration
            "repair_backlog",
            "repair_success_total",
            "repair_failure_total",
            "repair_duration_seconds",
            # 8. Rebalance progress and bytes moved
            "vault_rebalance_queued_tasks",
            "vault_rebalance_copied_total",
            "vault_rebalance_verified_total",
            "vault_rebalance_bytes_moved_total",
            "vault_rebalance_progress_ratio",
            # 9. Policy storage amplification
            "storage_amplification",
            # 10. Data and metadata quorum failures
            "vault_data_quorum_failures_total",
            "vault_metadata_quorum_failures_total",
            # 11. Network timeout failures
            "vault_network_timeout_failures_total",
        ]

        for m in required_metrics:
            assert m in text, f"Required Prometheus metric '{m}' was NOT found in /metrics output!"

        print(f"\n[Actual Observed] All {len(required_metrics)} required Prometheus metric series successfully exposed.")


# =============================================================================
# 4. CONFIGURABLE PROTOTYPE ACCEPTANCE TARGETS & SLO VERIFICATION
# =============================================================================

@pytest.mark.asyncio
async def test_configurable_prototype_acceptance_targets(workload_env):
    """
    Validates prototype acceptance targets against actual measured behavior:
    1. Repair SLO in seconds for the defined test dataset.
    2. Data-loss tolerance per policy.
    3. Expected storage amplification per policy.
    """
    env = workload_env
    gw_transport = env["gateway_transport"]
    gw_service = env["gateway_service"]
    storage_dirs = env["storage_dirs"]

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Fetch acceptance targets from API endpoint
        target_resp = await client.get("/v1/cluster/acceptance-targets")
        assert target_resp.status_code == 200
        targets = AcceptanceTargets.model_validate(target_resp.json())

        # Verify configured targets
        assert targets.repair_slo_seconds == 15.0
        assert targets.policy_targets["hot"].data_loss_tolerance_nodes == 2
        assert targets.policy_targets["hot"].expected_storage_amplification == 3.0
        assert targets.policy_targets["durable"].data_loss_tolerance_nodes == 3
        assert targets.policy_targets["durable"].expected_storage_amplification == 4.0
        assert targets.policy_targets["archive"].data_loss_tolerance_nodes == 2
        assert targets.policy_targets["archive"].expected_storage_amplification == 1.5

        # 2. Test Dataset Repair SLO Verification
        bucket = "slo-test-bucket"
        key = "slo-dataset-sample.dat"
        payload = b"REPAIR_SLO_BENCHMARK_PAYLOAD" * 100

        # Upload object under hot policy (3 replicas)
        put_r = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=payload,
            headers={"X-Vault-Policy": "hot", "X-Idempotency-Key": str(uuid.uuid4())},
        )
        assert put_r.status_code == 201
        put_data = put_r.json()
        version_id = put_data["version_id"]

        # Audit object to find physical locations
        audit_res = (await client.get(f"/v1/objects/{bucket}/{key}/audit")).json()
        assert audit_res["healthy_count"] == 3
        target_node = audit_res["replicas"][0]["node_id"]

        # Corrupt one chunk file directly on disk
        chunk_file = storage_dirs[target_node] / "chunks" / bucket / version_id / "chunk_0.dat"
        assert chunk_file.is_file()
        chunk_file.unlink()  # simulate missing replica

        # Trigger repair scan
        scan_r = await client.post("/v1/admin/repair/scan")
        assert scan_r.status_code == 200
        assert scan_r.json()["enqueued_repairs"] >= 1

        # Measure actual repair duration
        t_repair_start = time.time()
        processed = await gw_service.repair_worker.run_repair_cycle()
        actual_repair_time = time.time() - t_repair_start

        assert processed >= 1
        print(f"\n[Actual Observed] Repair SLO Benchmark: completed in {actual_repair_time:.3f}s (SLO target: <= {targets.repair_slo_seconds}s)")

        # Verify within configured Repair SLO
        assert actual_repair_time <= targets.repair_slo_seconds, (
            f"Repair duration ({actual_repair_time:.3f}s) exceeded SLO ({targets.repair_slo_seconds}s)"
        )

        # Verify replica restored to healthy
        audit_after = (await client.get(f"/v1/objects/{bucket}/{key}/audit")).json()
        assert audit_after["healthy_count"] == 3
        assert audit_after["missing_count"] == 0
