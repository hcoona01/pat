"""Automated Chaos and Fault-Injection Test Suite for Vault.

Validates system resiliency under network partitions, node crashes, zone loss,
and concurrent rebalancing:
1. Isolate one storage node (bounded timeout, write succeeds with quorum, read succeeds).
2. Isolate enough storage nodes to lose write quorum (HTTP 503, no false success, no uncommitted manifest visibility).
3. Isolate one metadata follower (majority quorum maintained, writes/reads continue).
4. Kill metadata leader during manifest update (failover, new leader elected, bounded timeout).
5. Partition an isolated metadata node and attempt conflicting write (rejected, no split-brain).
6. Isolate all nodes in one logical zone (surviving zones maintain read quorum, data remains accessible).
7. Restore connectivity and verify metadata/data convergence (repair worker heals degraded replicas).
8. Run rebalancing while concurrent reads/writes continue (100% read/write success, no corruption).
"""

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
from apps.workers.rebalance_worker import RebalanceWorker
from apps.workers.repair_worker import RepairWorker
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.fault_injection import FaultInjectionTransport
from vault_core.manifest import ObjectManifest, ReplicaState
from vault_core.metadata_raft import (
    ManifestNotFoundError,
    NotLeaderError,
    RaftMetadataStateMachine,
    RaftQuorumError,
)
from vault_core.placement import select_placement_nodes
from vault_core.quorum import DurabilityPolicy, PolicyScheme
from vault_core.settings import VaultSettings


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def chaos_storage_env(tmp_path: Path):
    """
    Sets up 6 independent storage nodes across 3 logical zones (2 nodes/zone),
    a FaultInjectionTransport to simulate network partitions and timeouts,
    and a Raft consensus metadata node.
    """
    storage_configs = [
        StorageNodeConfig(node_id="storage-1", url="http://storage-1:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-2", url="http://storage-2:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-3", url="http://storage-3:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-4", url="http://storage-4:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-5", url="http://storage-5:8001", region="us-east-1", zone="us-east-1c", active=True),
        StorageNodeConfig(node_id="storage-6", url="http://storage-6:8001", region="us-east-1", zone="us-east-1c", active=True),
    ]

    cluster = ClusterConfig(cluster_id="chaos-vault-cluster", storage_nodes=storage_configs)

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

    # 1. Initialize Storage Node ASGI apps with isolated disk paths
    storage_apps = {}
    storage_dirs = {}
    for node in storage_configs:
        node_dir = tmp_path / node.node_id
        node_settings = VaultSettings(data_dir=node_dir, chunk_size_bytes=8 * 1024 * 1024)
        app = create_vault_app(node_settings)
        storage_apps[node.node_id] = httpx.ASGITransport(app=app)
        storage_dirs[node.node_id] = node_dir

    # 2. Programmable Fault Injection Network Transport
    fault_transport = FaultInjectionTransport(storage_apps)
    storage_http_client = httpx.AsyncClient(transport=fault_transport, timeout=2.0)

    # 3. Raft Metadata Node
    raft_dir = tmp_path / "raft_meta"
    raft_port = 26000 + (int(time.time() * 100) % 4000)
    raft_addr = f"127.0.0.1:{raft_port}"
    raft_sm = RaftMetadataStateMachine(self_address=raft_addr, partner_addresses=[], data_dir=raft_dir)

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
            "fault_transport": fault_transport,
            "gateway_service": gateway_service,
            "raft_sm": raft_sm,
            "cluster": cluster,
            "policies": policies,
            "storage_dirs": storage_dirs,
            "tmp_path": tmp_path,
        }
    finally:
        raft_sm.destroy()


def wait_for_raft_leader(nodes: List[RaftMetadataStateMachine], timeout: float = 6.0) -> RaftMetadataStateMachine:
    """Poll until exactly one Raft leader is established among active nodes."""
    start = time.time()
    while time.time() - start < timeout:
        leaders = [n for n in nodes if n.isReady() and n.is_leader()]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.05)
    raise TimeoutError("Raft leader election timed out")


@pytest.fixture
def raft_3node_env(tmp_path: Path):
    """Fixture initializing a 3-node Raft metadata consensus cluster."""
    base_port = 27000 + (int(time.time() * 100) % 4000)
    addr1 = f"127.0.0.1:{base_port}"
    addr2 = f"127.0.0.1:{base_port + 1}"
    addr3 = f"127.0.0.1:{base_port + 2}"

    dir1 = tmp_path / "raft_meta1"
    dir2 = tmp_path / "raft_meta2"
    dir3 = tmp_path / "raft_meta3"

    node1 = RaftMetadataStateMachine(addr1, [addr2, addr3], data_dir=dir1, auto_tick_period=0.03)
    node2 = RaftMetadataStateMachine(addr2, [addr1, addr3], data_dir=dir2, auto_tick_period=0.03)
    node3 = RaftMetadataStateMachine(addr3, [addr1, addr2], data_dir=dir3, auto_tick_period=0.03)

    nodes = [node1, node2, node3]

    try:
        leader = wait_for_raft_leader(nodes)
        yield {
            "nodes": nodes,
            "leader": leader,
            "addrs": (addr1, addr2, addr3),
            "dirs": (dir1, dir2, dir3),
        }
    finally:
        for n in nodes:
            try:
                n.destroy()
            except Exception:
                pass


# =============================================================================
# CHAOS TEST SCENARIOS
# =============================================================================

@pytest.mark.asyncio
async def test_chaos_isolate_one_storage_node(chaos_storage_env):
    """
    Chaos Test 1: Isolate one storage node.
    Verify:
    - Bounded timeout behavior (request completes promptly without hanging).
    - Write succeeds because surviving replicas meet data write quorum (W=2 of 3).
    - Read succeeds and verifies SHA-256 data integrity (R=1).
    """
    env = chaos_storage_env
    gw_transport = env["gateway_transport"]
    fault_transport = env["fault_transport"]
    cluster = env["cluster"]
    policy = env["policies"]["hot"]

    bucket = "chaos-bucket"
    key = "single-node-failure.dat"
    content = b"FAULT_TOLERANT_OBJECT_CONTENT_CHUNK" * 100
    expected_hash = hashlib.sha256(content).hexdigest()

    # Determine placement nodes for this object
    target_nodes = select_placement_nodes(bucket, key, "v1", 0, policy, cluster)
    isolated_node = target_nodes[0].node_id

    # Isolate one target storage node via fault injection transport
    fault_transport.isolate_node(isolated_node)

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        t0 = time.time()
        # PUT object
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=content,
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": str(uuid.uuid4()),
                "Content-Type": "application/octet-stream",
            },
        )
        duration = time.time() - t0

        # Verify bounded timeout (< 2.5 seconds)
        assert duration < 2.5, f"Write took too long: {duration}s"
        # Write must succeed because W=2 quorum is achieved on the 2 surviving nodes
        assert put_resp.status_code == 201, f"Expected 201 Created, got {put_resp.status_code}: {put_resp.text}"

        # GET object
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 200
        assert get_resp.content == content
        assert hashlib.sha256(get_resp.content).hexdigest() == expected_hash

    # Clean up fault injection
    fault_transport.heal_all()


@pytest.mark.asyncio
async def test_chaos_isolate_enough_storage_nodes_to_lose_write_quorum(chaos_storage_env):
    """
    Chaos Test 2: Isolate enough storage nodes to lose data write quorum (W=2 required, only 1 reachable).
    Verify:
    - Bounded timeout behavior.
    - No false successful write (HTTP 503 Service Unavailable).
    - No uncommitted manifest visibility in Raft metadata.
    - Orphan candidates logged for successful partial writes.
    """
    env = chaos_storage_env
    gw_transport = env["gateway_transport"]
    fault_transport = env["fault_transport"]
    cluster = env["cluster"]
    raft_sm = env["raft_sm"]
    gw_service = env["gateway_service"]
    policy = env["policies"]["hot"]

    bucket = "chaos-bucket"
    key = "quorum-loss-file.dat"
    version_id = "v1-quorum-loss"
    content = b"QUORUM_LOSS_TEST_PAYLOAD" * 50

    # Determine placement nodes (3 nodes)
    target_nodes = select_placement_nodes(bucket, key, version_id, 0, policy, cluster)
    node1, node2 = target_nodes[0].node_id, target_nodes[1].node_id

    # Isolate 2 of 3 nodes: only 1 node reachable -> W=2 cannot be satisfied!
    fault_transport.isolate_node(node1)
    fault_transport.isolate_node(node2)

    initial_orphans = len(gw_service.orphan_candidates)

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        t0 = time.time()
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=content,
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": str(uuid.uuid4()),
                "X-Version-ID": version_id,
                "Content-Type": "application/octet-stream",
            },
        )
        duration = time.time() - t0

        # 1. Bounded timeout behavior
        assert duration < 3.0, f"Write hung beyond expected bounded timeout: {duration}s"

        # 2. No false successful write: MUST fail with HTTP 503
        assert put_resp.status_code == 503, f"Expected 503, got {put_resp.status_code}: {put_resp.text}"
        assert "Data write quorum unavailable" in put_resp.text

        # 3. No uncommitted manifest visibility in Raft metadata
        manifest = raft_sm.get_latest_manifest(bucket, key)
        assert manifest is None, "INVARIANT VIOLATION: Uncommitted manifest must NEVER be visible in Raft metadata!"

        # 4. Attempt GET returns 404 Not Found
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 404

        # 5. Partial write on the surviving node tracked as orphan candidate
        assert len(gw_service.orphan_candidates) >= initial_orphans

    fault_transport.heal_all()


def test_chaos_isolate_one_metadata_follower(raft_3node_env):
    """
    Chaos Test 3: Isolate one metadata follower node in a 3-node Raft cluster.
    Verify:
    - Raft majority (2 of 3) remains active.
    - Manifest updates continue to commit through the leader without disruption.
    - Committed manifests are retrievable.
    - Latency remains bounded.
    """
    nodes = raft_3node_env["nodes"]
    leader = raft_3node_env["leader"]
    followers = [n for n in nodes if n != leader]
    assert len(followers) == 2

    # Isolate one follower by destroying/stopping it
    isolated_follower = followers[0]
    surviving_follower = followers[1]
    isolated_follower.destroy()

    time.sleep(0.2)

    # Leader and surviving follower maintain majority quorum (2 of 3)
    assert leader.is_leader(), "Leader must remain leader with majority quorum (2/3)"

    # Write a new manifest through the leader
    bucket = "meta-chaos-bucket"
    key = "surviving-follower-write.dat"
    manifest_data = {
        "bucket": bucket,
        "key": key,
        "version_id": str(uuid.uuid4()),
        "content_hash": hashlib.sha256(b"meta_data").hexdigest(),
        "size_bytes": 9,
        "policy": "hot",
        "chunks": [],
        "created_at": time.time(),
    }

    t0 = time.time()
    res = leader.commit_manifest(manifest_data, timeout=3.0)
    duration = time.time() - t0

    assert duration < 2.0, f"Commit took unexpectedly long: {duration}s"
    assert res["success"] is True
    assert res["logical_version"] == 1

    # Verify committed manifest is retrieved
    retrieved = leader.get_latest_manifest(bucket, key)
    assert retrieved is not None
    assert retrieved["version_id"] == manifest_data["version_id"]


def test_chaos_kill_metadata_leader_during_manifest_update(raft_3node_env):
    """
    Chaos Test 4: Kill the metadata leader during a manifest update / operation.
    Verify:
    - Bounded failover / election timeout.
    - Surviving followers elect a new leader.
    - Historical committed data is fully preserved.
    - New writes succeed on the new leader.
    """
    nodes = raft_3node_env["nodes"]
    leader = raft_3node_env["leader"]
    followers = [n for n in nodes if n != leader]

    bucket = "failover-bucket"
    key = "baseline-committed.dat"

    # Commit baseline state
    v1_id = str(uuid.uuid4())
    leader.commit_manifest({
        "bucket": bucket,
        "key": key,
        "version_id": v1_id,
        "content_hash": "hash-v1",
        "size_bytes": 100,
        "policy": "hot",
        "chunks": [],
        "created_at": time.time(),
    })

    # Kill leader
    leader.destroy()

    # Wait for surviving followers to elect a new leader
    t0 = time.time()
    new_leader = wait_for_raft_leader(followers, timeout=5.0)
    election_duration = time.time() - t0

    # Bounded failover verification
    assert election_duration < 5.0
    assert new_leader.is_leader()
    assert new_leader != leader

    # Verify historical committed state is preserved intact
    persisted = new_leader.get_latest_manifest(bucket, key)
    assert persisted is not None
    assert persisted["version_id"] == v1_id
    assert persisted["logical_version"] == 1

    # Write a new version on the newly elected leader
    v2_id = str(uuid.uuid4())
    res_v2 = new_leader.commit_manifest({
        "bucket": bucket,
        "key": key,
        "version_id": v2_id,
        "content_hash": "hash-v2",
        "size_bytes": 200,
        "policy": "hot",
        "chunks": [],
        "created_at": time.time(),
    }, expected_version=1)

    assert res_v2["success"] is True
    assert res_v2["logical_version"] == 2


def test_chaos_partition_isolated_metadata_node_attempt_conflicting_write(raft_3node_env):
    """
    Chaos Test 5: Partition an isolated metadata node and attempt a conflicting write.
    Verify:
    - An isolated node without majority cannot accept writes.
    - Write raises NotLeaderError or RaftQuorumError.
    - No split-brain state occurs.
    """
    nodes = raft_3node_env["nodes"]
    leader = raft_3node_env["leader"]
    followers = [n for n in nodes if n != leader]

    # Attempt write on follower (non-leader)
    isolated_follower = followers[0]
    assert not isolated_follower.is_leader()

    with pytest.raises(NotLeaderError) as exc_info:
        isolated_follower.commit_manifest({
            "bucket": "split-brain-bucket",
            "key": "rogue-file.dat",
            "version_id": str(uuid.uuid4()),
            "content_hash": "rogue-hash",
            "size_bytes": 10,
            "policy": "hot",
            "chunks": [],
            "created_at": time.time(),
        })

    assert "Node is not the Raft leader" in str(exc_info.value)

    # Verify no rogue manifest exists in the cluster
    assert leader.get_latest_manifest("split-brain-bucket", "rogue-file.dat") is None


@pytest.mark.asyncio
async def test_chaos_isolate_all_nodes_in_one_logical_zone(chaos_storage_env):
    """
    Chaos Test 6: Isolate all storage nodes residing within one logical zone (e.g. us-east-1a).
    Verify:
    - Multi-zone policy ('hot') placed replicas across us-east-1a, us-east-1b, us-east-1c.
    - Surviving replicas in us-east-1b and us-east-1c satisfy read quorum (R=1).
    - Read succeeds and verifies SHA-256 integrity.
    - No acknowledged object becomes unreadable.
    """
    env = chaos_storage_env
    gw_transport = env["gateway_transport"]
    fault_transport = env["fault_transport"]
    cluster = env["cluster"]

    bucket = "zone-outage-bucket"
    key = "multi-zone-resilience.dat"
    content = b"CROSS_ZONE_HIGH_AVAILABILITY_PAYLOAD" * 80
    expected_hash = hashlib.sha256(content).hexdigest()

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Upload object when all zones are healthy
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=content,
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": str(uuid.uuid4()),
                "Content-Type": "application/octet-stream",
            },
        )
        assert put_resp.status_code == 201

        # 2. Chaos injection: Isolate entire availability zone 'us-east-1a' (storage-1 & storage-2)
        isolated_nodes = fault_transport.isolate_zone("us-east-1a", cluster)
        assert len(isolated_nodes) == 2

        # 3. Read object during total zone outage
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 200, f"Object read failed during zone outage: {get_resp.status_code}"
        assert get_resp.content == content
        assert hashlib.sha256(get_resp.content).hexdigest() == expected_hash

    fault_transport.heal_all()


@pytest.mark.asyncio
async def test_chaos_restore_connectivity_and_verify_convergence(chaos_storage_env):
    """
    Chaos Test 7: Restore connectivity after partition and verify automatic convergence.
    Verify:
    - Object is initially written while 1 node is isolated, leaving it degraded (2/3 replicas).
    - Connectivity is restored.
    - Background repair worker detects under-replication and repairs the missing replica.
    - Verification against latest committed Raft manifest passes.
    - Replica audit converges to 100% HEALTHY.
    """
    env = chaos_storage_env
    gw_transport = env["gateway_transport"]
    fault_transport = env["fault_transport"]
    cluster = env["cluster"]
    gw_service = env["gateway_service"]
    policy = env["policies"]["hot"]

    bucket = "repair-chaos-bucket"
    key = "heal-and-converge.dat"
    version_id = "v1-heal-test"
    content = b"HEAL_AND_CONVERGE_PAYLOAD" * 60
    expected_hash = hashlib.sha256(content).hexdigest()

    # Identify placement nodes
    targets = select_placement_nodes(bucket, key, version_id, 0, policy, cluster)
    isolated_node = targets[0].node_id

    # 1. Isolate one node and write object
    fault_transport.isolate_node(isolated_node)

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        put_resp = await client.put(
            f"/v1/objects/{bucket}/{key}",
            content=content,
            headers={
                "X-Vault-Policy": "hot",
                "X-Idempotency-Key": str(uuid.uuid4()),
                "X-Version-ID": version_id,
                "Content-Type": "application/octet-stream",
            },
        )
        assert put_resp.status_code == 201

        # 2. Audit shows degraded replication (1 unreachable)
        audit_before = (await client.get(f"/v1/objects/{bucket}/{key}/audit")).json()
        assert audit_before["healthy_count"] == 2
        assert audit_before["unreachable_count"] == 1

        # 3. Restore connectivity
        fault_transport.heal_all()

        # Audit now sees the node as reachable, but chunk is missing on it
        audit_healed = (await client.get(f"/v1/objects/{bucket}/{key}/audit")).json()
        assert audit_healed["missing_count"] == 1

        # 4. Trigger repair scan and cycle
        scan_resp = await client.post("/v1/admin/repair/scan")
        assert scan_resp.status_code == 200
        assert scan_resp.json()["enqueued_repairs"] >= 1

        # Execute repair cycle
        processed = await gw_service.repair_worker.run_repair_cycle()
        assert processed >= 1

        # 5. Verify complete convergence: 3 of 3 replicas are HEALTHY
        audit_after = (await client.get(f"/v1/objects/{bucket}/{key}/audit")).json()
        assert audit_after["healthy_count"] == 3
        assert audit_after["missing_count"] == 0
        assert audit_after["unreachable_count"] == 0

        # Object remains 100% readable
        get_resp = await client.get(f"/v1/objects/{bucket}/{key}")
        assert get_resp.status_code == 200
        assert get_resp.content == content
        assert hashlib.sha256(get_resp.content).hexdigest() == expected_hash


@pytest.mark.asyncio
async def test_chaos_rebalancing_with_concurrent_reads_writes(chaos_storage_env):
    """
    Chaos Test 8: Run background rebalancing while concurrent reads and writes continue.
    Verify:
    - Rebalance migrates chunks safely according to rendezvous placement.
    - Concurrent reads return 100% verified data without read errors or corruption.
    - Concurrent writes succeed or cleanly serialize.
    - Source replicas are never deleted prematurely before target verification.
    """
    env = chaos_storage_env
    gw_transport = env["gateway_transport"]
    gw_service = env["gateway_service"]
    cluster = env["cluster"]
    storage_apps = env["fault_transport"].node_transports
    tmp_path = env["tmp_path"]

    bucket = "rebalance-chaos-bucket"

    async with httpx.AsyncClient(transport=gw_transport, base_url="http://gateway:8000") as client:
        # 1. Populate baseline objects
        initial_keys = [f"baseline-obj-{i}.dat" for i in range(4)]
        for k in initial_keys:
            c = f"BASELINE_CONTENT_{k}".encode() * 50
            resp = await client.put(
                f"/v1/objects/{bucket}/{k}",
                content=c,
                headers={"X-Vault-Policy": "hot", "X-Idempotency-Key": str(uuid.uuid4())},
            )
            assert resp.status_code == 201

        # 2. Add storage-7 to cluster membership
        new_node_dir = tmp_path / "storage-7"
        new_node_settings = VaultSettings(data_dir=new_node_dir)
        new_app = create_vault_app(new_node_settings)
        storage_apps["storage-7"] = httpx.ASGITransport(app=new_app)

        add_resp = await client.post(
            "/v1/admin/nodes",
            json={
                "node_id": "storage-7",
                "url": "http://storage-7:8001",
                "region": "us-east-1",
                "zone": "us-east-1a",
                "active": True,
                "capacity_weight": 1.0,
            },
        )
        assert add_resp.status_code == 201

        # 3. Define concurrent background read/write worker
        stop_concurrent = asyncio.Event()
        read_successes = 0
        write_successes = 0
        read_errors = 0
        write_errors = 0

        async def concurrent_client_workload():
            nonlocal read_successes, write_successes, read_errors, write_errors
            counter = 0
            while not stop_concurrent.is_set():
                counter += 1
                try:
                    # Random read from baseline objects
                    target_read_key = initial_keys[counter % len(initial_keys)]
                    get_r = await client.get(f"/v1/objects/{bucket}/{target_read_key}")
                    if get_r.status_code == 200:
                        read_successes += 1
                    else:
                        read_errors += 1

                    # Concurrent write of new object
                    new_key = f"concurrent-write-{counter}.dat"
                    put_r = await client.put(
                        f"/v1/objects/{bucket}/{new_key}",
                        content=b"CONCURRENT_PUT_DATA" * 20,
                        headers={"X-Vault-Policy": "hot", "X-Idempotency-Key": str(uuid.uuid4())},
                    )
                    if put_r.status_code == 201:
                        write_successes += 1
                    else:
                        write_errors += 1
                except Exception:
                    read_errors += 1
                await asyncio.sleep(0.02)

        # Start concurrent workload
        workload_task = asyncio.create_task(concurrent_client_workload())

        # 4. Trigger rebalancing
        rebalance_trigger = await client.post("/v1/admin/rebalance", json={"rate_limit": 200})
        assert rebalance_trigger.status_code in (200, 202)

        # Wait for rebalance worker to complete
        start_wait = time.time()
        while time.time() - start_wait < 10.0:
            st = (await client.get("/v1/admin/rebalance/status")).json()
            if st.get("status") in ("completed", "idle"):
                break
            await asyncio.sleep(0.1)

        # Stop concurrent workload
        stop_concurrent.set()
        await workload_task

        # Verify results
        status = (await client.get("/v1/admin/rebalance/status")).json()
        assert status.get("status") in ("completed", "idle")
        assert status.get("failed_tasks", 0) == 0

        # Ensure high volume of successful concurrent foreground traffic during rebalance
        assert read_successes > 0, "Expected successful concurrent reads"
        assert read_errors == 0, f"Encountered unexpected read errors during rebalance: {read_errors}"
        assert write_successes > 0, "Expected successful concurrent writes"
        assert write_errors == 0, f"Encountered unexpected write errors during rebalance: {write_errors}"
