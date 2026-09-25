"""Integration tests for Archive Durability Policy using Reed-Solomon Erasure Coding (4+2).

Validates:
- Object chunks encoded into 6 fragments (4 data + 2 parity)
- Placement across 6 distinct storage nodes spanning 3 availability zones
- Storage amplification measured and exposed as 1.50x
- Reconstruction and read success with 1 missing fragment (any index)
- Reconstruction and read success with 2 missing fragments (data/parity combinations)
- Read failure with HTTP 503 when fewer than 4 valid fragments remain
- Cryptographic SHA-256 detection and quarantine of corrupt fragments
- Automatic background repair of missing or corrupt fragments
- Object hash validation ensuring corrupt reconstructed data is never served
"""

import asyncio
import hashlib
import os
from pathlib import Path
import time
from typing import Dict
import httpx
import pytest

from apps.gateway.service import GatewayService, create_gateway_app
from apps.storage_node.service import create_vault_app
from apps.workers.repair_worker import RepairWorker
from vault_core.cluster import ClusterConfig, StorageNodeConfig
from vault_core.manifest import ObjectManifest, ReplicaState
from vault_core.metadata_raft import RaftMetadataStateMachine
from vault_core.metrics import (
    REPAIR_BACKLOG,
    REPAIR_SUCCESS_TOTAL,
    STORAGE_AMPLIFICATION,
)
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
def archive_cluster_env(tmp_path: Path):
    """Sets up a 6-node storage cluster spanning 3 zones and Raft metadata node for Archive policy."""
    storage_configs = [
        StorageNodeConfig(node_id="storage-1", url="http://storage-1:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-2", url="http://storage-2:8001", region="us-east-1", zone="us-east-1a", active=True),
        StorageNodeConfig(node_id="storage-3", url="http://storage-3:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-4", url="http://storage-4:8001", region="us-east-1", zone="us-east-1b", active=True),
        StorageNodeConfig(node_id="storage-5", url="http://storage-5:8001", region="us-east-1", zone="us-east-1c", active=True),
        StorageNodeConfig(node_id="storage-6", url="http://storage-6:8001", region="us-east-1", zone="us-east-1c", active=True),
    ]

    cluster = ClusterConfig(cluster_id="vault-archive-cluster", storage_nodes=storage_configs)

    policies = {
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
        storage_dirs[node.node_id] = node_dir
        node_settings = VaultSettings(data_dir=node_dir, chunk_size_bytes=8 * 1024 * 1024)
        storage_app = create_vault_app(node_settings)
        storage_apps[node.node_id] = httpx.ASGITransport(app=storage_app)

    transport = MultiNodeHttpTransport(storage_apps)
    storage_http_client = httpx.AsyncClient(transport=transport, timeout=5.0)

    raft_dir = tmp_path / "raft_meta"
    raft_port = 28000 + (int(time.time() * 100) % 2000)
    raft_addr = f"127.0.0.1:{raft_port}"
    raft_sm = RaftMetadataStateMachine(self_address=raft_addr, partner_addresses=[], data_dir=raft_dir)

    start = time.time()
    while time.time() - start < 4.0:
        if raft_sm.is_leader():
            break
        time.sleep(0.05)

    assert raft_sm.is_leader(), "Raft metadata leader failed to elect"

    repair_worker = RepairWorker(
        cluster=cluster,
        metadata_raft=raft_sm,
        http_client=storage_http_client,
        secret_key="test-secret-key",
    )

    gateway_service = GatewayService(
        cluster=cluster,
        policies=policies,
        metadata_raft=raft_sm,
        storage_http_client=storage_http_client,
    )
    gateway_service.repair_worker = repair_worker

    gateway_app = create_gateway_app(gateway_service)
    gateway_transport = httpx.ASGITransport(app=gateway_app)

    try:
        yield {
            "gateway_app": gateway_app,
            "gateway_transport": gateway_transport,
            "transport": transport,
            "raft_sm": raft_sm,
            "service": gateway_service,
            "repair_worker": repair_worker,
            "cluster": cluster,
            "storage_dirs": storage_dirs,
            "storage_apps": storage_apps,
        }
    finally:
        raft_sm.destroy()


# =============================================================================
# 1. ENCODE / PLACE / AMPLIFICATION ACROSS 3 ZONES
# =============================================================================

@pytest.mark.asyncio
async def test_archive_policy_upload_placement_across_zones_and_storage_amplification(archive_cluster_env):
    """
    Test that uploading an object under archive policy:
    1. Encodes payload into 6 fragments (4 data, 2 parity).
    2. Places fragments on 6 distinct storage nodes across 3 distinct zones.
    3. Persists fragment hashes and node locations in Raft manifest.
    4. Exposes 1.50x storage amplification in metrics and health endpoint.
    5. Retrieves verified original data matching content hash.
    """
    env = archive_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    payload = b"Vault Archive Erasure Coding Test: 4 data fragments + 2 parity fragments spanning 3 availability zones!" * 200
    expected_sha = hashlib.sha256(payload).hexdigest()

    # Upload with archive policy
    resp = await gw_client.put(
        "/v1/objects/archive-bucket/data/file1.bin",
        content=payload,
        headers={
            "Idempotency-Key": "archive-put-1",
            "X-Vault-Policy": "archive",
            "Content-Type": "application/octet-stream",
        }
    )
    assert resp.status_code == 201, resp.text
    put_data = resp.json()
    version_id = put_data["version_id"]
    assert put_data["policy"] == "archive"

    # Verify manifest in Raft metadata
    manifest_dict = env["raft_sm"].get_latest_manifest("archive-bucket", "data/file1.bin")
    assert manifest_dict is not None
    manifest = ObjectManifest.model_validate(manifest_dict)
    assert len(manifest.chunks) == 1
    chunk = manifest.chunks[0]

    assert chunk.is_erasure_coded is True
    assert len(chunk.fragments) == 6

    # Verify placed on 6 distinct nodes
    placed_nodes = [f.node_id for f in chunk.fragments]
    assert len(set(placed_nodes)) == 6

    # Verify placed across all 3 zones
    placed_zones = {env["cluster"].get_storage_node(nid).zone for nid in placed_nodes}
    assert len(placed_zones) == 3
    assert placed_zones == {"us-east-1a", "us-east-1b", "us-east-1c"}

    # Verify fragment indices 0..3 are data, 4..5 are parity
    for f in chunk.fragments:
        assert 0 <= f.fragment_index < 6
        if f.fragment_index < 4:
            assert not f.is_parity
        else:
            assert f.is_parity

    # Verify storage amplification in object health endpoint
    health_resp = await gw_client.get("/v1/objects/archive-bucket/data/file1.bin/health")
    assert health_resp.status_code == 200
    health_data = health_resp.json()
    assert health_data["storage_amplification"] == 1.5

    # Verify GET returns exact original data
    get_resp = await gw_client.get("/v1/objects/archive-bucket/data/file1.bin")
    assert get_resp.status_code == 200
    assert get_resp.content == payload
    assert get_resp.headers["ETag"] == f'"{expected_sha}"'
    assert get_resp.headers["X-Vault-Policy"] == "archive"


# =============================================================================
# 2. RECONSTRUCTION WITH ONE MISSING FRAGMENT
# =============================================================================

@pytest.mark.asyncio
async def test_archive_read_with_one_missing_fragment(archive_cluster_env):
    """
    Test that an object under archive policy is successfully reconstructed and served
    when 1 storage node / fragment is completely missing or offline.
    """
    env = archive_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    payload = b"Testing reconstruction with 1 missing fragment in Reed-Solomon 4+2 setup." * 150
    expected_sha = hashlib.sha256(payload).hexdigest()

    # Upload archive object
    put_resp = await gw_client.put(
        "/v1/objects/archive-bucket/survive-1.dat",
        content=payload,
        headers={"Idempotency-Key": "survive-1-put", "X-Vault-Policy": "archive"}
    )
    assert put_resp.status_code == 201
    manifest_dict = env["raft_sm"].get_latest_manifest("archive-bucket", "survive-1.dat")
    manifest = ObjectManifest.model_validate(manifest_dict)
    frag_1 = manifest.chunks[0].fragments[1]

    # Stop node holding fragment 1 (simulating complete node loss)
    env["transport"].stop_node(frag_1.node_id)

    # Read should reconstruct seamlessly from the 5 surviving fragments
    get_resp = await gw_client.get("/v1/objects/archive-bucket/survive-1.dat")
    assert get_resp.status_code == 200
    assert get_resp.content == payload
    assert hashlib.sha256(get_resp.content).hexdigest() == expected_sha


# =============================================================================
# 3. RECONSTRUCTION WITH TWO MISSING FRAGMENTS
# =============================================================================

@pytest.mark.asyncio
async def test_archive_read_with_two_missing_fragments(archive_cluster_env):
    """
    Test that an object under archive policy is successfully reconstructed and served
    when 2 storage nodes / fragments are missing (maximum fault tolerance of 4+2).
    """
    env = archive_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    payload = b"Testing maximum fault tolerance: 2 node failures concurrently in RS 4+2 code." * 300
    expected_sha = hashlib.sha256(payload).hexdigest()

    put_resp = await gw_client.put(
        "/v1/objects/archive-bucket/survive-2.dat",
        content=payload,
        headers={"Idempotency-Key": "survive-2-put", "X-Vault-Policy": "archive"}
    )
    assert put_resp.status_code == 201

    manifest_dict = env["raft_sm"].get_latest_manifest("archive-bucket", "survive-2.dat")
    manifest = ObjectManifest.model_validate(manifest_dict)
    frag_0 = manifest.chunks[0].fragments[0]  # Data fragment
    frag_5 = manifest.chunks[0].fragments[5]  # Parity fragment

    # Stop both nodes holding fragment 0 and fragment 5
    env["transport"].stop_node(frag_0.node_id)
    env["transport"].stop_node(frag_5.node_id)

    # Read should reconstruct from the remaining 4 valid fragments
    get_resp = await gw_client.get("/v1/objects/archive-bucket/survive-2.dat")
    assert get_resp.status_code == 200
    assert get_resp.content == payload
    assert hashlib.sha256(get_resp.content).hexdigest() == expected_sha


# =============================================================================
# 4. FAILED READ WITH FEWER THAN FOUR VALID FRAGMENTS
# =============================================================================

@pytest.mark.asyncio
async def test_archive_failed_read_with_fewer_than_four_fragments(archive_cluster_env):
    """
    Test that reading an object strictly fails with HTTP 503 when 3 or more fragments
    are missing or corrupt, and no partial or corrupt data is served.
    """
    env = archive_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    payload = b"Testing strict failure when fewer than K=4 fragments survive." * 50

    put_resp = await gw_client.put(
        "/v1/objects/archive-bucket/fail-3.dat",
        content=payload,
        headers={"Idempotency-Key": "fail-3-put", "X-Vault-Policy": "archive"}
    )
    assert put_resp.status_code == 201

    manifest_dict = env["raft_sm"].get_latest_manifest("archive-bucket", "fail-3.dat")
    manifest = ObjectManifest.model_validate(manifest_dict)
    frags_to_kill = manifest.chunks[0].fragments[:3]  # Kill 3 nodes

    for f in frags_to_kill:
        env["transport"].stop_node(f.node_id)

    # Read must fail with HTTP 503
    get_resp = await gw_client.get("/v1/objects/archive-bucket/fail-3.dat")
    assert get_resp.status_code == 503
    assert "only 3 valid fragments available, required at least 4" in get_resp.text


# =============================================================================
# 5. CRYPTOGRAPHIC CORRUPTION DETECTION & AUTOMATIC BACKGROUND REPAIR
# =============================================================================

@pytest.mark.asyncio
async def test_archive_automatic_background_fragment_repair(archive_cluster_env):
    """
    Test that:
    1. Fragment corruption directly on disk is detected via SHA-256.
    2. Read request succeeds via surviving fragments.
    3. The repair worker reconstructs the damaged fragment from surviving fragments.
    4. The repaired fragment's hash is verified and restored onto the node.
    5. The cluster audit confirms all 6 fragments return to HEALTHY state.
    """
    env = archive_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    payload = b"Bit rot corruption repair in Reed-Solomon erasure coding background worker!" * 120
    expected_sha = hashlib.sha256(payload).hexdigest()

    put_resp = await gw_client.put(
        "/v1/objects/archive-bucket/repair-test.bin",
        content=payload,
        headers={"Idempotency-Key": "repair-test-put", "X-Vault-Policy": "archive"}
    )
    assert put_resp.status_code == 201
    version_id = put_resp.json()["version_id"]

    manifest_dict = env["raft_sm"].get_latest_manifest("archive-bucket", "repair-test.bin")
    manifest = ObjectManifest.model_validate(manifest_dict)
    target_frag = manifest.chunks[0].fragments[2]  # Target fragment index 2

    # Directly corrupt fragment 2 on its physical storage node disk
    node_dir = env["storage_dirs"][target_frag.node_id]
    chunk_path = node_dir / "chunks" / "archive-bucket" / version_id / "chunk_2.dat"
    assert chunk_path.is_file(), f"Chunk file {chunk_path} must exist"

    # Flip bytes directly
    with open(chunk_path, "r+b") as f:
        data = bytearray(f.read())
        data[0] ^= 0xFF
        f.seek(0)
        f.write(data)

    # 1. Audit object - should detect CORRUPT for fragment 2
    audit_resp = await gw_client.get("/v1/objects/archive-bucket/repair-test.bin/audit")
    assert audit_resp.status_code == 200
    audit_data = audit_resp.json()
    corrupt_audits = [a for a in audit_data["replicas"] if a["state"] == "corrupt"]
    assert len(corrupt_audits) == 1
    assert corrupt_audits[0]["chunk_index"] == 2
    assert corrupt_audits[0]["node_id"] == target_frag.node_id

    # 2. Read request succeeds seamlessly despite the corruption (reconstructed from the 5 healthy frags)
    get_resp = await gw_client.get("/v1/objects/archive-bucket/repair-test.bin")
    assert get_resp.status_code == 200
    assert get_resp.content == payload

    # 3. Trigger repair worker audit scan and verify task is queued (via read repair or audit)
    await env["repair_worker"].scan_and_enqueue_degraded_objects()
    assert env["repair_worker"].queue_size() >= 1

    success_count = await env["repair_worker"].repair_all()
    assert success_count >= 1

    # 4. Re-audit object - all 6 fragments must now be HEALTHY!
    audit_resp_post = await gw_client.get("/v1/objects/archive-bucket/repair-test.bin/audit")
    assert audit_resp_post.status_code == 200
    audit_data_post = audit_resp_post.json()
    assert audit_data_post["healthy_count"] == 6
    assert audit_data_post["corrupt_count"] == 0

    # Check on disk that fragment 2 has been restored with correct SHA-256
    with open(chunk_path, "rb") as f:
        restored_bytes = f.read()
    assert hashlib.sha256(restored_bytes).hexdigest() == target_frag.sha256


# =============================================================================
# 6. OBJECT HASH VALIDATION BEFORE SERVING
# =============================================================================

@pytest.mark.asyncio
async def test_archive_final_object_hash_validation_guards_against_serving_corrupted_data(archive_cluster_env):
    """
    Test that the gateway does NOT serve reconstructed data unless the final object hash validates.
    If the manifest or payload has a checksum mismatch, the read is aborted with HTTP 503.
    """
    env = archive_cluster_env
    gw_client = httpx.AsyncClient(transport=env["gateway_transport"], base_url="http://gateway:8000")

    payload = b"Tamper detection test: gateway must never serve reconstructed data if content hash fails."
    put_resp = await gw_client.put(
        "/v1/objects/archive-bucket/tamper.dat",
        content=payload,
        headers={"Idempotency-Key": "tamper-put", "X-Vault-Policy": "archive"}
    )
    assert put_resp.status_code == 201

    # Tamper with the committed manifest in Raft state machine so content_hash is wrong
    manifest_dict = env["raft_sm"].get_latest_manifest("archive-bucket", "tamper.dat")
    vid = manifest_dict["version_id"]
    env["raft_sm"]._manifests["archive-bucket/tamper.dat"][vid]["content_hash"] = "0" * 64

    # Attempt to read object: gateway decodes fragments, computes overall hash,
    # detects mismatch against manifest.content_hash, and refuses to serve with HTTP 503
    get_resp = await gw_client.get("/v1/objects/archive-bucket/tamper.dat")
    assert get_resp.status_code == 503
    assert "Reconstructed object checksum mismatch" in get_resp.text

