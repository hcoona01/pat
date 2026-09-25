"""Integration and failure-mode tests for the 3-node Raft metadata consensus cluster."""

import time
import uuid
from pathlib import Path
import pytest

from vault_core.metadata_raft import (
    CASConflictError,
    ManifestNotFoundError,
    NotLeaderError,
    RaftMetadataStateMachine,
    RaftQuorumError,
)


def get_free_port_triple(base: int):
    """Generate triple of local TCP addresses for Raft testing."""
    return (
        f"127.0.0.1:{base}",
        f"127.0.0.1:{base + 1}",
        f"127.0.0.1:{base + 2}",
    )


def wait_for_leader(nodes: list[RaftMetadataStateMachine], timeout: float = 6.0) -> RaftMetadataStateMachine:
    """Poll until exactly one Raft leader is established among active nodes."""
    start = time.time()
    while time.time() - start < timeout:
        leaders = [n for n in nodes if n.isReady() and n.is_leader()]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.1)
    raise TimeoutError("Raft leader election timed out")


@pytest.fixture
def raft_cluster(tmp_path: Path):
    """Fixture initializing a 3-node Raft metadata cluster on local ports."""
    # Unique port range per fixture invocation based on timestamp
    base_port = 23000 + (int(time.time() * 100) % 5000)
    addr1, addr2, addr3 = get_free_port_triple(base_port)

    dir1 = tmp_path / "node1"
    dir2 = tmp_path / "node2"
    dir3 = tmp_path / "node3"

    node1 = RaftMetadataStateMachine(addr1, [addr2, addr3], data_dir=dir1, auto_tick_period=0.03)
    node2 = RaftMetadataStateMachine(addr2, [addr1, addr3], data_dir=dir2, auto_tick_period=0.03)
    node3 = RaftMetadataStateMachine(addr3, [addr1, addr2], data_dir=dir3, auto_tick_period=0.03)

    nodes = [node1, node2, node3]

    try:
        leader = wait_for_leader(nodes)
        yield {"nodes": nodes, "leader": leader, "addrs": (addr1, addr2, addr3), "dirs": (dir1, dir2, dir3)}
    finally:
        for n in nodes:
            try:
                n.destroy()
            except Exception:
                pass


def test_raft_three_node_leader_election(raft_cluster):
    """Verify that a 3-node Raft cluster establishes consensus and elects exactly one leader."""
    nodes = raft_cluster["nodes"]
    leaders = [n for n in nodes if n.is_leader()]
    assert len(leaders) == 1, f"Expected exactly 1 leader, found {len(leaders)}"

    leader = leaders[0]
    followers = [n for n in nodes if not n.is_leader()]
    assert len(followers) == 2

    # Verify followers recognize the leader
    time.sleep(0.3)
    for f in followers:
        assert f.get_leader_address() == leader.self_address


def test_committed_manifest_visibility_across_nodes(raft_cluster):
    """Verify that a manifest committed to the leader converges and becomes visible on all nodes."""
    nodes = raft_cluster["nodes"]
    leader = wait_for_leader(nodes)

    bucket = "docs"
    key = "manual.pdf"
    version_id = str(uuid.uuid4())
    manifest = {
        "bucket": bucket,
        "key": key,
        "version_id": version_id,
        "content_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "size_bytes": 1024,
        "created_at": time.time(),
    }

    # Commit via leader
    res = leader.commit_manifest(manifest, expected_version=0, timeout=4.0)
    assert res["success"] is True
    assert res["logical_version"] == 1

    # Verify visible on leader
    leader_manifest = leader.get_latest_manifest(bucket, key)
    assert leader_manifest is not None
    assert leader_manifest["version_id"] == version_id

    # Wait for replication to followers
    time.sleep(0.4)
    followers = [n for n in nodes if not n.is_leader()]
    for f in followers:
        f_manifest = f.get_latest_manifest(bucket, key)
        assert f_manifest is not None
        assert f_manifest["version_id"] == version_id
        assert f_manifest["logical_version"] == 1


def test_follower_cannot_accept_writes(raft_cluster):
    """Verify that attempting to commit directly on a follower raises NotLeaderError."""
    nodes = raft_cluster["nodes"]
    leader = wait_for_leader(nodes)
    follower = [n for n in nodes if not n.is_leader()][0]

    manifest = {
        "bucket": "b",
        "key": "k",
        "version_id": str(uuid.uuid4()),
        "content_hash": "abc",
        "created_at": time.time(),
    }

    with pytest.raises(NotLeaderError) as exc_info:
        follower.commit_manifest(manifest)

    assert exc_info.value.leader_address == leader.self_address


def test_competing_writes_and_stale_update_rejection(raft_cluster):
    """
    Verify that concurrent writes with the same expected version allow exactly one success,
    and stale updates raise CASConflictError (HTTP 409 Conflict equivalent).
    """
    nodes = raft_cluster["nodes"]
    leader = wait_for_leader(nodes)
    bucket = "finance"
    key = "ledger.csv"

    # Initial write -> version 1
    v1_id = str(uuid.uuid4())
    leader.commit_manifest({
        "bucket": bucket,
        "key": key,
        "version_id": v1_id,
        "content_hash": "hash1",
        "created_at": time.time(),
    }, expected_version=0)

    curr = leader.get_latest_manifest(bucket, key)
    assert curr["logical_version"] == 1

    # Competing writers: Writer A and Writer B both expect version 1
    writer_a_manifest = {
        "bucket": bucket,
        "key": key,
        "version_id": str(uuid.uuid4()),
        "content_hash": "hash_a",
        "created_at": time.time(),
    }
    writer_b_manifest = {
        "bucket": bucket,
        "key": key,
        "version_id": str(uuid.uuid4()),
        "content_hash": "hash_b",
        "created_at": time.time(),
    }

    # First writer succeeds
    res_a = leader.commit_manifest(writer_a_manifest, expected_version=1)
    assert res_a["success"] is True
    assert res_a["logical_version"] == 2

    # Second writer with stale expected_version=1 MUST be rejected with CASConflictError
    with pytest.raises(CASConflictError) as exc_info:
        leader.commit_manifest(writer_b_manifest, expected_version=1)

    assert exc_info.value.expected_version == 1
    assert exc_info.value.current_version == 2

    # Verify that only Writer A's version is committed and visible
    current_manifest = leader.get_latest_manifest(bucket, key)
    assert current_manifest["version_id"] == writer_a_manifest["version_id"]
    assert current_manifest["content_hash"] == "hash_a"


def test_follower_isolation_and_committed_state_recovery(raft_cluster, tmp_path: Path):
    """
    Verify that:
    1. Isolating 1 follower allows operations to continue (2/3 quorum maintained).
    2. Reconnecting / restarting the isolated follower results in full convergence to latest state.
    """
    nodes = raft_cluster["nodes"]
    leader = wait_for_leader(nodes)
    followers = [n for n in nodes if not n.is_leader()]
    isolated_follower = followers[0]
    surviving_follower = followers[1]

    # 1. Isolate one follower
    isolated_addr = isolated_follower.self_address
    isolated_dir = tmp_path / "isolated_node"
    isolated_follower.destroy()
    nodes.remove(isolated_follower)

    time.sleep(0.5)

    # 2. Verify surviving majority (leader + 1 follower = 2 nodes) continues to commit writes
    bucket = "data"
    key = "stream.log"
    v_id = str(uuid.uuid4())
    res = leader.commit_manifest({
        "bucket": bucket,
        "key": key,
        "version_id": v_id,
        "content_hash": "hash_during_partition",
        "created_at": time.time(),
    }, expected_version=0, timeout=4.0)

    assert res["success"] is True

    # 3. Recover the isolated follower
    partner_addrs = [leader.self_address, surviving_follower.self_address]
    recovered_node = RaftMetadataStateMachine(
        isolated_addr,
        partner_addrs,
        data_dir=isolated_dir,
        auto_tick_period=0.03
    )
    nodes.append(recovered_node)

    # Allow time for Raft log catchup / synchronization with bounded polling
    recovered_manifest = None
    start_wait = time.time()
    while time.time() - start_wait < 5.0:
        recovered_manifest = recovered_node.get_latest_manifest(bucket, key)
        if recovered_manifest is not None:
            break
        time.sleep(0.1)

    # 4. Verify recovered node has synchronized the committed manifest
    assert recovered_manifest is not None, "Recovered follower failed to sync committed manifest within 5s"
    assert recovered_manifest["version_id"] == v_id
    assert recovered_manifest["content_hash"] == "hash_during_partition"


def test_leader_loss_and_re_election(raft_cluster):
    """
    Verify that killing the active leader triggers a clean election among surviving nodes
    and allows continuous writes without split-brain.
    """
    nodes = raft_cluster["nodes"]
    original_leader = wait_for_leader(nodes)
    original_leader_addr = original_leader.self_address

    # Commit baseline object on original leader
    leader.commit_manifest({
        "bucket": "b",
        "key": "pre_kill.txt",
        "version_id": "v-pre",
        "content_hash": "pre_hash",
        "created_at": time.time(),
    }, expected_version=0) if (leader := original_leader) else None

    # Kill original leader
    original_leader.destroy()
    nodes.remove(original_leader)

    # Wait for surviving 2 followers to elect a new leader
    new_leader = wait_for_leader(nodes, timeout=5.0)
    assert new_leader.self_address != original_leader_addr
    assert new_leader.is_leader()

    # Verify baseline object is intact on new leader
    pre_manifest = new_leader.get_latest_manifest("b", "pre_kill.txt")
    assert pre_manifest is not None
    assert pre_manifest["version_id"] == "v-pre"

    # Commit new object on new leader
    res = new_leader.commit_manifest({
        "bucket": "b",
        "key": "post_kill.txt",
        "version_id": "v-post",
        "content_hash": "post_hash",
        "created_at": time.time(),
    }, expected_version=0)
    assert res["success"] is True

    post_manifest = new_leader.get_latest_manifest("b", "post_kill.txt")
    assert post_manifest is not None
    assert post_manifest["version_id"] == "v-post"


def test_tombstone_commit_and_invisibility(raft_cluster):
    """Verify that committing a tombstone hides the object across the Raft cluster."""
    nodes = raft_cluster["nodes"]
    leader = wait_for_leader(nodes)
    bucket = "records"
    key = "customer_101.json"

    # 1. Create object
    leader.commit_manifest({
        "bucket": bucket,
        "key": key,
        "version_id": str(uuid.uuid4()),
        "content_hash": "valid_content",
        "created_at": time.time(),
    }, expected_version=0)

    assert leader.get_latest_manifest(bucket, key) is not None

    # 2. Tombstone object
    res = leader.create_tombstone(bucket, key, expected_version=1)
    assert res["success"] is True
    assert res["logical_version"] == 2

    # 3. Verify immediately invisible on leader
    assert leader.get_latest_manifest(bucket, key) is None

    # 4. Verify invisible across followers after sync
    time.sleep(0.3)
    for f in [n for n in nodes if not n.is_leader()]:
        assert f.get_latest_manifest(bucket, key) is None
