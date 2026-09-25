# Vault Consistency Model

## 1. Formal Consistency Statement

> **“Vault provides strong consistency for committed object manifests through its Raft metadata cluster. Object data is immutable and verified against committed manifests. Vault rejects writes that cannot meet the selected durability policy or metadata quorum. Availability depends on the surviving metadata quorum and selected data policy.”**

---

## 2. Architectural Consistency Guarantees

### 2.1 Metadata Serializability via Raft Consensus
All manifest updates, logical version updates, tombstone creations, and cluster membership changes are serialized through a 3-node Raft consensus cluster.
- **Strict Linearizability**: Writes to metadata must be accepted by the Raft leader and committed across a majority quorum ($Q = \lfloor N/2 \rfloor + 1 = 2$) before acknowledging success.
- **Split-Brain Prevention**: A partitioned metadata minority (e.g. 1 isolated node) cannot elect a leader or commit any log entry. Client write requests routed to a partitioned minority will time out or be redirected to the legitimate leader.
- **Compare-And-Swap (CAS) & Version Preconditions**: Every object possesses an immutable version UUID and an incrementing logical version number ($v_1, v_2, \dots$). Updates can specify preconditions (e.g., `expected_version=N`). If concurrent writes attempt to update the same key with the same expected version, Raft guarantees sequential execution where **exactly one write succeeds** and the second fails with **HTTP 409 Conflict**.

### 2.2 Immutability of Object Chunks
Vault storage nodes never mutate existing data in place.
- Chunks and erasure fragments are keyed by `(bucket, version_uuid, chunk_index)`.
- Overwriting an object creates a brand new `version_uuid` and distinct chunk set on storage nodes.
- Reads fetch data specifically mapped to the version designated by the committed manifest, eliminating read-write torn states.

### 2.3 Visibility Guarantees
- **Atomicity**: An object becomes visible system-wide if and only if its manifest is committed to the Raft state machine.
- **No Dirty Reads / Uncommitted Visibility**: Chunks written to storage nodes prior to manifest commit are completely invisible to standard `GET` and `HEAD` requests.
- **Read-Your-Writes**: Once a `PUT` request returns `HTTP 200 OK` (confirming Raft commit), subsequent reads by any client that contact the Raft quorum will immediately observe the new version.

---

## 3. Quorum Math and Failure Handling

### 3.1 Data Write & Read Quorum
For a replication factor $N$, data write quorum $W$, and data read quorum $R$:
$$\text{Strict Consistency Condition: } W + R > N$$

* **Hot Policy ($N=3, W=2, R=1$)**:
  - $W + R = 3 \ge N$.
  - Writes require at least 2 nodes to acknowledge persistent write to disk with verified SHA-256 before Raft commit.
  - Reads fetch from any 1 available replica. If that replica is corrupt or missing, read-repair falls back to surviving replicas.
* **Durable Policy ($N=4, W=3, R=1$)**:
  - $W + R = 4 \ge N$.
  - Survives 1 storage node crash during writes while maintaining durability.
* **Archive Policy ($K=4, M=2, N=6$)**:
  - Full write requires all 6 fragments ($W=6$) to ensure maximum durability at upload.
  - Read reconstruction requires any $K=4$ valid fragments ($R=4$).

### 3.2 Orphan Handling (Data Written, Raft Aborted)
If chunk streaming to storage nodes succeeds (data quorum achieved) but the subsequent Raft manifest commit fails (e.g., Raft leader partitioned or crashed):
1. The gateway returns `HTTP 503 Service Unavailable` or `HTTP 500 Internal Error` to the client.
2. The object remains **uncommitted and invisible**.
3. Storage nodes track chunk creation timestamps. A background garbage collection worker sweeps the storage nodes, checks chunk version UUIDs against the Raft committed inventory, marks unreferenced chunks as **orphaned**, and safely deletes them after a 24-hour grace period.
