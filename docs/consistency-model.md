# Vault Consistency & Replica-Convergence Model

## 1. Formal Consistency Statement

> **“Vault provides linearizable strong consistency for committed object manifests through its Raft consensus cluster. Object chunk data is immutable, content-addressed, and cryptographically verified against committed manifests. Vault rejects writes that cannot meet the selected data quorum or metadata precondition. Availability depends on surviving metadata consensus quorum and configured durability policies.”**

---

## 2. Metadata Consistency Model

### 2.1 Dual-Identifier Versioning Architecture
Vault separates logical progression from physical chunk immutability:
1. **Immutable Version UUID (`version_id`)**:
   - A randomly generated RFC 4122 v4 UUID (`str(uuid.uuid4())`).
   - Globally unique per object mutation or deletion.
   - Designates the physical directory path on storage nodes: `/chunks/{bucket}/{version_id}/chunk_{index}.dat`.
   - Never reused, modified, or overwritten in place.
2. **Monotonic Logical Version (`logical_version`)**:
   - An integer counter ($1, 2, 3, \dots$) managed strictly by the Raft replicated state machine.
   - Increments sequentially on every committed mutation or tombstone for that object key.
   - Exposed to clients via the `X-Vault-Logical-Version` HTTP header.

### 2.2 Compare-And-Set (CAS) Preconditions
Vault supports atomic conditional writes to prevent lost updates in concurrent environments:
- Clients submit the `X-Expected-Version` HTTP request header:
  - `X-Expected-Version: 0`: Asserts the object does not exist (atomic create-if-absent).
  - `X-Expected-Version: N`: Asserts the current logical version is exactly $N$.
- **Raft State Machine Validation**:
  - The check is evaluated deterministically on the Raft leader and confirmed upon quorum commit.
  - If `expected_version == current_logical_version`: Commit succeeds, advancing the logical version to $N + 1$.
  - If `expected_version != current_logical_version`: Commit is rejected immediately with a `CAS_CONFLICT` error.
- **HTTP 409 Conflict Semantics**:
  - The gateway translates CAS rejection to `HTTP 409 Conflict` with a diagnostic message (e.g., `CAS Precondition Failed: expected version 1, but current logical version is 2`).
  - When multiple concurrent writers compete with the same `X-Expected-Version`, **exactly one writer succeeds** ($201\text{ Created}$) and all competing stale writers receive `HTTP 409 Conflict`.

### 2.3 Versioned Tombstones
Object deletion in Vault is not a physical file removal, but the atomic creation of an immutable tombstone version:
- **Tombstone Generation**:
  - `DELETE /v1/objects/{bucket}/{key}` evaluates CAS preconditions (if supplied) and commits a tombstone manifest to Raft.
  - The tombstone is assigned a new `version_id` UUID, increments `logical_version`, sets `is_tombstone = True`, and records `deleted_at`.
- **Immediate Invisibility**:
  - Once committed to Raft, subsequent `GET` and `HEAD` requests immediately return `HTTP 404 Not Found`.
  - A duplicate `DELETE` request returns `HTTP 404 Not Found`.
- **Recreation Lifecycle**:
  - A subsequent `PUT` to a tombstoned key succeeds (provided CAS matches the tombstone's logical version, or if no CAS header is supplied) and increments the logical version past the tombstone.

### 2.4 Failed Raft Commit Isolation
- If chunk uploads achieve the required data write quorum ($W$) but the subsequent Raft manifest commit fails (e.g., Raft leader partition, election in flight, or disk sync timeout):
  1. The gateway returns `HTTP 503 Service Unavailable` or `HTTP 500 Internal Error`.
  2. The manifest is **never recorded in Raft**.
  3. Subsequent reads for the key return `HTTP 404 Not Found`.
  4. Any chunks already written to storage nodes are queued as **orphan candidates** for background reclamation.

---

## 3. Replica-Convergence Model

Storage nodes in Vault are independent, horizontally scalable workers that store immutable chunk files on local disk. Because nodes can crash, experience disk errors, or encounter network partitions, replica state can diverge from the latest committed manifest.

### 3.1 Ground Truth: The Raft Manifest
The Raft consensus log is the single source of truth for cluster state:
- An object chunk is valid if and only if its SHA-256 matches the hash in the **latest committed Raft manifest** for that key.
- Storage nodes do not run consensus among themselves; they serve chunks by `(bucket, version_id, chunk_index)`.

### 3.2 Explicit Replica States
Vault defines five explicit lifecycle states for each physical chunk replica across the cluster:

| State | Definition | Read Action | Convergence Trigger |
| :--- | :--- | :--- | :--- |
| **`HEALTHY`** | Chunk exists on node, version matches manifest, and SHA-256 is validated. | Served to client. | None (already convergent). |
| **`STALE`** | Node holds a chunk belonging to an older, superseded version UUID, but lacks the current version. | Skipped on read. | Repaired: write latest version chunk to node; schedule old version for GC. |
| **`CORRUPT`** | Chunk exists on node for the current version, but SHA-256 mismatch detected or file quarantined. | Skipped on read. | Repaired: copy verified chunk from healthy replica; quarantine corrupt file. |
| **`MISSING`** | Storage node is active and reachable, but has no chunk file for either current or historical versions. | Skipped on read. | Repaired: copy verified chunk from surviving replica. |
| **`UNREACHABLE`** | Storage node connection timed out, connection refused, or node marked inactive in topology. | Skipped on read. | Rebalanced / failed over to another node in the target zone. |

### 3.3 Replica Inventory Auditing
Vault provides the `GET /v1/objects/{bucket}/{key:path}/audit` API to inspect convergence:
1. Gateway fetches the latest committed manifest from Raft.
2. For every chunk and every placement node, gateway queries the storage node:
   - If current chunk exists: verifies cryptographic SHA-256. If matching $\to$ `HEALTHY`; if mismatch $\to$ `CORRUPT`.
   - If current chunk is 404: inspects historical versions from Raft. If node holds an older version $\to$ `STALE`; if node holds no version $\to$ `MISSING`.
   - If node fails to respond $\to$ `UNREACHABLE`.
3. Returns aggregated counts: `{healthy_count, stale_count, corrupt_count, missing_count, unreachable_count}`.

### 3.4 Safe Old-Version Garbage Collection Eligibility
Because in-flight reads may be streaming historical versions, Vault enforces strict garbage-collection eligibility rules before any chunk is purged from storage nodes:

$$\text{Eligible for GC} \iff \text{not active} \land \Delta t \ge \text{retention\_grace\_period}$$

The formal evaluation function `evaluate_gc_eligibility(manifest, current_manifest, retention_period)` dictates:
1. **Active Current Version**: If `manifest.version_id == current_manifest.version_id` and `is_tombstone == False`, it is **NEVER eligible** for garbage collection.
2. **Versioned Tombstone**: If `manifest.is_tombstone == True`, it is eligible only after the grace period has elapsed from `deleted_at`:
   $$\text{age} = t_{\text{now}} - \text{deleted\_at} \ge \text{retention\_period}$$
3. **Superseded Historical Version**: If an older version was superseded by a newer committed version, it is eligible only after:
   $$\text{age} = t_{\text{now}} - \text{created\_at} \ge \text{retention\_period}$$
4. **Uncommitted Orphan Chunks**: Chunks written to storage nodes where Raft commit failed or timed out are eligible after the orphan grace period (default 24 hours), preventing premature deletion during slow or concurrent uploads.

---

## 4. Formal CAP Trade-off Analysis

### 4.1 Fundamental Trade-off: CP Architecture
Under the Brewer CAP theorem, a distributed data store can guarantee at most two out of Consistency, Availability, and Partition Tolerance:

$$\mathbf{C} \ (\text{Consistency}) + \mathbf{P} \ (\text{Partition Tolerance}) \implies \neg \mathbf{A} \ (\text{Availability under Partition})$$

**Vault is explicitly and strictly designed as a CP system:**

> **Vault prioritizes committed metadata consistency and configured write durability over accepting writes without required quorum.**

In the presence of network partitions, isolated metadata followers, or storage node loss, Vault rejects writes with `HTTP 503 Service Unavailable` rather than accepting un-quorumed, stale, or potentially conflicting data.

### 4.2 Architectural Guarantees & Invariants
1. **Bounded Timeout Behavior**:
   - All inter-node communications (gateway $\to$ storage nodes, metadata Raft RPCs) enforce deterministic, bounded timeouts ($\le 2.0-3.0\text{s}$).
   - System never hangs or deadlocks under packet loss, connection drop, or network partition.
2. **No False Successful Writes**:
   - Vault **never** acknowledges a write (`HTTP 201 Created`) unless:
     a. Configured data write quorum ($W$) has verified storage node receipts.
     b. Object manifest has achieved strict majority consensus commit in Raft ($Q_{\text{meta}} = \lfloor N / 2 \rfloor + 1$).
3. **No Uncommitted Manifest Visibility**:
   - Uncommitted object manifests are **never visible** to clients. If data write quorum fails or Raft leader crashes prior to commit, the partial chunks are marked as orphan candidates for garbage collection, and subsequent reads return `HTTP 404 Not Found`.
4. **Policy-Specific Availability Behavior**:
   - **`hot` Policy** ($N=3, W=2, R=1$, $\ge 3$ zones): Continues serving reads if $\ge 1$ replica survives; accepts writes if $\ge 2$ replicas in $\ge 2$ distinct zones acknowledge.
   - **`durable` Policy** ($N=4, W=3, R=1$, $\ge 3$ zones): Requires 3 of 4 replicas for write durability.
   - **`archive` Policy** (Reed-Solomon $4+2$, $W=4, R=4$, $\ge 3$ zones): Tolerates arbitrary loss of any 2 storage nodes/fragments without data loss.
5. **No Acknowledged Object Becomes Unreadable**:
   - Any write acknowledged to a client withstands the failure or total isolation of an entire availability zone.
   - Surviving zones retain sufficient replicas or erasure-coded fragments to satisfy read quorum ($R$).
6. **Automatic Convergence & Self-Healing**:
   - When partitioned or rebooted nodes recover connectivity, background repair workers (`RepairWorker`) detect degraded replica sets, audit physical inventories against committed Raft manifests, and converge replica health back to 100% without operator intervention.

