# Vault Test Plan and Validation Strategy

## 1. Philosophy and Principles
- **No Fabricated Tests**: All tests execute against real code, databases, and network sockets.
- **Fail-Fast Quorum Validation**: Ensure failures occur at boundary conditions rather than producing silent corruption or inconsistent reads.
- **Two-Tier Test Architecture**:
  1. **Unit Tests (`tests/unit/`)**: Fast, in-memory algorithmic tests for placement, hashing, quorum math, Reed–Solomon EC, and CAS validation.
  2. **Integration & Chaos Tests (`tests/integration/`, `tests/chaos/`)**: Multi-container Docker Compose tests exercising real HTTP APIs, Raft leader elections, disk corruption injection, and network disconnects.

---

## 2. Unit Test Matrix

| ID | Test Name | Target Module | Description | Expected Outcome |
| :--- | :--- | :--- | :--- | :--- |
| **UT-01** | Deterministic HRW Placement | `vault_core.placement` | Computes placement for 1,000 distinct keys; verifies deterministic repeatability across runs. | Identical node ordering for identical inputs. |
| **UT-02** | Zone-First Placement Diversity | `vault_core.placement` | Evaluates placement across 6 nodes in 3 zones. | Maximum zone separation achieved before node reuse. |
| **UT-03** | Invalid Policy Validation | `vault_core.quorum` | Requests placement requiring 4 zones in a 3-zone cluster. | Raises `InsufficientZonesError` / rejected at validation. |
| **UT-04** | Quorum Bounds & Calculation | `vault_core.quorum` | Tests $W$, $R$, and min-survivor formulas for `hot`, `durable`, and `archive`. | Accurate quorum limits; rejects invalid quorum specs. |
| **UT-05** | Checksum Mismatch Detection | `vault_core.integrity` | Modifies 1 bit in a test chunk and executes SHA-256 verification. | Detects mismatch; triggers quarantine action. |
| **UT-06** | Stale Manifest CAS Rejection | `vault_core.manifest` | Simulates two writers updating version 1 with expected version 1. | First writer succeeds (v2); second receives `CASConflictError` (HTTP 409). |
| **UT-07** | Idempotency Key Semantics | `vault_core.manifest` | Submits duplicate write with identical `Idempotency-Key`. | Returns cached committed manifest without duplicate writes. |
| **UT-08** | Reed–Solomon Encode/Decode | `vault_core.erasure` | Encodes chunk into 4 data + 2 parity fragments; corrupts/drops any 2 fragments. | Exact byte reconstruction verified against original SHA-256. |
| **UT-09** | Safe Rebalance Copy-Before-Delete| `vault_core.rebalance` | Simulates node rebalance migration. | Source chunk is NEVER unlinked until target checksum is verified. |

---

## 3. Integration & Chaos Test Matrix (Docker Compose)

| Scenario | Objective | Injection Mechanism | Verification & Assertion |
| :--- | :--- | :--- | :--- |
| **IT-01** | 128 MiB Object Upload & Retrieval | Streaming 16 chunks (8 MiB each) through Gateway | Streamed SHA-256 matches input bytes byte-for-byte; no excessive RAM usage. |
| **IT-02** | Concurrent Multi-Client Load | 10 concurrent clients performing continuous PUT/GET | All 10 operations succeed; zero 500 errors; metrics track p50/p95/p99 latency. |
| **IT-03** | Concurrent CAS Conflict | 2 concurrent PUT requests targeting same key with expected version 1 | Exactly one writer succeeds (200 OK); the second receives HTTP 409 Conflict. |
| **IT-04** | Single Storage Node Failure During Write | Stop 1 storage container while client streams data | Write succeeds because $W=2$ quorum is met; repair backlog increments. |
| **IT-05** | Loss of Data Write Quorum | Stop 5 of 6 storage containers and initiate write | Gateway returns HTTP 503 Quorum Failure; no partial manifest is visible. |
| **IT-06** | Storage Node Failure After Upload | Stop node holding replica; initiate GET | Gateway fails over to healthy replica; reads return clean bytes; enqueues repair. |
| **IT-07** | Silent Chunk Bit Rot Detection | Overwrite bytes in a stored chunk on disk directly | Background scanner detects corruption, quarantines file, and repairs from replica. |
| **IT-08** | EC Fragment Corruption & Repair | Flip bits in 1 parity fragment of `archive` object | GET reconstructs data successfully; background repair rebuilds healthy fragment. |
| **IT-09** | Recovering Node Replica Convergence | Re-start previously stopped storage container | Background repair syncs missing chunks to the recovered node; replica count reaches 3. |
| **IT-10** | Add Node & Deterministic Rebalance | Register 7th storage node via Admin API | Rebalance moves designated HRW chunks to new node; verifies SHA-256 at destination. |
| **IT-11** | Rebalance Under Foreground Load | Execute background rebalance while clients run continuous reads/writes | 100% of reads succeed without error; no performance cliff or data loss. |
| **IT-12** | Partial Network Partition | Disconnect gateway from 2 storage nodes via Docker network disconnect | Bounded timeout triggers; requests fail over cleanly or reject safely. |
| **IT-13** | Isolate 1 Raft Metadata Follower | Disconnect `metadata-3` from network | Raft maintains quorum (2/3); metadata reads and writes proceed uninterrupted. |
| **IT-14** | Kill Raft Leader During Manifest Commit | `docker kill` active Raft leader during in-flight commit | Cluster elects new leader; uncommitted version never visible; system self-heals. |
| **IT-15** | Isolated Metadata Node Write Rejection | Route write directly to partitioned metadata follower | Request rejected with HTTP 503 or redirect; cannot commit conflicting split-brain log. |
| **IT-16** | Total Availability Zone Blackout | Stop all containers in `us-east-1a` (`storage-1`, `storage-2`)| Replicas in zones B and C continue serving reads and durable writes. |
| **IT-17** | End-to-End Metrics & State Auditing | Audit Prometheus metrics after chaos suite | Chunks verified, corrupt count, repair backlog, and rebalance bytes match exact events. |
