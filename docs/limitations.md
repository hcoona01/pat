# Vault: Honest Known Limitations and Architectural Boundaries

> **Notice**: Vault is a hackathon-grade prototype engineered to demonstrate core distributed systems algorithms (Raft replicated state machines, Highest Random Weight rendezvous placement, quorum write semantics, cryptographic bit rot quarantine, Reed–Solomon erasure coding, and active background repair). It is **not** hardened for production storage workloads.

---

## 1. Prototype Limitations vs. Production Systems

### 1.1 Cluster Membership Discovery
- **Limitation**: Node membership is **admin-managed** via configuration (`config/cluster.yaml`) and explicitly committed administrative APIs.
- **Production Contrast**: Production systems (e.g., Cassandra, Garage, CockroachDB) use automated gossip protocols (SWIM / Scuttlebutt) with failure detectors (Phi Accrual) for autonomic node discovery.

### 1.2 Authentication, Authorization & Security
- **Limitation**: Inter-node API communication relies on a shared HMAC secret key with timestamp replay protection.
- **Production Contrast**: Lacks enterprise multi-tenant IAM, role-based access control (RBAC), signed mTLS between internal microservices, and client authentication (such as AWS Signature Version 4).

### 1.3 Consensus Boundaries & Multi-Region Topology
- **Limitation**: The Raft consensus group operates within a single logical region across low-latency availability zones.
- **Production Contrast**: Does not implement multi-region consensus (e.g., Multi-Raft or WAN-spanning Paxos) due to speed-of-light cross-continental latency limitations.

### 1.4 Single API Gateway in Default Compose
- **Limitation**: The baseline Docker Compose topology defines 1 API Gateway instance. While the gateway is completely stateless, running a single instance constitutes a single point of entry for client traffic.
- **Production Contrast**: Requires a resilient upstream layer-4 / layer-7 load balancer (e.g., HAProxy, Envoy, or AWS NLB) distributing requests across a horizontally scaled pool of gateways.

### 1.5 Local Storage Backend Engine
- **Limitation**: Storage nodes use the local POSIX filesystem combined with SQLite (`aiosqlite` in WAL mode) for inventory indexing.
- **Production Contrast**: Production object engines (such as Ceph BlueStore or MinIO) interact directly with block devices or bypass POSIX kernel caches using direct I/O (O_DIRECT) and asynchronous I/O (io_uring).

### 1.7 Current Phase: Integrity Verification & Prioritized Repair (Operational)
- **Status**: Periodic full integrity scanning, bit rot detection, quarantine isolation, read repair, and rate-limited prioritized background replica repair are fully operational. Storage nodes recalculate SHA-256 for all stored chunks and isolate corrupt files into quarantine so they are never served. The repair worker prioritizes objects with the fewest surviving replicas, enforces strict cryptographic verification on source and destination, and exposes Prometheus metrics (`last_full_scan_at`, `chunks_verified_total`, `corrupt_chunks_total`, `quarantined_chunks_total`, `repair_backlog`, `repair_success_total`, `repair_failure_total`, `repair_duration_seconds`).
- **Limitation**: **Erasure coding (`archive` policy $4+2$) and dynamic node rebalancing** are scheduled for subsequent phases.

---

## 2. Requirement Status Tracker

| Requirement Category | Specified Capability | Implementation Status | Evidence / Notes |
| :--- | :--- | :--- | :--- |
| **Integrity & Repair** | Full periodic scanner, quarantine, read-repair, prioritized repair | ✅ Complete | Verified in `tests/integration/test_integrity_repair.py` (4/4 passed: bit rot injection, scanner detection, quarantine isolation, read repair, prioritized recovery) |
| **Object Versioning & Consistency** | Immutable UUIDs, CAS writes, HTTP 409, tombstones, audit, explicit states | ✅ Complete | Verified in `tests/integration/test_consistency_versioning.py` (7/7 passed: CAS writers, 409, tombstones, old replica isolation, audit states) |
| **Storage Engine & Replication** | 6 nodes across 3 zones, Hot & Durable write quorums, fault survival | ✅ Complete | Verified in `tests/integration/test_cluster_replication.py` (5/5 passed: node crash, quorum abort, multi-zone) |
| **Metadata Consensus** | 3-Node Raft cluster, CAS logical versions, leader election, partitions | ✅ Complete | Verified in `tests/integration/test_metadata_raft.py` (7/7 passed: election, CAS 409, partition recovery, leader loss) |
| **Placement Engine** | Deterministic Rendezvous HRW across regions & zones | ✅ Complete | Verified in `tests/unit/test_placement.py` (9/9 passed, 4-tier diversity, stability) |
| **Durability Policies** | `hot` (3x), `durable` (4x), `archive` (4+2 EC), quorum math | ✅ Complete | Verified in `tests/unit/test_placement.py` (policy validation, quorum bounds) |
| **Single-Node Core Engine** | Streaming PUT/GET/HEAD/DELETE, 8 MiB chunks, SHA-256, SQLite WAL, Idempotency | ✅ Complete | Verified in `tests/integration/test_single_node.py` (7/7 passed, including 128 MiB stream) |
| **Erasure Coding (EC)** | Reed–Solomon 4+2 archive policy codec and fragment dispersal | 🔄 In Progress (Design complete) | Scheduled for Phase 8 |
| **Membership & Rebalance**| Admin node addition, HRW migration, verify-before-delete | 🔄 In Progress (Design complete) | Rebalance workflow planned |
| **Testing & Chaos** | Comprehensive integration & fault scenarios | 🔄 In Progress (43 unit/int tests pass) | Chaos test scenarios advancing with cluster components |

