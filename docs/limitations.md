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

### 1.7 Current Phase: Single-Node Baseline (No Fault Tolerance Yet)
### 1.7 Current Phase: Replicated Multi-Node Cluster (Hot & Durable Operational)
- **Status**: The cluster replication engine connects the API Gateway, 3-Node Raft metadata cluster, 4-tier HRW placement engine, and 6 independent storage nodes across 3 availability zones. Replicated policies (`hot` [3x, $W=2$] and `durable` [4x, $W=3$]) are fully functional and verified under storage node failure, quorum aborts, and concurrent workloads.
- **Limitation**: **Erasure coding (`archive` policy $4+2$) and active background repair workers** are scheduled for subsequent phases.

---

## 2. Requirement Status Tracker

| Requirement Category | Specified Capability | Implementation Status | Evidence / Notes |
| :--- | :--- | :--- | :--- |
| **Storage Engine & Replication** | 6 nodes across 3 zones, Hot & Durable write quorums, fault survival | ✅ Complete | Verified in `tests/integration/test_cluster_replication.py` (5/5 passed: node crash, quorum abort, multi-zone) |
| **Metadata Consensus** | 3-Node Raft cluster, CAS logical versions, leader election, partitions | ✅ Complete | Verified in `tests/integration/test_metadata_raft.py` (7/7 passed: election, CAS 409, partition recovery, leader loss) |
| **Placement Engine** | Deterministic Rendezvous HRW across regions & zones | ✅ Complete | Verified in `tests/unit/test_placement.py` (9/9 passed, 4-tier diversity, stability) |
| **Durability Policies** | `hot` (3x), `durable` (4x), `archive` (4+2 EC), quorum math | ✅ Complete | Verified in `tests/unit/test_placement.py` (policy validation, quorum bounds) |
| **Single-Node Core Engine** | Streaming PUT/GET/HEAD/DELETE, 8 MiB chunks, SHA-256, SQLite WAL, Idempotency | ✅ Complete | Verified in `tests/integration/test_single_node.py` (7/7 passed, including 128 MiB stream) |
| **Erasure Coding (EC)** | Reed–Solomon 4+2 archive policy codec and fragment dispersal | 🔄 In Progress (Design complete) | Scheduled for Phase 7 |
| **Integrity & Repair** | Periodic full scanner, read-repair, background repair worker | 🔄 In Progress (Quarantine active) | Scheduled for Phase 6 |
| **Membership & Rebalance**| Admin node addition, HRW migration, verify-before-delete | 🔄 In Progress (Design complete) | Rebalance workflow planned |
| **Testing & Chaos** | 17 comprehensive integration & fault scenarios | 🔄 In Progress (32 unit/int tests pass) | Chaos test scenarios advancing with cluster components |

