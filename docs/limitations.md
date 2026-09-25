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
- **Status**: The metadata consensus cluster is fully operational as a 3-node Raft cluster (`vault_core/metadata_raft.py`, `apps/metadata_node`), verified with leader election, follower partition recovery, leader loss re-election, CAS conflict rejection, and tombstone propagation.
- **Limitation**: **Multi-node storage data replication has not yet been connected to the Raft metadata cluster.** Storage nodes currently operate independently. Coordinated distributed writes (Phase 5) will bind the gateway, storage nodes, and Raft consensus together.

---

## 2. Requirement Status Tracker

| Requirement Category | Specified Capability | Implementation Status | Evidence / Notes |
| :--- | :--- | :--- | :--- |
| **Metadata Consensus** | 3-Node Raft cluster, CAS logical versions, leader election, partitions | ✅ Complete | Verified in `tests/integration/test_metadata_raft.py` (7/7 passed: election, CAS 409, partition recovery, leader loss) |
| **Single-Node Core Engine** | Streaming PUT/GET/HEAD/DELETE, 8 MiB chunks, SHA-256, SQLite WAL, Idempotency | ✅ Complete | Verified in `tests/integration/test_single_node.py` (7/7 passed, including 128 MiB stream) |
| **Durability Policies** | `hot` (3x), `durable` (4x), `archive` (4+2 EC), quorum math | ✅ Complete | Verified in `tests/unit/test_placement.py` (policy validation, quorum bounds) |
| **Placement Engine** | Deterministic Rendezvous HRW across regions & zones | ✅ Complete | Verified in `tests/unit/test_placement.py` (9/9 passed, 4-tier diversity, stability) |
| **Storage Engine** | 6 nodes, independent SQLite + chunks | 🔄 In Progress (Topology defined) | Single-node baseline operational; cluster rollout next |
| **Integrity & Repair** | SHA-256 validation, quarantine, background repair | 🔄 In Progress (Quarantine active) | Single-node on-read quarantine verified; background scrubber planned |
| **Membership & Rebalance**| Admin node addition, HRW migration, verify-before-delete | 🔄 In Progress (Design complete) | Rebalance workflow planned |
| **Testing & Chaos** | 17 comprehensive integration & fault scenarios | 🔄 In Progress (27 unit/int tests pass) | Chaos test scenarios advancing with cluster components |

