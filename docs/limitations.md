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

### 1.6 Erasure Coding Performance
- **Limitation**: Reed–Solomon matrix encoding/decoding is performed in Python via `zfec` / C-Python wrappers.
- **Production Contrast**: Production systems utilize hardware-accelerated SIMD instructions (Intel ISA-L AVX-512 / ARM NEON) for multi-gigabyte-per-second throughput per core.

---

## 2. Requirement Status Tracker

| Requirement Category | Specified Capability | Implementation Status | Evidence / Notes |
| :--- | :--- | :--- | :--- |
| **Metadata Consensus** | 3-Node Raft cluster, CAS logical versions | 🔄 In Progress (Design complete) | Raft state machine defined; tests in Phase 4 |
| **Durability Policies** | `hot` (3x), `durable` (4x), `archive` (4+2 EC) | 🔄 In Progress (Config complete) | Policy spec defined in `config/policies.yaml` |
| **Placement Engine** | Deterministic Rendezvous HRW across zones | 🔄 In Progress (Design complete) | Zone-first diversity logic planned |
| **Storage Engine** | 6 nodes, independent SQLite + chunks | 🔄 In Progress (Design complete) | Docker compose topology defined |
| **Integrity & Repair** | SHA-256 validation, quarantine, background repair | 🔄 In Progress (Design complete) | Repair flows and test plan defined |
| **Membership & Rebalance**| Admin node addition, HRW migration, verify-before-delete | 🔄 In Progress (Design complete) | Rebalance workflow planned |
| **Testing & Chaos** | 17 comprehensive integration & fault scenarios | 🔄 In Progress (Test plan complete) | Automated suite planned for Phase 9 |
