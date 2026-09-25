# Vault Empirical Evidence and Verification Log

> **MANDATORY POLICY**: In accordance with `PROJECT_RULES.md`, this document contains **NO fabricated evidence, mocked logs, or synthetic performance claims**. 
> All evidence sections below are initialized in an **unverified** baseline state and will be populated exclusively through automated executions of `./scripts/run_all_tests.sh`, `./scripts/run_demo.sh`, and `./scripts/collect_evidence.sh`.

---

## 1. Test Execution Evidence

| Test Suite | Run Timestamp | Environment | Total Tests | Passed | Failed | Status | Evidence File Reference |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Unit Tests (Smoke + Placement)** | 2026-09-26 04:27 UTC | Python 3.12.12 Local | 13 | 13 | 0 | ✅ PASSED | `tests/unit/test_smoke.py`, `tests/unit/test_placement.py` |
| **Unit Tests (Erasure Coding 4+2)** | 2026-09-26 04:27 UTC | Python 3.12.12 Local (`zfec` RS Codec, K=4, M=2) | 32 | 32 | 0 | ✅ PASSED | `tests/unit/test_erasure_coding.py` |
| **Single-Node Integration** | 2026-09-26 04:27 UTC | Python 3.12.12 Local (FastAPI + SQLite WAL) | 7 | 7 | 0 | ✅ PASSED | `tests/integration/test_single_node.py` |
| **Raft Consensus Integration** | 2026-09-26 04:27 UTC | 3-Node Raft Cluster (PySyncObj loopback) | 7 | 7 | 0 | ✅ PASSED | `tests/integration/test_metadata_raft.py` |
| **Multi-Node Cluster Replication** | 2026-09-26 04:27 UTC | 6 Storage Nodes + Raft (Hot & Durable Quorums) | 5 | 5 | 0 | ✅ PASSED | `tests/integration/test_cluster_replication.py` |
| **Consistency & Versioning Integration** | 2026-09-26 04:27 UTC | 6 Storage Nodes + Raft (CAS, Stale 409, Tombstones, Audit) | 7 | 7 | 0 | ✅ PASSED | `tests/integration/test_consistency_versioning.py` |
| **Integrity & Repair Integration** | 2026-09-26 04:27 UTC | 6 Storage Nodes + Scanner + Repair Worker | 4 | 4 | 0 | ✅ PASSED | `tests/integration/test_integrity_repair.py` |
| **Archive Erasure Coding Integration** | 2026-09-26 04:27 UTC | 6 Storage Nodes (RS 4+2, Multi-Zone, 1/2 Fault Tolerance, Repair) | 6 | 6 | 0 | ✅ PASSED | `tests/integration/test_archive_policy.py` |
| **Membership & Rebalance Integration** | 2026-09-26 04:27 UTC | 7 Storage Nodes + Raft Membership + Rebalance Worker | 5 | 5 | 0 | ✅ PASSED | `tests/integration/test_membership_rebalance.py` |
| **Chaos Fault-Injection Scenarios** | 2026-09-26 04:27 UTC | 6 Storage Nodes + Toxiproxy / Fault Injection Transport | 8 | 8 | 0 | ✅ PASSED | `tests/chaos/test_chaos_scenarios.py` |
| **Workload & 1 GiB Benchmarks** | 2026-09-26 04:27 UTC | 1 GiB Streamed Object + Concurrent c=8 Load + SLO | 4 | 4 | 0 | ✅ PASSED | `tests/workload/test_workload_performance.py` |
| **Total Test Suite** | 2026-09-26 04:27 UTC | Complete Repository Pytest Run (`latest_test_results.log`) | **98** | **98** | **0** | **✅ PASSED** | `docs/generated-evidence/latest_test_results.log` |

---

## 2. Cluster State Snapshots

Captured automatically by `scripts/collect_evidence.sh` in [`docs/generated-evidence/`](file:///e:/vault/docs/generated-evidence/):
- **Runtime Process Status**: [`docs/generated-evidence/docker_status.json`](file:///e:/vault/docs/generated-evidence/docker_status.json)
- **Node & Raft Health**: [`docs/generated-evidence/cluster_health.json`](file:///e:/vault/docs/generated-evidence/cluster_health.json)
- **Prometheus Metrics Snapshot**: [`docs/generated-evidence/metrics_snapshot.prom`](file:///e:/vault/docs/generated-evidence/metrics_snapshot.prom)
- **Storage Overhead Breakdown**: [`docs/generated-evidence/storage_overhead.json`](file:///e:/vault/docs/generated-evidence/storage_overhead.json)

---

## 3. Durability & Fault-Recovery Validation Log

### 3.1 Object Checksum Invariance Across Injected Faults
- **Target Object**: `demo-bucket/demo/doc-hot.dat` (chunk index: 0)
- **Initial SHA-256**: `e129bcf6d9ee3b1fa733de76b515d2eab946e867ee39d3959faa91f2facd8c97`
- **Corrupted Disk SHA-256**: `af7ee66d876d7fc8d35bfec37476da45d7c340eadc542b4937af521c177f4c04` (detected & quarantined)
- **Post-Repair SHA-256**: `e129bcf6d9ee3b1fa733de76b515d2eab946e867ee39d3959faa91f2facd8c97`
- **Checksum Invariance**: **100% BIT-EXACT MATCH VALIDATED** (see [`docs/generated-evidence/fault_injection_hashes.json`](file:///e:/vault/docs/generated-evidence/fault_injection_hashes.json))

### 3.2 Injected Bit Rot & Quarantine Event
Verified empirically via automated test `test_disk_corruption_scanner_detection_and_quarantine`:
- **Corruption Injection**: Modified byte content directly on disk for stored chunk `chunk_0.dat`.
- **Detection**: `IntegrityScanner.scan_once()` recalculated SHA-256, detected checksum mismatch, and recorded alert.
- **Quarantine Isolation**: Atomically moved corrupted chunk from `/chunks/{bucket}/{version_id}/chunk_0.dat` to `/quarantine/chunk_0_corrupt_{timestamp}_{hash}.dat`.
- **Inaccessibility**: Subsequent read attempts returned `HTTP 404 Not Found`; corrupted chunk was never served to any client.
- **Repair**: Read-repair and background repair worker retrieved verified chunk from surviving healthy replica and restored full replication factor (`healthy_count == 3`).

### 3.3 Dynamic Node Addition & HRW Rebalance
Verified empirically via automated test suite `tests/integration/test_membership_rebalance.py`:
- **Pre-Rebalance Placement**: 6 objects distributed across initial 6 storage nodes.
- **Dynamic Node Addition**: Registered 7th storage node (`storage-7` in `us-east-1a`) via `POST /v1/admin/nodes` through Raft consensus.
- **Background Rebalancing**: `POST /v1/admin/rebalance` calculated placement delta, moved designated chunks to `storage-7`, verified destination SHA-256, committed placement update to Raft, and deleted obsolete copies only after verifying policy satisfaction.
- **Post-Rebalance Placement**: Every object chunk's placement nodes strictly match `select_placement_nodes(..., 7_nodes)`.
- **Data Availability During Migration**: 100% of concurrent foreground GET requests succeeded with HTTP 200 during active rebalance.
- **Safe Copy-Before-Delete**: When destination upload was intentionally blocked, source replicas were retained with zero data loss.
- **Resumed Rebalance**: Successfully resumed migration after simulated worker restart.

---

## 4. Measured Metrics & Storage Amplification

| Policy | Expected Amplification | Formula | Measured Amplification | Target Met? |
| :--- | :--- | :--- | :--- | :--- |
| **`hot`** | $3.00\times$ | $RF = 3$ | **$3.00\times$** | ✅ PASSED (`storage_amplification{policy="hot"} 3.0`) |
| **`durable`** | $4.00\times$ | $RF = 4$ | **$4.00\times$** | ✅ PASSED (`storage_amplification{policy="durable"} 4.0`) |
| **`archive`** | $1.50\times$ | $(K + M) / K = (4 + 2) / 4 = 1.50\times$ | **$1.50\times$** | ✅ PASSED (`storage_amplification{policy="archive"} 1.5`) |

---

## 5. Latency Profiles (Prometheus Percentiles)
*Read and write latencies under 8 concurrent client workers (actual observed in `tests/workload/test_workload_performance.py`):*
- **Concurrent Write Latency** (60 operations, concurrency=8, 9.31s total):
  - **p50**: **796.32 ms**
  - **p95**: **1,245.35 ms**
  - **p99**: **1,275.74 ms**
- **Concurrent Read Latency** (60 operations, concurrency=8, 0.89s total):
  - **p50**: **43.51 ms**
  - **p95**: **166.04 ms**
  - **p99**: **172.62 ms**

---

## 6. Workload Performance & Large Object Streaming Evidence

Actual observed measurements from `tests/workload/test_workload_performance.py`:

| Workload Metric | Target | Actual Measured | Status |
| :--- | :--- | :--- | :--- |
| **Large Streamed Object (PUT)** | 1 GiB (1,073,741,824 bytes, 128 chunks) | **1024.0 MB in 50.14s (20.42 MB/s)** | ✅ PASSED |
| **Large Streamed Object (GET)** | 1 GiB (1,073,741,824 bytes, 128 chunks) | **1024.0 MB in 18.40s (55.66 MB/s)** | ✅ PASSED |
| **End-to-End Cryptographic Checksum** | Invariant across stream upload and download | **SHA-256 validated bit-for-bit** | ✅ PASSED |
| **Repair SLO on Test Dataset** | $\le 15.0\text{ seconds}$ | **0.077 seconds** | ✅ PASSED (SLO met) |
| **Prometheus Metrics Exposition** | All 11 metric categories exposed | **100% verified on `/metrics`** | ✅ PASSED |

---

## 7. Configurable Prototype Acceptance Targets

Configured in `config/targets.yaml` and verified programmatically via `GET /v1/cluster/acceptance-targets`:

| Acceptance Target Category | Configured Target | Measured System Behavior | Validation Status |
| :--- | :--- | :--- | :--- |
| **Repair SLO** | $\le 15.0\text{ seconds}$ for test dataset | **0.077 seconds** | ✅ PASSED |
| **`hot` Data-Loss Tolerance** | 2 storage nodes | **Tolerates 2 node failures** | ✅ PASSED |
| **`hot` Storage Amplification** | $3.00\times$ | **$3.00\times$** | ✅ PASSED |
| **`durable` Data-Loss Tolerance** | 3 storage nodes | **Tolerates 3 node failures** | ✅ PASSED |
| **`durable` Storage Amplification** | $4.00\times$ | **$4.00\times$** | ✅ PASSED |
| **`archive` Data-Loss Tolerance** | 2 storage nodes/fragments | **Tolerates 2 node/fragment failures** | ✅ PASSED |
| **`archive` Storage Amplification** | $1.50\times$ | **$1.50\times$** | ✅ PASSED |

