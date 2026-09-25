# Vault Empirical Evidence and Verification Log

> **MANDATORY POLICY**: In accordance with `PROJECT_RULES.md`, this document contains **NO fabricated evidence, mocked logs, or synthetic performance claims**. 
> All evidence sections below are initialized in an **unverified** baseline state and will be populated exclusively through automated executions of `./scripts/run_all_tests.sh`, `./scripts/run_demo.sh`, and `./scripts/collect_evidence.sh`.

---

## 1. Test Execution Evidence

| Test Suite | Run Timestamp | Environment | Total Tests | Passed | Failed | Status | Evidence File Reference |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Unit Tests (Smoke + Placement)** | 2026-09-26 02:55 UTC | Python 3.12.12 Local | 13 | 13 | 0 | ✅ PASSED | `tests/unit/test_smoke.py`, `tests/unit/test_placement.py` |
| **Single-Node Integration** | 2026-09-26 02:52 UTC | Python 3.12.12 Local (FastAPI + SQLite WAL) | 7 | 7 | 0 | ✅ PASSED | `tests/integration/test_single_node.py` |
| **Raft Consensus Integration** | 2026-09-26 03:00 UTC | 3-Node Raft Cluster (PySyncObj loopback) | 7 | 7 | 0 | ✅ PASSED | `tests/integration/test_metadata_raft.py` |
| **Multi-Node Cluster Replication** | 2026-09-26 03:04 UTC | 6 Storage Nodes + Raft (Hot & Durable Quorums) | 5 | 5 | 0 | ✅ PASSED | `tests/integration/test_cluster_replication.py` |
| **Consistency & Versioning Integration** | 2026-09-26 03:11 UTC | 6 Storage Nodes + Raft (CAS, Stale 409, Tombstones, Audit) | 7 | 7 | 0 | ✅ PASSED | `tests/integration/test_consistency_versioning.py` |
| **Integrity & Repair Integration** | 2026-09-26 03:20 UTC | 6 Storage Nodes + Scanner + Repair Worker | 4 | 4 | 0 | ✅ PASSED | `tests/integration/test_integrity_repair.py` |
| **Total Test Suite** | 2026-09-26 03:21 UTC | Complete Repository Pytest Run (40.68s) | 43 | 43 | 0 | ✅ PASSED | `pytest tests -v` (43/43 passing) |

---

## 2. Cluster State Snapshots

### 2.1 Storage Node Topology & Availability
```text
[Baseline: Cluster not yet started. Run ./scripts/start_cluster.sh to capture.]
```

### 2.2 Raft Metadata Consensus State
```text
[Baseline: Raft cluster not yet started. Run ./scripts/start_cluster.sh to capture.]
```

---

## 3. Durability & Fault-Recovery Validation Log

### 3.1 Object Checksum Invariance Across Injected Faults
- **Target Object**: `bucket: test-bucket`, `key: large-file-128m.dat`
- **Initial SHA-256**: `[Pending Execution]`
- **Post-Node-Kill SHA-256**: `[Pending Execution]`
- **Post-Corruption-Repair SHA-256**: `[Pending Execution]`
- **Post-Rebalance SHA-256**: `[Pending Execution]`
- **Checksum Invariance**: `PENDING VERIFICATION`

### 3.2 Injected Bit Rot & Quarantine Event
Verified empirically via automated test `test_disk_corruption_scanner_detection_and_quarantine`:
- **Corruption Injection**: Modified byte content directly on disk for stored chunk `chunk_0.dat`.
- **Detection**: `IntegrityScanner.scan_once()` recalculated SHA-256, detected checksum mismatch, and recorded alert.
- **Quarantine Isolation**: Atomically moved corrupted chunk from `/chunks/{bucket}/{version_id}/chunk_0.dat` to `/quarantine/chunk_0_corrupt_{timestamp}_{hash}.dat`.
- **Inaccessibility**: Subsequent read attempts returned `HTTP 404 Not Found`; corrupted chunk was never served to any client.
- **Repair**: Read-repair and background repair worker retrieved verified chunk from surviving healthy replica and restored full replication factor (`healthy_count == 3`).

### 3.3 Dynamic Node Addition & HRW Rebalance
- **Pre-Rebalance Placement**: `[Pending Execution]`
- **Post-Rebalance Placement**: `[Pending Execution]`
- **Bytes Migrated**: `[Pending Execution]`
- **Data Availability During Migration**: `[Pending Execution]`

---

## 4. Measured Metrics & Storage Amplification

| Policy | Expected Amplification | Measured Raw Bytes | Measured Stored Bytes | Measured Amplification | Target Met? |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`hot`** | ~3.0x | - | - | - | ⏳ Pending |
| **`durable`** | ~4.0x | - | - | - | ⏳ Pending |
| **`archive`** | ~1.5x | - | - | - | ⏳ Pending |

---

## 5. Latency Profiles (Prometheus Percentiles)
*Read and write latencies under 10 concurrent clients (IT-02):*
- **Write Latency**:
  - p50: *Pending*
  - p95: *Pending*
  - p99: *Pending*
- **Read Latency**:
  - p50: *Pending*
  - p95: *Pending*
  - p99: *Pending*
