# Vault Empirical Evidence and Verification Log

> **MANDATORY POLICY**: In accordance with `PROJECT_RULES.md`, this document contains **NO fabricated evidence, mocked logs, or synthetic performance claims**. 
> All evidence sections below are initialized in an **unverified** baseline state and will be populated exclusively through automated executions of `./scripts/run_all_tests.sh`, `./scripts/run_demo.sh`, and `./scripts/collect_evidence.sh`.

---

## 1. Test Execution Evidence

| Test Suite | Run Timestamp | Environment | Total Tests | Passed | Failed | Status | Evidence File Reference |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Unit Tests** | *Pending Run* | Python 3.12 Local | - | - | - | ⏳ Pending | `docs/generated-evidence/unit-test.log` |
| **Integration Suite** | *Pending Run* | Docker Compose Cluster | - | - | - | ⏳ Pending | `docs/generated-evidence/integration-test.log` |
| **Chaos Scenarios** | *Pending Run* | Docker Network + Faults| - | - | - | ⏳ Pending | `docs/generated-evidence/chaos-test.log` |

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
```text
[Awaiting execution of scripts/inject_corruption.py and background scrubber detection]
```

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
