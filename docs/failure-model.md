# Vault Failure Model and Fault Recovery

## 1. Fault Domain Definitions
Vault models faults across four distinct levels of granularity:
1. **Drive / Chunk Level**: Silent bit rot, incomplete writes, truncated files, disk I/O errors.
2. **Process / Node Level**: Crash-stop failures, out-of-memory kills, ungraceful daemon restarts.
3. **Zone Level**: Power or top-of-rack network switch failure taking down all nodes in an entire availability zone (e.g., `us-east-1a`).
4. **Network Partition Level**: Asymmetric or symmetric message loss between nodes, transient latency spikes, and partitioned Raft majorities/minorities.

---

## 2. Failure Scenarios and System Response

### 2.1 Storage Node Crash-Stop
* **During Read**: If a target storage node fails to respond within the connection timeout (default: 1.5s), the gateway immediately fails over to the next candidate replica in the manifest's placement list.
* **During Write**: If a storage node crashes during chunk streaming:
  - If the remaining active nodes satisfy the policy's `data_write_quorum` ($W$), the write proceeds, the manifest is committed, and the missing replica is enqueued for background repair.
  - If surviving nodes $< W$, the write immediately aborts, returning `HTTP 503 Quorum Unavailable`. No manifest is committed.
* **Post-Recovery**: When the crashed node restarts, it verifies its local SQLite inventory against disk. The background repair worker detects stale/missing chunks and brings the node to convergence.

### 2.2 Metadata Cluster Node Crash (Raft)
* **Follower Crash ($1/3$ nodes down)**: The Raft leader maintains consensus with the remaining active follower ($2/3$ nodes = majority). Metadata reads and writes continue with zero interruption.
* **Leader Crash**: The surviving followers detect heartbeat loss within the election timeout window (typically 150–300ms) and elect a new leader.
  - In-flight manifest commits without quorum are safely discarded.
  - No partial or uncommitted version ever becomes visible.
* **Loss of Raft Quorum ($2/3$ nodes down)**: The cluster enters read-only safe mode or rejects new writes with `HTTP 503 Metadata Quorum Lost`. Write operations are strictly blocked to prevent split-brain.

### 2.3 Silent Data Corruption (Bit Rot)
* **Detection**: Every chunk write requires a client/gateway computed SHA-256 hash. Storage nodes compute SHA-256 during streaming ingestion.
* **On-Read Verification**: Storage nodes verify SHA-256 checksums before serving bytes. If a hash mismatch occurs:
  1. The storage node logs an error and atomically moves the corrupt chunk file into a `.quarantine/` directory.
  2. The storage node returns `HTTP 410 Corrupted Chunk`.
  3. The gateway fails over to a healthy replica, returns clean data to the client, and enqueues a background repair job.
* **Active Scrubbing**: The background integrity scanner iterates through every local chunk at a scheduled interval, independently verifying SHA-256 against the SQLite inventory and isolating corrupted data before clients ever request it.

### 2.4 Zone Outage (Simultaneous Failure of All Nodes in 1 Zone)
* By policy, every chunk's placement vector is strictly distributed across **at least 3 distinct availability zones**.
* In a 6-node cluster configured across 3 zones (`us-east-1a`, `us-east-1b`, `us-east-1c` with 2 nodes each):
  - A total blackout of Zone A leaves 4 storage nodes active in Zones B and C.
  - **Hot Policy ($N=3, W=2, R=1$)**: Retains 2 valid replicas across Zones B and C $\rightarrow$ **100% read and write availability maintained**.
  - **Archive Policy ($4+2=6$)**: Retains 4 surviving fragments across Zones B and C $\rightarrow$ **Read reconstruction succeeds without data loss**.

### 2.5 Network Partitions and Split-Brain
* **Bounded Timeouts**: All inter-node HTTP requests use bounded timeouts (connect: 1.0s, read: 3.0s) and exponential backoff with jitter (max 3 retries).
* **Partitioned Minority**: Nodes isolated in a minority network partition cannot satisfy Raft consensus and cannot fulfill write quorums. Vault guarantees that no write succeeds in a partitioned minority.
