"""Prometheus metrics collector, rolling percentile tracker, and definitions for Vault."""

import collections
from typing import Tuple
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# =============================================================================
# 1. API REQUESTS, SUCCESS/FAILURE COUNTS
# =============================================================================

HTTP_REQUESTS_TOTAL = Counter(
    "vault_http_requests_total",
    "Total number of HTTP requests processed",
    ["method", "endpoint", "status"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "vault_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

API_SUCCESS_TOTAL = Counter(
    "vault_api_success_total",
    "Total successful high-level API operations",
    ["operation"],
)

API_FAILURE_TOTAL = Counter(
    "vault_api_failure_total",
    "Total failed high-level API operations",
    ["operation", "error_type"],
)

# =============================================================================
# 2. OPERATION LATENCY & P50/P95/P99 PERCENTILES
# =============================================================================

OPERATION_LATENCY_SECONDS = Histogram(
    "vault_operation_latency_seconds",
    "Latency of core storage operations in seconds",
    ["operation"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

READ_LATENCY_P50 = Gauge("vault_read_latency_p50_seconds", "Rolling p50 read latency in seconds")
READ_LATENCY_P95 = Gauge("vault_read_latency_p95_seconds", "Rolling p95 read latency in seconds")
READ_LATENCY_P99 = Gauge("vault_read_latency_p99_seconds", "Rolling p99 read latency in seconds")

WRITE_LATENCY_P50 = Gauge("vault_write_latency_p50_seconds", "Rolling p50 write latency in seconds")
WRITE_LATENCY_P95 = Gauge("vault_write_latency_p95_seconds", "Rolling p95 write latency in seconds")
WRITE_LATENCY_P99 = Gauge("vault_write_latency_p99_seconds", "Rolling p99 write latency in seconds")


class RollingLatencyTracker:
    """Sliding-window latency percentile calculator (p50, p95, p99)."""

    def __init__(self, max_samples: int = 1000) -> None:
        self._samples: collections.deque[float] = collections.deque(maxlen=max_samples)

    def record(self, latency_seconds: float) -> None:
        self._samples.append(max(0.0001, latency_seconds))

    def quantiles(self) -> Tuple[float, float, float]:
        if not self._samples:
            return 0.0, 0.0, 0.0
        sorted_samples = sorted(self._samples)
        n = len(sorted_samples)
        p50 = sorted_samples[int(n * 0.50)]
        p95 = sorted_samples[min(int(n * 0.95), n - 1)]
        p99 = sorted_samples[min(int(n * 0.99), n - 1)]
        return p50, p95, p99


READ_LATENCY_TRACKER = RollingLatencyTracker()
WRITE_LATENCY_TRACKER = RollingLatencyTracker()


def record_operation_metrics(
    operation: str,
    duration_seconds: float,
    success: bool = True,
    error_type: str = "",
) -> None:
    """Record operation latency and update rolling p50/p95/p99 percentiles."""
    OPERATION_LATENCY_SECONDS.labels(operation=operation).observe(duration_seconds)
    if success:
        API_SUCCESS_TOTAL.labels(operation=operation).inc()
    else:
        API_FAILURE_TOTAL.labels(operation=operation, error_type=error_type or "unknown").inc()

    if operation == "read":
        READ_LATENCY_TRACKER.record(duration_seconds)
        p50, p95, p99 = READ_LATENCY_TRACKER.quantiles()
        READ_LATENCY_P50.set(p50)
        READ_LATENCY_P95.set(p95)
        READ_LATENCY_P99.set(p99)
    elif operation == "write":
        WRITE_LATENCY_TRACKER.record(duration_seconds)
        p50, p95, p99 = WRITE_LATENCY_TRACKER.quantiles()
        WRITE_LATENCY_P50.set(p50)
        WRITE_LATENCY_P95.set(p95)
        WRITE_LATENCY_P99.set(p99)


# =============================================================================
# 3. NODE AVAILABILITY & TOPOLOGY METRICS
# =============================================================================

STORAGE_NODE_ACTIVE = Gauge(
    "vault_storage_node_active",
    "Availability status of each storage node (1=active, 0=inactive)",
    ["node_id", "zone"],
)

STORAGE_NODES_ACTIVE_TOTAL = Gauge(
    "vault_storage_nodes_active_total",
    "Total active storage nodes in the cluster topology",
)

# =============================================================================
# 4. RAFT LEADER & QUORUM HEALTH METRICS
# =============================================================================

RAFT_IS_LEADER = Gauge(
    "vault_raft_is_leader",
    "Whether local metadata node is currently elected Raft leader (1=yes, 0=no)",
)

RAFT_LEADER_ELECTED = Gauge(
    "vault_raft_leader_elected",
    "Whether a cluster Raft leader is currently recognized (1=yes, 0=no)",
)

RAFT_QUORUM_HEALTHY = Gauge(
    "vault_raft_quorum_healthy",
    "Whether metadata consensus quorum is healthy (1=healthy, 0=unhealthy)",
)

RAFT_PEERS_ACTIVE = Gauge(
    "vault_raft_peers_active",
    "Number of active connected peers in the Raft metadata cluster",
)

# =============================================================================
# 5. REPLICA HEALTH, INTEGRITY, BITROT & QUARANTINE
# =============================================================================

UNDER_REPLICATED_OBJECTS = Gauge(
    "vault_under_replicated_objects",
    "Current count of objects with degraded, missing, or corrupt replicas",
)

OBJECTS_TOTAL = Gauge("vault_objects_total", "Current total number of active objects")
CHUNKS_TOTAL = Gauge("vault_chunks_total", "Current total number of stored chunks")
BYTES_STORED_TOTAL = Gauge("vault_bytes_stored_total", "Total raw bytes of chunk data stored on disk")

LAST_FULL_SCAN_AT = Gauge("last_full_scan_at", "Timestamp of last completed full integrity scan")
CHUNKS_VERIFIED_TOTAL = Counter("chunks_verified_total", "Total chunks verified via cryptographic hash")
CORRUPT_CHUNKS_TOTAL = Counter("corrupt_chunks_total", "Total corrupt chunks detected via checksum validation")
QUARANTINED_CHUNKS_TOTAL = Counter("quarantined_chunks_total", "Total number of chunks successfully moved to quarantine")
CORRUPT_CHUNKS_DETECTED_TOTAL = Counter("vault_corrupt_chunks_detected_total", "Total corrupt chunks detected via checksum validation")

# =============================================================================
# 6. REPAIR BACKLOG & DURATION
# =============================================================================

REPAIR_BACKLOG = Gauge("repair_backlog", "Current number of chunk replicas pending background repair")
REPAIR_SUCCESS_TOTAL = Counter("repair_success_total", "Total number of successfully repaired chunk replicas")
REPAIR_FAILURE_TOTAL = Counter("repair_failure_total", "Total number of failed replica repair attempts")
REPAIR_DURATION_SECONDS = Histogram(
    "repair_duration_seconds",
    "Duration of replica repair operations in seconds",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# =============================================================================
# 7. REBALANCE PROGRESS & BYTES MOVED
# =============================================================================

REBALANCE_QUEUED_TOTAL = Gauge("vault_rebalance_queued_tasks", "Current number of chunk migration tasks queued for rebalancing")
REBALANCE_COPIED_TOTAL = Counter("vault_rebalance_copied_total", "Total number of chunks copied during rebalance operations")
REBALANCE_VERIFIED_TOTAL = Counter("vault_rebalance_verified_total", "Total number of chunks destination-verified during rebalance operations")
REBALANCE_FAILED_TOTAL = Counter("vault_rebalance_failed_total", "Total number of failed chunk rebalance attempts")
REBALANCE_BYTES_MOVED_TOTAL = Counter("vault_rebalance_bytes_moved_total", "Total bytes of chunk data transferred during rebalancing")
REBALANCE_PROGRESS_RATIO = Gauge("vault_rebalance_progress_ratio", "Rebalance completion percentage ratio from 0.0 to 1.0")
REBALANCE_ACTIVE = Gauge("vault_rebalance_active", "Whether a rebalance operation is currently running (1 for active, 0 for idle)")

# =============================================================================
# 8. STORAGE AMPLIFICATION BY POLICY
# =============================================================================

STORAGE_AMPLIFICATION = Gauge(
    "storage_amplification",
    "Measured storage amplification ratio by durability policy",
    ["policy"],
)
STORAGE_AMPLIFICATION.labels(policy="archive").set(1.5)
STORAGE_AMPLIFICATION.labels(policy="hot").set(3.0)
STORAGE_AMPLIFICATION.labels(policy="durable").set(4.0)

# =============================================================================
# 9. QUORUM & NETWORK TIMEOUT FAILURES
# =============================================================================

DATA_QUORUM_FAILURES_TOTAL = Counter(
    "vault_data_quorum_failures_total",
    "Total write operations rejected due to data quorum failure",
    ["policy"],
)

METADATA_QUORUM_FAILURES_TOTAL = Counter(
    "vault_metadata_quorum_failures_total",
    "Total operations rejected due to Raft metadata quorum failure",
)

NETWORK_TIMEOUT_FAILURES_TOTAL = Counter(
    "vault_network_timeout_failures_total",
    "Total inter-node requests failing due to network timeout",
    ["target_node"],
)

IDEMPOTENT_HITS_TOTAL = Counter(
    "vault_idempotent_requests_total",
    "Total number of duplicate PUT requests served idempotently",
)


def get_latest_metrics() -> Tuple[bytes, str]:
    """Generate latest Prometheus metrics formatted bytes and content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
