"""Prometheus metrics collector and definitions for Vault."""

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Request metrics
HTTP_REQUESTS_TOTAL = Counter(
    "vault_http_requests_total",
    "Total number of HTTP requests processed",
    ["method", "endpoint", "status"]
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "vault_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# Object storage counters
OBJECTS_TOTAL = Gauge(
    "vault_objects_total",
    "Current total number of active objects"
)

CHUNKS_TOTAL = Gauge(
    "vault_chunks_total",
    "Current total number of stored chunks"
)

BYTES_STORED_TOTAL = Gauge(
    "vault_bytes_stored_total",
    "Total raw bytes of chunk data stored on disk"
)

# Corruption and integrity counters
CORRUPT_CHUNKS_DETECTED_TOTAL = Counter(
    "vault_corrupt_chunks_detected_total",
    "Total number of corrupt chunks detected via checksum validation"
)

LAST_FULL_SCAN_AT = Gauge(
    "last_full_scan_at",
    "Timestamp of last completed full integrity scan"
)

CHUNKS_VERIFIED_TOTAL = Counter(
    "chunks_verified_total",
    "Total chunks verified via cryptographic hash"
)

CORRUPT_CHUNKS_TOTAL = Counter(
    "corrupt_chunks_total",
    "Total corrupt chunks detected via checksum validation"
)

QUARANTINED_CHUNKS_TOTAL = Counter(
    "quarantined_chunks_total",
    "Total number of chunks successfully moved to quarantine"
)

# Active Repair Worker Metrics
REPAIR_BACKLOG = Gauge(
    "repair_backlog",
    "Current number of chunk replicas pending background repair"
)

REPAIR_SUCCESS_TOTAL = Counter(
    "repair_success_total",
    "Total number of successfully repaired chunk replicas"
)

REPAIR_FAILURE_TOTAL = Counter(
    "repair_failure_total",
    "Total number of failed replica repair attempts"
)

REPAIR_DURATION_SECONDS = Histogram(
    "repair_duration_seconds",
    "Duration of replica repair operations in seconds",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# Storage amplification factor by durability policy
STORAGE_AMPLIFICATION = Gauge(
    "storage_amplification",
    "Measured storage amplification ratio by durability policy",
    ["policy"]
)
STORAGE_AMPLIFICATION.labels(policy="archive").set(1.5)
STORAGE_AMPLIFICATION.labels(policy="hot").set(3.0)
STORAGE_AMPLIFICATION.labels(policy="durable").set(4.0)

# Idempotency hits
IDEMPOTENT_HITS_TOTAL = Counter(
    "vault_idempotent_requests_total",
    "Total number of duplicate PUT requests served idempotently"
)


# Rebalance worker metrics
REBALANCE_QUEUED_TOTAL = Gauge(
    "vault_rebalance_queued_tasks",
    "Current number of chunk migration tasks queued for rebalancing"
)

REBALANCE_COPIED_TOTAL = Counter(
    "vault_rebalance_copied_total",
    "Total number of chunks copied during rebalance operations"
)

REBALANCE_VERIFIED_TOTAL = Counter(
    "vault_rebalance_verified_total",
    "Total number of chunks destination-verified during rebalance operations"
)

REBALANCE_FAILED_TOTAL = Counter(
    "vault_rebalance_failed_total",
    "Total number of failed chunk rebalance attempts"
)

REBALANCE_BYTES_MOVED_TOTAL = Counter(
    "vault_rebalance_bytes_moved_total",
    "Total bytes of chunk data transferred during rebalancing"
)

REBALANCE_PROGRESS_RATIO = Gauge(
    "vault_rebalance_progress_ratio",
    "Rebalance completion percentage ratio from 0.0 to 1.0"
)

REBALANCE_ACTIVE = Gauge(
    "vault_rebalance_active",
    "Whether a rebalance operation is currently running (1 for active, 0 for idle)"
)


def get_latest_metrics() -> tuple[bytes, str]:
    """Generate latest Prometheus metrics formatted bytes and content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
