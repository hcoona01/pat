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

QUARANTINED_CHUNKS_TOTAL = Counter(
    "vault_quarantined_chunks_total",
    "Total number of chunks successfully moved to quarantine"
)

# Idempotency hits
IDEMPOTENT_HITS_TOTAL = Counter(
    "vault_idempotent_requests_total",
    "Total number of duplicate PUT requests served idempotently"
)


def get_latest_metrics() -> tuple[bytes, str]:
    """Generate latest Prometheus metrics formatted bytes and content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
