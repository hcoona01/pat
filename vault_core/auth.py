"""Inter-node shared-secret HMAC authentication for prototype cluster security."""

import hashlib
import hmac
import time
from typing import Optional


def create_auth_token(secret_key: str, node_id: str = "gateway") -> str:
    """Generate a signed HMAC-SHA256 authentication token with timestamp."""
    timestamp = int(time.time())
    message = f"{node_id}:{timestamp}".encode("utf-8")
    signature = hmac.new(secret_key.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"{node_id}:{timestamp}:{signature}"


def verify_auth_token(
    token: Optional[str],
    secret_key: str,
    max_drift_seconds: int = 300
) -> bool:
    """Validate token signature and freshness within allowable clock drift."""
    if not token:
        return False

    parts = token.split(":")
    if len(parts) != 3:
        return False

    node_id, timestamp_str, received_sig = parts
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        return False

    now = int(time.time())
    if abs(now - timestamp) > max_drift_seconds:
        return False

    expected_msg = f"{node_id}:{timestamp}".encode("utf-8")
    expected_sig = hmac.new(secret_key.encode("utf-8"), expected_msg, hashlib.sha256).hexdigest()

    return hmac.compare_digest(received_sig, expected_sig)
