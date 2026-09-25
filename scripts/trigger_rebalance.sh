#!/usr/bin/env bash
# =============================================================================
# Vault Rebalance: Trigger and Monitor
# =============================================================================
set -euo pipefail

RATE_LIMIT="${1:-50.0}"
GATEWAY_URL="${2:-http://localhost:8000}"

echo ">>> Triggering background cluster rebalance (rate limit: ${RATE_LIMIT} ops/sec)..."

TRIGGER_RESP=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/v1/admin/rebalance?rate_limit=${RATE_LIMIT}")
HTTP_BODY=$(echo "${TRIGGER_RESP}" | sed '$d')
HTTP_CODE=$(echo "${TRIGGER_RESP}" | tail -n1)

if [ "${HTTP_CODE}" -ne 200 ]; then
    echo "ERROR: Failed to trigger rebalance. HTTP ${HTTP_CODE}"
    echo "${HTTP_BODY}"
    exit 1
fi

echo "Rebalance initiated successfully."
echo "${HTTP_BODY}"

echo ">>> Monitoring rebalance progress..."
for i in $(seq 1 30); do
    STATUS_RESP=$(curl -s "${GATEWAY_URL}/v1/admin/rebalance/status" || echo '{"status":"unreachable"}')
    echo "Status [${i}s]: ${STATUS_RESP}"
    
    STATE=$(echo "${STATUS_RESP}" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
    if [ "${STATE}" = "completed" ]; then
        echo ">>> Rebalance COMPLETED successfully!"
        exit 0
    elif [ "${STATE}" = "failed" ]; then
        echo ">>> Rebalance FAILED!"
        exit 1
    fi
    sleep 1
done

echo ">>> Rebalance still in progress after 30 seconds."
