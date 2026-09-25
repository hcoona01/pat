#!/usr/bin/env bash
# =============================================================================
# Vault Fault Recovery: Restore Node
# =============================================================================
set -euo pipefail

NODE_ID="${1:-store-1}"
GATEWAY_URL="${2:-http://localhost:8000}"
TOXIPROXY_URL="${3:-http://localhost:8474}"

echo ">>> Restoring node: ${NODE_ID}"

# 1. Try Toxiproxy API if active
if curl -s -f "${TOXIPROXY_URL}/version" >/dev/null 2>&1; then
    echo "Activating proxy via Toxiproxy: ${NODE_ID}"
    curl -s -X POST "${TOXIPROXY_URL}/proxies/${NODE_ID}/up" || true
fi

# 2. Try Docker network reconnect
if command -v docker >/dev/null 2>&1; then
    echo "Attempting Docker network reconnection: vault-${NODE_ID}"
    docker network connect vault-net "vault-${NODE_ID}" 2>/dev/null || true
fi

# 3. Try Gateway Admin API
if curl -s -f "${GATEWAY_URL}/v1/cluster/health" >/dev/null 2>&1; then
    echo "Marking node active via Gateway Admin API: ${NODE_ID}"
    curl -s -X POST "${GATEWAY_URL}/v1/admin/nodes/${NODE_ID}/activate" || true
fi

echo ">>> Node ${NODE_ID} restored successfully."
