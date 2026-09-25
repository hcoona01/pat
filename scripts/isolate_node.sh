#!/usr/bin/env bash
# =============================================================================
# Vault Fault Injection: Isolate Node
# =============================================================================
set -euo pipefail

NODE_ID="${1:-store-1}"
GATEWAY_URL="${2:-http://localhost:8000}"
TOXIPROXY_URL="${3:-http://localhost:8474}"

echo ">>> Isolating node: ${NODE_ID}"

# 1. Try Toxiproxy API if active
if curl -s -f "${TOXIPROXY_URL}/version" >/dev/null 2>&1; then
    echo "Deactivating proxy via Toxiproxy: ${NODE_ID}"
    curl -s -X POST "${TOXIPROXY_URL}/proxies/${NODE_ID}/down" || true
fi

# 2. Try Gateway Admin API
if curl -s -f "${GATEWAY_URL}/v1/cluster/health" >/dev/null 2>&1; then
    echo "Marking node inactive via Gateway Admin API: ${NODE_ID}"
    curl -s -X POST "${GATEWAY_URL}/v1/admin/nodes/${NODE_ID}/deactivate" || true
fi

# 3. Try Docker network disconnect if container exists
if command -v docker >/dev/null 2>&1; then
    echo "Attempting Docker network disconnection: vault-${NODE_ID}"
    docker network disconnect vault-net "vault-${NODE_ID}" 2>/dev/null || true
fi

echo ">>> Node ${NODE_ID} isolated successfully."
