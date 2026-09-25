#!/usr/bin/env bash
# =============================================================================
# Vault: Stop Cluster
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "======================================================================="
echo "                         STOPPING VAULT CLUSTER                        "
echo "======================================================================="

# Stop Docker Compose if running
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo ">>> Stopping Docker containers..."
    docker compose down --volumes --remove-orphans 2>/dev/null || true
fi

# Stop any local cluster PIDs
if [ -f .cluster_pids ]; then
    echo ">>> Stopping local cluster processes recorded in .cluster_pids..."
    while IFS= read -r pid || [ -n "$pid" ]; do
        if [ -n "$pid" ]; then
            echo "Terminating PID ${pid}..."
            kill "${pid}" 2>/dev/null || true
        fi
    done < .cluster_pids
    rm -f .cluster_pids
fi

echo ">>> Vault cluster stopped."
