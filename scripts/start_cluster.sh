#!/usr/bin/env bash
# =============================================================================
# Vault: Start Cluster
# Starts full multi-node cluster via Docker Compose if available, or local
# Python processes if running standalone.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "======================================================================="
echo "                         STARTING VAULT CLUSTER                        "
echo "======================================================================="

# Check if Docker is available
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo ">>> Starting cluster via Docker Compose..."
    docker compose up -d
    echo ">>> Waiting for Gateway health check at http://localhost:8000/v1/cluster/health..."
    for i in $(seq 1 30); do
        if curl -s -f http://localhost:8000/v1/cluster/health >/dev/null 2>&1; then
            echo ">>> Vault Docker cluster is ready!"
            curl -s http://localhost:8000/v1/cluster/health
            echo ""
            exit 0
        fi
        sleep 1
    done
    echo "Warning: Cluster started but health check timed out."
    exit 0
fi

echo ">>> Docker daemon not detected. Starting local standalone multi-node cluster..."
mkdir -p .cluster_data/meta-1 .cluster_data/meta-2 .cluster_data/meta-3
mkdir -p .cluster_data/storage-1 .cluster_data/storage-2 .cluster_data/storage-3
mkdir -p .cluster_data/storage-4 .cluster_data/storage-5 .cluster_data/storage-6

rm -f .cluster_pids

echo ">>> Starting 6 local storage nodes..."
for i in $(seq 1 6); do
    PORT=$((8000 + i))
    NODE_ID="storage-${i}"
    python -m uvicorn apps.storage_node.main:app --host 127.0.0.1 --port ${PORT} \
        --app-dir "${ROOT_DIR}" > ".cluster_data/${NODE_ID}.log" 2>&1 &
    PID=$!
    echo "${PID}" >> .cluster_pids
done

echo ">>> Starting 3 Raft metadata nodes..."
python -m uvicorn apps.metadata_node.main:app --host 127.0.0.1 --port 9001 \
    --app-dir "${ROOT_DIR}" > ".cluster_data/meta-1.log" 2>&1 &
echo "$!" >> .cluster_pids

python -m uvicorn apps.metadata_node.main:app --host 127.0.0.1 --port 9011 \
    --app-dir "${ROOT_DIR}" > ".cluster_data/meta-2.log" 2>&1 &
echo "$!" >> .cluster_pids

python -m uvicorn apps.metadata_node.main:app --host 127.0.0.1 --port 9021 \
    --app-dir "${ROOT_DIR}" > ".cluster_data/meta-3.log" 2>&1 &
echo "$!" >> .cluster_pids

echo ">>> Starting Vault API Gateway..."
python -m uvicorn apps.gateway.main:app --host 127.0.0.1 --port 8000 \
    --app-dir "${ROOT_DIR}" > ".cluster_data/gateway.log" 2>&1 &
echo "$!" >> .cluster_pids

echo ">>> Vault local processes started (PIDs recorded in .cluster_pids)."
echo ">>> Checking Gateway health..."
sleep 2
if curl -s -f http://127.0.0.1:8000/v1/cluster/health >/dev/null 2>&1; then
    echo ">>> Vault Gateway is LIVE!"
    curl -s http://127.0.0.1:8000/v1/cluster/health
    echo ""
else
    echo ">>> Gateway launched; listening on http://127.0.0.1:8000"
fi
