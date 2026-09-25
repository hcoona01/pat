#!/usr/bin/env bash
# =============================================================================
# Vault Dynamic Membership: Add Node
# =============================================================================
set -euo pipefail

NODE_ID="${1:-storage-7}"
URL="${2:-http://storage-7:8001}"
REGION="${3:-us-east-1}"
ZONE="${4:-us-east-1a}"
WEIGHT="${5:-1.0}"
GATEWAY_URL="${6:-http://localhost:8000}"

echo ">>> Registering new node with Vault cluster..."
echo "    Node ID:  ${NODE_ID}"
echo "    URL:      ${URL}"
echo "    Region:   ${REGION}"
echo "    Zone:     ${ZONE}"
echo "    Weight:   ${WEIGHT}"

PAYLOAD=$(cat <<EOF
{
  "node_id": "${NODE_ID}",
  "url": "${URL}",
  "region": "${REGION}",
  "zone": "${ZONE}",
  "active": true,
  "capacity_weight": ${WEIGHT}
}
EOF
)

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${GATEWAY_URL}/v1/admin/nodes" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")

HTTP_BODY=$(echo "${RESPONSE}" | sed '$d')
HTTP_CODE=$(echo "${RESPONSE}" | tail -n1)

if [ "${HTTP_CODE}" -eq 200 ] || [ "${HTTP_CODE}" -eq 201 ]; then
    echo "SUCCESS: Node ${NODE_ID} added successfully."
    echo "${HTTP_BODY}"
else
    echo "FAILED: HTTP status ${HTTP_CODE}"
    echo "${HTTP_BODY}"
    exit 1
fi
