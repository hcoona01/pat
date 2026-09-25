#!/usr/bin/env bash
# =============================================================================
# Vault: Interactive Reproducible Demonstration
# Demonstrates:
#   1. Upload across hot, durable, archive policies
#   2. Concurrent reads & writes
#   3. Storage node failure
#   4. Continued safe reads
#   5. Corruption detection
#   6. Automatic repair
#   7. Node addition
#   8. Rebalance
#   9. Final hash and replica/fragment verification
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

python scripts/demo_runner.py "$@"
