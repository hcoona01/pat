#!/usr/bin/env bash
# =============================================================================
# Vault Test Suite Runner: Unit, Integration, Chaos, and Workload Tests
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "======================================================================="
echo "               VAULT: RUNNING COMPLETE AUTOMATED TEST SUITE            "
echo "======================================================================="
echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Root Dir:  ${ROOT_DIR}"
echo ""

# Run pytest across tests/
python -m pytest tests/ -v "$@"
TEST_EXIT_CODE=$?

echo ""
if [ ${TEST_EXIT_CODE} -eq 0 ]; then
    echo "======================================================================="
    echo "                      ALL TESTS PASSED SUCCESSFULLY                    "
    echo "======================================================================="
else
    echo "======================================================================="
    echo "                      TEST RUN FAILED (CODE ${TEST_EXIT_CODE})                 "
    echo "======================================================================="
fi

exit ${TEST_EXIT_CODE}
