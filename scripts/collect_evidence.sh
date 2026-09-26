#!/usr/bin/env bash
# Collect only evidence obtained from a live Docker Compose cluster and passing tests.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

command -v docker >/dev/null
command -v curl >/dev/null
docker info >/dev/null

STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
EVIDENCE_DIR="${ROOT_DIR}/docs/generated-evidence/${STAMP}"
mkdir -p "${EVIDENCE_DIR}"

# Fail if the gateway is not a live service. Evidence is never synthesized.
curl --fail --silent --show-error http://localhost:8000/v1/cluster/health > "${EVIDENCE_DIR}/gateway_cluster_health.json"
curl --fail --silent --show-error http://localhost:8000/metrics > "${EVIDENCE_DIR}/gateway_metrics.prom"
docker compose ps --format json > "${EVIDENCE_DIR}/docker_compose_ps.json"
docker version > "${EVIDENCE_DIR}/docker_version.txt"
python --version > "${EVIDENCE_DIR}/python_version.txt"
git rev-parse HEAD > "${EVIDENCE_DIR}/git_revision.txt"

# Tests must pass; their real output is retained as evidence.
python -m pytest tests/e2e_docker -v | tee "${EVIDENCE_DIR}/docker_e2e_tests.log"

python - "${EVIDENCE_DIR}" <<'PY'
import hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path

target = Path(sys.argv[1])
files = []
for path in sorted(target.iterdir()):
    if path.name == "manifest.json" or not path.is_file():
        continue
    files.append({
        "name": path.name,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "bytes": path.stat().st_size,
    })
(target / "manifest.json").write_text(json.dumps({
    "created_at": datetime.now(timezone.utc).isoformat(),
    "source": "live Docker Compose cluster and Docker E2E test run",
    "files": files,
}, indent=2), encoding="utf-8")
PY

rm -f "${ROOT_DIR}/docs/generated-evidence/latest"
ln -s "${STAMP}" "${ROOT_DIR}/docs/generated-evidence/latest"
echo "Evidence collected in ${EVIDENCE_DIR}"
