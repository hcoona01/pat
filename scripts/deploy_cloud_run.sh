#!/usr/bin/env bash
set -euo pipefail

: "${GOOGLE_CLOUD_PROJECT:?Set GOOGLE_CLOUD_PROJECT}"
: "${GOOGLE_CLOUD_REGION:?Set GOOGLE_CLOUD_REGION}"
command -v gcloud >/dev/null
gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q .

GIT_SHA="$(git rev-parse --short HEAD)"
IMAGE="gcr.io/${GOOGLE_CLOUD_PROJECT}/vault-demo:${GIT_SHA}"

gcloud builds submit \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --config cloudbuild.cloudrun.yaml \
  --substitutions "_IMAGE=${IMAGE}" \
  --quiet

gcloud run deploy vault-demo \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated \
  --set-env-vars "VAULT_MODE=cloud_demo,GIT_SHA=${GIT_SHA}" \
  --quiet
