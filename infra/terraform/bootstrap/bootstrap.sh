#!/usr/bin/env bash
# Create the Terraform remote-state bucket and emit backend.hcl.
# Idempotent: safe to re-run. The only thing that lives outside Terraform
# (terraform's gcs backend can't bootstrap the bucket holding its own state).
#
# Usage:
#   PROJECT_ID=my-gcp-project ./bootstrap.sh
#   PROJECT_ID=my-gcp-project LOCATION=us-east1 PREFIX=custom ./bootstrap.sh

set -euo pipefail

: "${PROJECT_ID:?PROJECT_ID env var is required}"
PREFIX="${PREFIX:-${PROJECT_ID}-airhealth}"
LOCATION="${LOCATION:-us-central1}"
STATE_BUCKET="${PREFIX}-tfstate"

echo ">> project:  ${PROJECT_ID}"
echo ">> bucket:   gs://${STATE_BUCKET}"
echo ">> location: ${LOCATION}"

gcloud config set project "${PROJECT_ID}" >/dev/null

echo ">> enabling required APIs"
gcloud services enable \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project="${PROJECT_ID}" >/dev/null

if gcloud storage buckets describe "gs://${STATE_BUCKET}" \
    --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo ">> bucket already exists, skipping create"
else
  echo ">> creating state bucket"
  gcloud storage buckets create "gs://${STATE_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}" \
    --uniform-bucket-level-access \
    --public-access-prevention \
    --default-storage-class=STANDARD
fi

echo ">> enforcing versioning + labels (idempotent)"
gcloud storage buckets update "gs://${STATE_BUCKET}" --versioning >/dev/null
gcloud storage buckets update "gs://${STATE_BUCKET}" \
  --update-labels=project=airhealth,zone=state,managed_by=bootstrap >/dev/null

# Write backend config consumed by `terraform init -backend-config=backend.hcl`
BACKEND_FILE="$(cd "$(dirname "$0")/.." && pwd)/backend.hcl"
cat > "${BACKEND_FILE}" <<EOF
bucket = "${STATE_BUCKET}"
prefix = "airhealth/storage"
EOF

echo
echo "Bootstrap complete."
echo "  state bucket: gs://${STATE_BUCKET}"
echo "  backend cfg:  ${BACKEND_FILE}"
echo
echo "Next:"
echo "  cd $(dirname "${BACKEND_FILE}")"
echo "  terraform init -backend-config=backend.hcl"
echo "  # Import the manually-created raw bucket before apply:"
echo "  terraform import google_storage_bucket.raw ${PREFIX}-raw"
echo "  terraform plan   # should show 0 changes for the raw bucket"
echo "  terraform apply"
