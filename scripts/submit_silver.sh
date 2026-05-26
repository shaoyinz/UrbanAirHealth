#!/usr/bin/env bash
# Submit the Sedona silver-zone build to Dataproc Serverless.
#
# Phase 2: manual gcloud invocation (Airflow comes in Phase 3). The
# upstream raw zone must already be populated by:
#   python -m airhealth.ingest.airnow   --project-id "${PROJECT_ID}"
#   python -m airhealth.ingest.overture --project-id "${PROJECT_ID}"
#
# Usage:
#   PROJECT_ID=urban-air-health-syz scripts/submit_silver.sh [--limit N] [--partitions P]
#
# Defaults run the full LA-basin silver build (per `config/release.yaml`
# AOI); pass --limit for a smoke. EXTRA_ARGS are forwarded verbatim to
# `python -m airhealth.spark.build_silver` inside the batch, so any
# arg that CLI takes also works here.
#
# The script:
#   1. Packages dist/airhealth.zip from src/airhealth/.
#   2. Bundles the apache-sedona python wrapper as dist/sedona.zip
#      (Dataproc Serverless 2.2 ships the Sedona JARs separately via
#      spark.jars.packages, but not the python wrapper).
#   3. Submits an async pyspark batch named airhealth-silver-<ts>.
#   4. Tails the batch state until SUCCEEDED / FAILED.

set -euo pipefail

: "${PROJECT_ID:?PROJECT_ID env var is required}"

REGION="${REGION:-us-central1}"
SA="${DATAPROC_SA:-airhealth-dataproc@${PROJECT_ID}.iam.gserviceaccount.com}"
# Deps bucket created by infra/terraform/buckets.tf; override only if
# the operator has customised bucket_prefix.
DEPS_BUCKET="${DEPS_BUCKET:-${PROJECT_ID}-airhealth-dataproc-deps}"
SEDONA_VERSION="${SEDONA_VERSION:-1.6.1}"
RUNTIME_VERSION="${RUNTIME_VERSION:-2.2}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

EXTRA_ARGS=("$@")

# --- 1. Package airhealth ----------------------------------------------
mkdir -p dist
rm -f dist/airhealth.zip
( cd src && zip -qr ../dist/airhealth.zip airhealth -x '*/__pycache__/*' '*.pyc' )

# --- 2. Bundle apache-sedona python wrapper ----------------------------
# apache-sedona ships as an sdist on PyPI; we extract just the sedona/
# package and zip it. shapely is assumed pre-installed on the Dataproc
# runtime; rasterio is imported only by sedona.raster, which we don't
# use — and airhealth.spark.session shims it out anyway.
if [[ ! -f dist/sedona.zip ]]; then
  mkdir -p dist/wheels
  if [[ ! -f "dist/wheels/apache_sedona-${SEDONA_VERSION}.tar.gz" ]]; then
    .venv/bin/python -m pip download "apache-sedona==${SEDONA_VERSION}" \
      --no-deps -d dist/wheels >/dev/null
  fi
  tar -xzf "dist/wheels/apache_sedona-${SEDONA_VERSION}.tar.gz" \
    -C dist/wheels
  ( cd "dist/wheels/apache_sedona-${SEDONA_VERSION}" && \
    zip -qr "${REPO_ROOT}/dist/sedona.zip" sedona )
fi

# --- 3. Submit ---------------------------------------------------------
BATCH_ID="airhealth-silver-$(date -u +%Y%m%d-%H%M%S)"

# Use '^|^' as the gcloud properties delimiter so commas inside
# spark.jars.packages don't split into separate property entries.
# Scala 2.13 build — Dataproc Serverless 2.2 ships Spark 3.5 on Scala
# 2.13. Using the _2.12 artifact will surface as ClassNotFoundException
# scala.Serializable at SedonaContext.create on the cluster (we *also*
# use 2.13 in airhealth.spark.session for the local-packages path; that
# code detects pyspark's bundled Scala version and may pick 2.12 on a
# laptop, which is correct for the local pip-installed pyspark).
PROPS="^|^spark.jars.packages=org.apache.sedona:sedona-spark-shaded-3.5_2.13:${SEDONA_VERSION},org.datasyslab:geotools-wrapper:${SEDONA_VERSION}-28.2"
PROPS+="|spark.serializer=org.apache.spark.serializer.KryoSerializer"
PROPS+="|spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator"
PROPS+="|spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
# Keep autoBroadcast at the Spark default (10 MB). The airhealth silver
# job's only "small" side is the per-task closure carrying ~hundreds of
# monitor stats — already tiny — so Catalyst doesn't need a higher
# threshold, and a 10 MB cap protects against future state-scale AirNow
# tables that would otherwise quietly inflate. Cf. the floodpipe
# project's experience broadcasting an FL NFHL zone table at ~4 GiB
# (which is exactly the failure mode we're guarding against here).
PROPS+="|spark.sql.autoBroadcastJoinThreshold=10485760"
# Executor sizing: 4 cores × 16g heap × 4g overhead per executor on the
# standard compute tier. The headline cost is the (N × M) haversine
# matrix in airhealth.spark.build_silver._haversine_km_to_all — at
# state-scale partitions (~10k rows × ~200 monitors) that's ~16 MB per
# partition, so 16g heap leaves a comfortable margin for the rest of
# the per-task Python state. Bump executor.memory if a CONUS partition
# (~50k rows × ~2000 monitors → ~800 MB matrix) starts pressuring this.
PROPS+="|spark.executor.cores=4"
PROPS+="|spark.executor.memory=16g"
PROPS+="|spark.executor.memoryOverhead=4g"
# Fail fast on deterministic per-partition errors. Dataproc's default
# of 4 retries can burn an hour reproducing the same NumPy error before
# the driver log shows it.
PROPS+="|spark.task.maxFailures=2"

echo "submitting batch ${BATCH_ID}"
gcloud dataproc batches submit pyspark \
  src/airhealth/spark/build_silver.py \
  --batch="${BATCH_ID}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --version="${RUNTIME_VERSION}" \
  --service-account="${SA}" \
  --deps-bucket="${DEPS_BUCKET}" \
  --py-files=dist/airhealth.zip,dist/sedona.zip \
  --files=config/release.yaml,config/concentration_response.yaml \
  --properties="${PROPS}" \
  --ttl=21600s \
  --async \
  -- --project-id="${PROJECT_ID}" \
     --release-config=release.yaml \
     --cr-config=concentration_response.yaml \
     ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}

# --- 4. Tail state -----------------------------------------------------
echo "polling batch state (Ctrl-C is safe; the batch keeps running)"
prev=""
while true; do
  state=$(gcloud dataproc batches describe "${BATCH_ID}" \
    --project="${PROJECT_ID}" --region="${REGION}" \
    --format="value(state)" 2>/dev/null || echo "DESCRIBE_FAILED")
  if [[ "$state" != "$prev" ]]; then
    echo "  state=${state}"
    prev="$state"
  fi
  case "$state" in
    SUCCEEDED) exit 0 ;;
    FAILED|CANCELLED) exit 1 ;;
  esac
  sleep 20
done
