"""Daily AirNow → monthly silver exposure roll-up → DALY mart refresh.

Phase 3 orchestration (CLAUDE.md §Phase 3). One daily-scheduled DAG::

    ingest_airnow ({{ ds }})           ── every day, lands one date partition
          │
          ▼
    wait_for_raw  (GCS existence sensor on the date partition)
          │
          ▼
    gate_month_end?  ──false──▶ short-circuit (daily pulls just accumulate)
          │ true (last calendar day of the month)
          ▼
    build_silver  (Dataproc Serverless batch, runs as airhealth-dataproc)
          │
          ▼
    dbt_run ▶ dbt_test  (BigQuery, runs as airhealth-dbt via impersonation)
          │
          ▼
    dq_fct_nonempty ▶ dq_daly_not_null  (BigQuery checks, as airhealth-dbt)

Idempotency (CLAUDE.md §Airflow): keyed on ``{{ ds }}`` for the daily
AirNow partition and on the pinned AirNow *window* string
(``AIRHEALTH_AIRNOW_WINDOW``) for the GCS path namespace. Re-running a
date overwrites only that date's partition; re-running a month rebuilds
the silver zone and the mart deterministically from raw. The Dataproc
batch *record* gets a per-attempt id (the silver output is idempotent by
GCS path, so a duplicate batch record is harmless).

Permission model (infra/terraform/composer.tf). This DAG runs as the
``airhealth-composer`` SA, which has NO data-plane write scope of its own.
Every write acts-as a Phase-2 runner SA:

  * Dataproc silver batch → runs as ``airhealth-dataproc`` (set as the
    batch's ``execution_config.service_account``; the Composer SA holds
    ``serviceAccountUser`` on it + ``dataproc.editor`` to create batches).
  * dbt + BigQuery checks → ``airhealth-dbt`` via ``impersonation_chain``
    (the Composer SA holds ``serviceAccountUser`` on it + ``bigquery.jobUser``).

==============================  PREREQUISITES  ==============================
This DAG is the orchestration *shape*. Three pieces must land alongside it
before a real scheduled run succeeds end-to-end; each is flagged inline at
the task that needs it.

  [P1] Raw-zone writer. composer.tf grants ``airhealth-composer`` only
       ``objectViewer`` on the raw bucket, but ingest_airnow writes there.
       Add the ``composer_raw_writer`` grant (objectCreator) — see the
       companion change in infra/terraform/composer.tf.
  [P2] ``airhealth`` importable on the Composer image (pypi_packages or a
       wheel under /home/airflow/gcs/plugins) AND a single-date AirNow
       *API* entrypoint. The Phase-1 CLI (airhealth.ingest.airnow) pulls a
       whole window from the public file dump; Phase 3 wants one {{ ds }}
       via the keyed API (airnow.py docstring §2). See ingest_airnow.
  [P3] dbt-bigquery reachable from the worker (pypi_packages or a
       KubernetesPodOperator image), the dbt project synced to
       AIRHEALTH_DBT_PROJECT_DIR, and the profiles.yml `bigquery` target
       extended with ``impersonate_service_account: <airhealth-dbt SA>``.

Everything is gated off until ``composer_enabled = true`` in
terraform.tfvars — the env doesn't exist yet, so this file only has to
parse cleanly in the dagbag (no airhealth/dbt import at module top level).
============================================================================
"""

from __future__ import annotations

import datetime
import json
import os

import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# --- Config (parse-safe: env vars set via Composer software_config, never
# top-level Variable.get which hits the metadata DB on every dagbag parse).
PROJECT_ID = os.environ.get("AIRHEALTH_PROJECT_ID", "urban-air-health-syz")
REGION = os.environ.get("AIRHEALTH_REGION", "us-central1")
# Mirrors config/release.yaml: airnow.window and aoi.name.
AIRNOW_WINDOW = os.environ.get("AIRHEALTH_AIRNOW_WINDOW", "20250101-20251231")
AOI_NAME = os.environ.get("AIRHEALTH_AOI_NAME", "la_basin")
SEDONA_VERSION = os.environ.get("AIRHEALTH_SEDONA_VERSION", "1.6.1")
# Force the monthly roll-up branch regardless of calendar date (for a
# manual backfill / smoke run). Off by default.
FORCE_ROLLUP = os.environ.get("AIRHEALTH_FORCE_ROLLUP", "").lower() in ("1", "true", "yes")

# Bucket / dataset names follow the Terraform defaults
# (bucket_prefix = "{project}-airhealth", bq_dataset_prefix = "airhealth").
RAW_BUCKET = f"{PROJECT_ID}-airhealth-raw"
DEPS = f"gs://{PROJECT_ID}-airhealth-dataproc-deps"
GOLD_DATASET = "airhealth_gold"
FCT_TABLE = f"{PROJECT_ID}.{GOLD_DATASET}.fct_building_dalys"

# Runner SAs the operators act-as (see infra/terraform/compute.tf).
DATAPROC_SA = f"airhealth-dataproc@{PROJECT_ID}.iam.gserviceaccount.com"
DBT_SA = f"airhealth-dbt@{PROJECT_ID}.iam.gserviceaccount.com"

# Secret Manager entry holding the AirNow API key (composer.tf provisions
# it; the Composer SA has secretAccessor).
AIRNOW_SECRET_ID = "airhealth-airnow-api-key"

# dbt project root as synced onto the worker (see [P3]).
DBT_PROJECT_DIR = os.environ.get("AIRHEALTH_DBT_PROJECT_DIR", "/home/airflow/gcs/dags/dbt")

# --- Silver-zone Dataproc Serverless batch ------------------------------
# Mirrors scripts/submit_silver.sh verbatim (Scala 2.13 Sedona shaded jar,
# Kryo + Sedona SQL extensions, 4c/16g executors, fail-fast maxFailures=2).
# Spark artifacts are pre-staged under {DEPS}/spark/ by a deploy step that
# reuses submit_silver.sh's packaging (zip airhealth, bundle the sedona
# python wrapper). The Composer SA has objectAdmin on the deps bucket to
# refresh them; the batch reads them as airhealth-dataproc.
_SILVER_PROPERTIES = {
    "spark.jars.packages": (
        f"org.apache.sedona:sedona-spark-shaded-3.5_2.13:{SEDONA_VERSION},"
        f"org.datasyslab:geotools-wrapper:{SEDONA_VERSION}-28.2"
    ),
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
    "spark.sql.extensions": (
        "org.apache.sedona.viz.sql.SedonaVizExtensions,"
        "org.apache.sedona.sql.SedonaSqlExtensions"
    ),
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
    "spark.executor.cores": "4",
    "spark.executor.memory": "16g",
    "spark.executor.memoryOverhead": "4g",
    "spark.task.maxFailures": "2",
}

SILVER_BATCH = {
    "pyspark_batch": {
        "main_python_file_uri": f"{DEPS}/spark/build_silver.py",
        "python_file_uris": [
            f"{DEPS}/spark/airhealth.zip",
            f"{DEPS}/spark/sedona.zip",
        ],
        "file_uris": [
            f"{DEPS}/spark/release.yaml",
            f"{DEPS}/spark/concentration_response.yaml",
        ],
        "args": [
            f"--project-id={PROJECT_ID}",
            "--release-config=release.yaml",
            "--cr-config=concentration_response.yaml",
        ],
    },
    "runtime_config": {
        "version": "2.2",
        "properties": _SILVER_PROPERTIES,
    },
    "environment_config": {
        "execution_config": {
            # The batch runs as the Phase-2 dataproc runner, which owns the
            # silver-zone write scope. The Composer SA only gets to *create*
            # the batch and act-as this SA — never to touch silver directly.
            "service_account": DATAPROC_SA,
            "ttl": "21600s",
        }
    },
}

default_args = {
    "owner": "airhealth",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "depends_on_past": False,
}


@dag(
    dag_id="air_pipeline",
    description="Daily AirNow pull → monthly silver roll-up → DALY mart refresh.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["airhealth", "phase3", "pm25", "dalys"],
)
def air_pipeline():
    # --- 1. Daily AirNow pull -------------------------------------------
    @task
    def ingest_airnow(ds: str | None = None) -> str:
        """[P2] Pull one day of AirNow PM2.5 for ``{{ ds }}`` into raw.

        Imported in-task (not at module top) so the dagbag parses without
        the airhealth wheel on the image. Requires a single-date *API*
        entrypoint on airhealth.ingest.airnow — the Phase-1 module only
        does whole-window file-dump pulls today (see [P2]).
        """
        from airhealth.ingest import airnow  # noqa: PLC0415  (deferred — see [P2])

        api_key = _airnow_api_key()
        # NOTE: `ingest_api_day` is the per-date API entrypoint to be added
        # in airhealth.ingest.airnow ([P2]); it should write to the same
        # raw path the Phase-1 file-dump path uses:
        #   airnow/window=<W>/aoi=<AOI>/date=<YYYYMMDD>/observations.parquet
        target = airnow.ingest_api_day(
            project_id=PROJECT_ID,
            window=AIRNOW_WINDOW,
            day=ds,
            api_key=api_key,
        )
        return target

    # --- 2. Confirm the raw partition landed ----------------------------
    wait_for_raw = GCSObjectExistenceSensor(
        task_id="wait_for_raw",
        bucket=RAW_BUCKET,
        object=(
            f"airnow/window={AIRNOW_WINDOW}/aoi={AOI_NAME}/"
            "date={{ ds_nodash }}/observations.parquet"
        ),
        timeout=60 * 30,
        poke_interval=60,
        mode="reschedule",  # free the worker slot between pokes
    )

    # --- 3. Monthly gate ------------------------------------------------
    @task.short_circuit
    def gate_month_end(ds: str) -> bool:
        """Proceed to the silver roll-up only on the last day of the month.

        Daily AirNow pulls accumulate in raw all month; the expensive
        Sedona IDW + DALY recompute fires once per month (CLAUDE.md §Phase 3
        "daily AirNow pull → monthly exposure roll-up"). ``FORCE_ROLLUP``
        overrides for a manual backfill.
        """
        if FORCE_ROLLUP:
            return True
        d = datetime.date.fromisoformat(ds)
        return (d + datetime.timedelta(days=1)).month != d.month

    # --- 4. Silver-zone build (Dataproc Serverless) ---------------------
    build_silver = DataprocCreateBatchOperator(
        task_id="build_silver",
        project_id=PROJECT_ID,
        region=REGION,
        # Per-attempt unique id; silver output is idempotent by GCS path so a
        # duplicate batch record across retries is harmless. Batch ids must
        # match [a-z0-9-] and be <= 63 chars.
        batch_id="airhealth-silver-{{ ds_nodash }}-{{ ts_nodash }}",
        batch=SILVER_BATCH,
        # The Composer SA creates the batch directly (it holds dataproc.editor
        # + serviceAccountUser on DATAPROC_SA, which is what lets it set the
        # execution_config.service_account above). No impersonation_chain
        # needed on the operator's own control-plane calls.
    )

    # --- 5. dbt: rebuild the gold-zone DALY mart ------------------------
    # [P3] Runs dbt against BigQuery as airhealth-dbt. The worker's ambient
    # identity (airhealth-composer) impersonates airhealth-dbt via the
    # bigquery target's `impersonate_service_account`. Needs dbt-bigquery on
    # the image and the dbt project synced to DBT_PROJECT_DIR.
    _dbt_env = {
        "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
        "GCP_PROJECT_ID": PROJECT_ID,
        "DBT_DALY_RUNNER_SA": DBT_SA,  # consumed by profiles.yml impersonate_service_account
    }

    @task.bash(env=_dbt_env, append_env=True)
    def dbt_run() -> str:
        return (
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --target bigquery --no-use-colors "
            "--profiles-dir ."
        )

    @task.bash(env=_dbt_env, append_env=True)
    def dbt_test() -> str:
        return (
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --target bigquery --no-use-colors "
            "--profiles-dir ."
        )

    # --- 6. Data-quality gates on the mart ------------------------------
    # These read airhealth_gold; the Composer SA has no gold dataset role,
    # so it impersonates airhealth-dbt (dataViewer/Editor on gold) for the
    # BigQuery jobs. This is the one branch that works with current IAM as-is.
    dq_fct_nonempty = BigQueryCheckOperator(
        task_id="dq_fct_nonempty",
        sql=f"SELECT COUNT(*) > 0 FROM `{FCT_TABLE}`",
        use_legacy_sql=False,
        location="US",
        impersonation_chain=DBT_SA,
    )

    dq_daly_not_null = BigQueryCheckOperator(
        task_id="dq_daly_not_null",
        # Every building must have a finite, non-negative total DALY burden.
        sql=(
            "SELECT COUNTIF(daly_total IS NULL OR daly_total < 0) = 0 "
            f"FROM `{FCT_TABLE}`"
        ),
        use_legacy_sql=False,
        location="US",
        impersonation_chain=DBT_SA,
    )

    # --- Wiring ---------------------------------------------------------
    raw_target = ingest_airnow()
    rollup = gate_month_end()
    run_dbt = dbt_run()
    test_dbt = dbt_test()

    raw_target >> wait_for_raw >> rollup >> build_silver >> run_dbt >> test_dbt
    test_dbt >> dq_fct_nonempty >> dq_daly_not_null


def _airnow_api_key() -> str:
    """Fetch the AirNow API key from Secret Manager (Composer SA has
    secretAccessor on AIRNOW_SECRET_ID). In-task import keeps the hook off
    the module-parse path."""
    from airflow.providers.google.cloud.hooks.secret_manager import (  # noqa: PLC0415
        GoogleCloudSecretManagerHook,
    )

    hook = GoogleCloudSecretManagerHook()
    return hook.access_secret(
        project_id=PROJECT_ID, secret_id=AIRNOW_SECRET_ID, secret_version="latest"
    ).payload.data.decode("utf-8")


dag_obj = air_pipeline()
