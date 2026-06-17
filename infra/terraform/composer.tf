# Phase 3 — Cloud Composer (Airflow 2.x) orchestration.
#
# Owns: the Composer 3 environment, its dedicated runner SA + IAM, and
# the Secret Manager entry for the AirNow API key. The DAG itself lives
# in dags/ and is uploaded to the env-managed bucket out-of-band
# (Composer creates the DAGs bucket and exposes its name via
# `dag_gcs_prefix` — that's how we wire the upload in scripts/).
#
# Cost discipline (CLAUDE.md §Pitfalls). A small Composer 3 env idles at
# ~$100+/month even with nothing running. This whole file is gated on
# var.composer_enabled; flip it to true in terraform.tfvars only when
# you're ready to start that meter. The env supports paused/unpaused
# DAGs but cannot be "stopped" — you delete it when not in use.
#
# Permission delegation. The Composer SA does NOT get direct write
# scope on silver/gold/BigQuery datasets. Instead it gets
# `roles/iam.serviceAccountUser` on dataproc_runner (to attach it to a
# batch) and `roles/iam.serviceAccountTokenCreator` on dbt_runner (to
# mint tokens for dbt + BigQuery impersonation), and the Airflow
# operators (DataprocCreateBatchOperator, BigQueryCheckOperator with
# impersonation_chain) act-as those SAs. See the per-resource note below.
# This keeps the Phase-2 IAM surface unchanged and means a compromised
# Airflow worker can't bypass the narrow per-zone scopes already in
# compute.tf.

# --- Service account ----------------------------------------------------

resource "google_service_account" "composer_runner" {
  count        = var.composer_enabled ? 1 : 0
  account_id   = "airhealth-composer"
  display_name = "Airhealth Composer runner"
  description  = "Identity used by the Cloud Composer environment that orchestrates the daily AirNow DAG."

  depends_on = [google_project_service.iam]
}

# --- Project-level roles ------------------------------------------------

# Composer 3 worker needs composer.worker on its node SA.
resource "google_project_iam_member" "composer_runner_worker" {
  count   = var.composer_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# Airflow operators read object metadata across all zones for sensor
# checks (GCSObjectExistenceSensor on raw partitions, etc). Scoped to
# objectViewer; writes go through impersonated SAs.
resource "google_storage_bucket_iam_member" "composer_raw_reader" {
  count  = var.composer_enabled ? 1 : 0
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# The daily AirNow DAG (dags/air_pipeline_dag.py, task ingest_airnow) runs
# on the Composer worker and writes one date partition per run to the raw
# zone. objectCreator (not objectAdmin) is the narrowest scope that allows
# it: the ingest skips dates that already exist, so it only ever creates new
# objects — a forced overwrite/backfill is a manual op that can borrow a
# broader role temporarily. This is the one write scope the Composer SA
# holds directly; silver/gold writes still go through impersonated runners.
resource "google_storage_bucket_iam_member" "composer_raw_writer" {
  count  = var.composer_enabled ? 1 : 0
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

resource "google_storage_bucket_iam_member" "composer_silver_reader" {
  count  = var.composer_enabled ? 1 : 0
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

resource "google_storage_bucket_iam_member" "composer_gold_reader" {
  count  = var.composer_enabled ? 1 : 0
  bucket = google_storage_bucket.gold.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# DAGs need to stage Spark job artifacts (zipped airhealth pkg, the
# silver job .py) to the deps bucket before submitting batches.
resource "google_storage_bucket_iam_member" "composer_deps_admin" {
  count  = var.composer_enabled ? 1 : 0
  bucket = google_storage_bucket.dataproc_deps.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# BigQuery jobUser is needed so the operator can issue jobs at all;
# the data-plane scope comes via impersonation of dbt_runner.
resource "google_project_iam_member" "composer_runner_bq_jobuser" {
  count   = var.composer_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# Dataproc editor lets the operator submit batches; the batch itself
# runs under the dataproc_runner SA (set in the operator call), so
# this is just "may create a batch resource" not "may read its data".
resource "google_project_iam_member" "composer_runner_dataproc_editor" {
  count   = var.composer_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# --- Cross-SA impersonation --------------------------------------------
# Two different mechanisms, two different roles — they are NOT
# interchangeable:
#
#   * Dataproc batch → serviceAccountUser. "May attach this SA as the
#     identity of a resource I create" (the batch's
#     execution_config.service_account). A control-plane attach, no token
#     is minted by Composer.
#   * dbt + BigQuery checks → serviceAccountTokenCreator. dbt's
#     impersonate_service_account and the operators' impersonation_chain
#     both call iamcredentials.generateAccessToken to mint a short-lived
#     token for the dbt SA. serviceAccountUser does NOT authorize that —
#     token minting requires serviceAccountTokenCreator.
#
# This is the bridge that lets Airflow run Dataproc/BQ work under the
# existing Phase-2 SAs without inheriting their data-plane scopes itself.

resource "google_service_account_iam_member" "composer_actas_dataproc_runner" {
  count              = var.composer_enabled ? 1 : 0
  service_account_id = google_service_account.dataproc_runner.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

resource "google_service_account_iam_member" "composer_impersonate_dbt_runner" {
  count              = var.composer_enabled ? 1 : 0
  service_account_id = google_service_account.dbt_runner.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# --- Secret Manager: AirNow API key ------------------------------------
# Phase 3 swaps Phase 1's file-dump ingest for the AirNow API
# (CLAUDE.md §Phase 3). Key is provisioned empty here — populate it
# manually with `gcloud secrets versions add` after `terraform apply`,
# so the value never sits in tfstate.

resource "google_secret_manager_secret" "airnow_api_key" {
  count     = var.composer_enabled ? 1 : 0
  secret_id = "airhealth-airnow-api-key"

  replication {
    auto {}
  }

  labels = local.common_labels

  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_iam_member" "composer_airnow_accessor" {
  count     = var.composer_enabled ? 1 : 0
  secret_id = google_secret_manager_secret.airnow_api_key[0].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.composer_runner[0].email}"
}

# --- The Composer environment itself -----------------------------------
#
# Composer 3, single-region, smallest preset by default. The DAG's
# daily AirNow pull + monthly silver-job kickoff doesn't need more.
# Resize via var.composer_environment_size if Phase 4 CONUS scale-up
# pushes scheduling latency.

resource "google_composer_environment" "airhealth" {
  count  = var.composer_enabled ? 1 : 0
  name   = "airhealth-orchestrator"
  region = var.region
  labels = local.common_labels

  config {
    software_config {
      image_version = var.composer_image_version

      # Airflow config overrides go here when needed. Empty for now;
      # the DAG handles its own retry/backoff inside dag definitions.
      airflow_config_overrides = {
        "core-dagbag_import_timeout" = "120"
      }

      # PYTHONPATH for the DAGs bucket already includes /home/airflow/gcs/dags
      # — when we package airhealth as a wheel in Phase 3, drop it in
      # /home/airflow/gcs/plugins and uncomment pypi_packages here.
      # pypi_packages = {}
    }

    node_config {
      service_account = google_service_account.composer_runner[0].email
    }

    environment_size = var.composer_environment_size
  }

  depends_on = [
    google_project_service.composer,
    google_project_iam_member.composer_runner_worker,
  ]
}
