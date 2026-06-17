# Phase 2 compute infrastructure: service accounts + IAM for the
# Dataproc Serverless silver job and for dbt's BigQuery materializer.
#
# No Dataproc batch resource is provisioned here. Per CLAUDE.md §Phase 2,
# batches are submitted ad-hoc via scripts/submit_silver.sh ("manual
# gcloud invocation; no Airflow yet"). Terraform owns the durable
# principals; the submit command lives in scripts/.

# --- APIs ---------------------------------------------------------------

resource "google_project_service" "dataproc" {
  service            = "dataproc.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  # Dataproc Serverless runs on Compute Engine networking; the API has
  # to be enabled even if we use the project's default VPC.
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

# --- Service accounts ---------------------------------------------------

resource "google_service_account" "dataproc_runner" {
  account_id   = "airhealth-dataproc"
  display_name = "Airhealth Dataproc Serverless runner"
  description  = "Identity used by Dataproc Serverless batches that build the silver zone."

  depends_on = [google_project_service.iam]
}

resource "google_service_account" "dbt_runner" {
  account_id   = "airhealth-dbt"
  display_name = "Airhealth dbt runner"
  description  = "Identity used by dbt to materialize gold-zone fact tables in BigQuery."

  depends_on = [google_project_service.iam]
}

# --- Project-level roles ------------------------------------------------

# Dataproc Serverless requires dataproc.worker on the runner SA.
resource "google_project_iam_member" "dataproc_runner_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

# Spark jobs read external tables and write query results; dbt issues
# query/load jobs. Both need jobUser at project level.
resource "google_project_iam_member" "dataproc_runner_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_project_iam_member" "dbt_runner_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt_runner.email}"
}

# Developer impersonation: a human's ADC mints short-lived tokens for the
# dbt SA (profiles.yml: impersonate_service_account), so local `dbt build
# --target bigquery` runs with exactly the SA's gold/silver_ext scope and
# leaves no downloaded key on disk. Members come from var.dbt_impersonators.
resource "google_service_account_iam_member" "dbt_runner_token_creators" {
  for_each           = toset(var.dbt_impersonators)
  service_account_id = google_service_account.dbt_runner.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

# --- Bucket-level roles -------------------------------------------------
# Principle: each SA gets the narrowest scope on each bucket. Versus
# project-level storage roles, this prevents either runner from touching
# the tfstate bucket.

# Dataproc reads raw, reads+writes silver, reads gold (for joins).
resource "google_storage_bucket_iam_member" "dataproc_raw_reader" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_silver_admin" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_gold_reader" {
  bucket = google_storage_bucket.gold.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

# Dataproc deps bucket: SA reads its own --py-files / --files uploads
# (the submit script does the upload via the operator's principal).
resource "google_storage_bucket_iam_member" "dataproc_deps_reader" {
  bucket = google_storage_bucket.dataproc_deps.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

# dbt reads silver (via external tables), writes gold.
resource "google_storage_bucket_iam_member" "dbt_silver_reader" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dbt_runner.email}"
}

resource "google_storage_bucket_iam_member" "dbt_gold_admin" {
  bucket = google_storage_bucket.gold.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dbt_runner.email}"
}

# --- Dataset-level roles ------------------------------------------------

# Dataproc writes external-table metadata when it lands a new silver
# partition (the BQ DDL is run from the same submit script).
resource "google_bigquery_dataset_iam_member" "dataproc_silver_ext_editor" {
  dataset_id = google_bigquery_dataset.silver_ext.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataproc_runner.email}"
}

# dbt reads silver_ext, writes gold.
resource "google_bigquery_dataset_iam_member" "dbt_silver_ext_reader" {
  dataset_id = google_bigquery_dataset.silver_ext.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dbt_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_gold_editor" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt_runner.email}"
}
