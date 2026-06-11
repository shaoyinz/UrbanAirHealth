output "raw_bucket" {
  description = "gs:// URL of the raw zone bucket."
  value       = google_storage_bucket.raw.url
}

output "raw_bucket_name" {
  description = "Bare name of the raw zone bucket (use with `terraform import`)."
  value       = google_storage_bucket.raw.name
}

output "silver_bucket" {
  description = "gs:// URL of the silver zone bucket."
  value       = google_storage_bucket.silver.url
}

output "gold_bucket" {
  description = "gs:// URL of the gold zone bucket."
  value       = google_storage_bucket.gold.url
}

output "dataproc_deps_bucket" {
  description = "gs:// URL of the Dataproc Serverless deps staging bucket."
  value       = google_storage_bucket.dataproc_deps.url
}

output "bq_silver_ext_dataset" {
  description = "Fully-qualified ID of the silver external-tables dataset."
  value       = "${var.project_id}.${google_bigquery_dataset.silver_ext.dataset_id}"
}

output "bq_gold_dataset" {
  description = "Fully-qualified ID of the gold (dbt outputs) dataset."
  value       = "${var.project_id}.${google_bigquery_dataset.gold.dataset_id}"
}

output "dataproc_runner_sa" {
  description = "Email of the service account that runs Dataproc Serverless batches."
  value       = google_service_account.dataproc_runner.email
}

output "dbt_runner_sa" {
  description = "Email of the service account that runs dbt against BigQuery."
  value       = google_service_account.dbt_runner.email
}

# --- Phase 3 / Composer (null when var.composer_enabled = false) -------

output "composer_runner_sa" {
  description = "Email of the service account attached to the Composer environment. Null when Composer is disabled."
  value       = var.composer_enabled ? google_service_account.composer_runner[0].email : null
}

output "composer_dag_gcs_prefix" {
  description = "gs:// URL of the Composer-managed DAGs folder. Upload DAGs here with `gcloud composer environments storage dags import` or a plain gsutil cp. Null when Composer is disabled."
  value       = var.composer_enabled ? google_composer_environment.airhealth[0].config[0].dag_gcs_prefix : null
}

output "composer_airflow_uri" {
  description = "URL of the Airflow web UI for the Composer environment. Null when Composer is disabled."
  value       = var.composer_enabled ? google_composer_environment.airhealth[0].config[0].airflow_uri : null
}

output "airnow_api_key_secret" {
  description = "Secret Manager secret ID for the AirNow API key. Populate the value with `gcloud secrets versions add` after apply. Null when Composer is disabled."
  value       = var.composer_enabled ? google_secret_manager_secret.airnow_api_key[0].secret_id : null
}
