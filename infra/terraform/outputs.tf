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
