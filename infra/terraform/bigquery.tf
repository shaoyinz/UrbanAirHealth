# BigQuery datasets. Two zones:
#
# - silver_ext: read-only external tables over silver-zone GeoParquet.
#   The truth lives in GCS — these are pointers BQ uses to push down
#   spatial queries onto the parquet files Sedona wrote. Re-pointing
#   them at a new release is the silver job's responsibility.
#
# - gold: dbt-materialized fact tables, the layer Looker/Kepler reads.
#   `fct_building_dalys` is the headline; tract / H3 aggregates follow.
#
# `delete_contents_on_destroy = false` is a deliberate guardrail.
# A careless `terraform destroy` should not wipe months of gold-zone
# DALYs — drop the dataset by hand if you really mean it.

resource "google_bigquery_dataset" "silver_ext" {
  dataset_id  = "${local.bq_dataset_prefix}_silver_ext"
  location    = var.data_location
  description = "External tables over silver-zone GeoParquet. Read-only from BQ; truth lives in GCS."

  delete_contents_on_destroy = false

  labels = merge(local.common_labels, { zone = "silver_ext" })

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "${local.bq_dataset_prefix}_gold"
  location    = var.data_location
  description = "Materialized dbt outputs: fct_building_dalys, H3 aggregates, equity overlays."

  delete_contents_on_destroy = false

  labels = merge(local.common_labels, { zone = "gold" })

  depends_on = [google_project_service.bigquery]
}
