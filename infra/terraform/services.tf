# APIs that something else in this Terraform depends on, kept separate
# so the implicit `depends_on` graph is easy to read. Phase-3 (Composer)
# and Phase-4 (Vertex AI for the XGBoost gap-fill) will add their own
# google_project_service resources here.

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}
