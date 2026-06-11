# APIs that something else in this Terraform depends on, kept separate
# so the implicit `depends_on` graph is easy to read. Phase-4 (Vertex AI
# for the XGBoost gap-fill) will add its own google_project_service
# resources here.

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# Composer + Secret Manager APIs are toggled with var.composer_enabled
# because enabling them adds nothing on its own, but the env they
# unlock has a non-trivial idle cost. See composer.tf.
resource "google_project_service" "composer" {
  count              = var.composer_enabled ? 1 : 0
  service            = "composer.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  count              = var.composer_enabled ? 1 : 0
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}
