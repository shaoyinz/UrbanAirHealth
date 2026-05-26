# Medallion data buckets. Naming is `${bucket_prefix}-${zone}`; the
# default prefix `${project_id}-airhealth` reaches the manually-created
# `urban-air-health-syz-airhealth-raw` exactly so `terraform import`
# slots cleanly into state. See main.tf for the import incantation.
#
# Location rationale (CLAUDE.md §Storage layer): US multi-region for
# all data zones so BigQuery (which we run in US multi-region) reads
# them with zero egress. Lifecycle transitions on raw/silver match the
# medallion-cost table — they're aggressive because both zones are
# reproducible from pinned upstream releases (`config/release.yaml`).
# Gold has versioning on instead of lifecycle, because it's the only
# zone that's expensive to recompute (dbt-materialized DALYs).

resource "google_storage_bucket" "raw" {
  name          = "${local.bucket_prefix}-raw"
  location      = var.data_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = merge(local.common_labels, { zone = "raw" })
}

resource "google_storage_bucket" "silver" {
  name          = "${local.bucket_prefix}-silver"
  location      = var.data_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(local.common_labels, { zone = "silver" })
}

resource "google_storage_bucket" "gold" {
  name          = "${local.bucket_prefix}-gold"
  location      = var.data_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = merge(local.common_labels, { zone = "gold" })
}

# Dataproc Serverless deps bucket — holds the packaged airhealth.zip
# and any per-batch staging the submit script uploads. Kept separate
# from the medallion zones so its lifecycle and IAM don't intersect
# with data permissions. Single-region (cheaper than multi-region for
# the small payloads + the deps are recreated per submit anyway).
resource "google_storage_bucket" "dataproc_deps" {
  name          = "${local.bucket_prefix}-dataproc-deps"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  # Submit artifacts go stale within hours; clean them up to keep the
  # bucket small. Adjust if Phase 3 Airflow needs longer retention.
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(local.common_labels, { zone = "dataproc-deps" })
}
