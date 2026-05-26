# UrbanAirHealth — Phase 2 GCP infrastructure.
#
# This Terraform owns: medallion data buckets (raw / silver / gold),
# BigQuery datasets (silver-ext, gold), service accounts + IAM for the
# Dataproc Serverless silver job and the dbt gold materializer.
#
# It does NOT own: the GCP project itself (created out-of-band) and the
# tfstate bucket (bootstrapped by infra/terraform/bootstrap/bootstrap.sh —
# would be a dependency loop otherwise).
#
# Importing the manually-created raw bucket
# -----------------------------------------
# The raw-zone bucket `${project_id}-airhealth-raw` was created with
# `gcloud storage buckets create` during Phase 1 so we could land
# AirNow + Overture before Terraform existed (see memory
# gcp_project.md). Before the first `terraform apply`, import it:
#
#   terraform init -backend-config=backend.hcl
#   terraform import google_storage_bucket.raw \
#     $(terraform output -raw -no-color raw_bucket_name 2>/dev/null \
#       || echo "${TF_VAR_project_id:-urban-air-health-syz}-airhealth-raw")
#   terraform plan
#
# `terraform plan` should report zero changes on the raw bucket — the
# resource definition in buckets.tf matches the manual `gcloud`
# invocation exactly. Any drift means the manual create has diverged
# from this file; fix the .tf, don't fight the plan.

provider "google" {
  project = var.project_id
  region  = var.region
}
