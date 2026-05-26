variable "project_id" {
  description = "GCP project ID that owns the storage."
  type        = string
}

variable "region" {
  description = "Default region for single-region resources. Data buckets use the US multi-region (see data_location)."
  type        = string
  default     = "us-central1"
}

variable "data_location" {
  description = "Location for medallion data buckets. US multi-region keeps reads from BigQuery US multi-region datasets free of egress."
  type        = string
  default     = "US"
}

variable "bucket_prefix" {
  description = "Prefix for bucket names. Final names are {prefix}-{zone}. Defaults to '{project_id}-airhealth' for global uniqueness."
  type        = string
  default     = ""
}

variable "bq_dataset_prefix" {
  description = "Prefix for BigQuery dataset IDs. Final IDs are {prefix}_{zone}. Underscores only — BQ disallows hyphens."
  type        = string
  default     = "airhealth"
}

locals {
  bucket_prefix     = var.bucket_prefix != "" ? var.bucket_prefix : "${var.project_id}-airhealth"
  bq_dataset_prefix = var.bq_dataset_prefix

  common_labels = {
    project    = "airhealth"
    managed_by = "terraform"
  }
}
