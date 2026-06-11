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

variable "composer_enabled" {
  description = "Toggle Cloud Composer (Phase 3 orchestration) on/off. The env has a non-trivial idle cost (~$100+/mo at the smallest size), so default off. Flip to true in terraform.tfvars when you're ready to spend on the DAG."
  type        = bool
  default     = false
}

variable "composer_image_version" {
  description = "Cloud Composer image version. Composer 3 + Airflow 2.x. Check `gcloud composer images list --location=$REGION` for current options; deprecates on a rolling schedule."
  type        = string
  default     = "composer-3-airflow-2.10.5-build.7"
}

variable "composer_environment_size" {
  description = "Composer 3 environment size preset. SMALL is the lowest, sufficient for a daily AirNow DAG."
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"

  validation {
    condition     = contains(["ENVIRONMENT_SIZE_SMALL", "ENVIRONMENT_SIZE_MEDIUM", "ENVIRONMENT_SIZE_LARGE"], var.composer_environment_size)
    error_message = "composer_environment_size must be one of ENVIRONMENT_SIZE_{SMALL,MEDIUM,LARGE}."
  }
}

locals {
  bucket_prefix     = var.bucket_prefix != "" ? var.bucket_prefix : "${var.project_id}-airhealth"
  bq_dataset_prefix = var.bq_dataset_prefix

  common_labels = {
    project    = "airhealth"
    managed_by = "terraform"
  }
}
