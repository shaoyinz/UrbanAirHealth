terraform {
  required_version = ">= 1.7.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }

  # State bucket is created by infra/terraform/bootstrap/bootstrap.sh,
  # which writes backend.hcl. Initialize with:
  #   terraform init -backend-config=backend.hcl
  backend "gcs" {}
}
