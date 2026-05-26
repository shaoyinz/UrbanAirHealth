# Terraform state bootstrap

`bootstrap.sh` creates one thing: the GCS bucket that holds Terraform's
remote state. Everything else (raw / silver / gold zone buckets,
BigQuery datasets, service accounts) is managed by Terraform in the
parent directory.

## Why a bootstrap step exists

Terraform's `gcs` backend needs the state bucket to **already exist**
before `terraform init` succeeds. You can't create the state bucket
inside the Terraform config that uses it as a backend — that's a
dependency loop. The standard workaround is a one-time imperative
bootstrap.

The state bucket is intentionally **not imported** into Terraform
afterwards. Keeping it out of state means a careless `terraform destroy`
cannot delete the very bucket holding the state file.

## Usage

```bash
PROJECT_ID=urban-air-health-syz ./bootstrap.sh
```

Idempotent — re-runs are safe.

Optional env vars:

| Var         | Default                            | Notes                              |
|-------------|------------------------------------|------------------------------------|
| `PROJECT_ID`| (required)                         | GCP project that owns the bucket   |
| `PREFIX`    | `${PROJECT_ID}-airhealth`          | Bucket name prefix                 |
| `LOCATION`  | `us-central1`                      | Single-region is fine for state    |

The script writes `infra/terraform/backend.hcl`, which is consumed by
`terraform init -backend-config=backend.hcl`.

## After the bootstrap — importing manually-created resources

The raw-zone bucket `gs://${PROJECT_ID}-airhealth-raw` was created
manually pre-Terraform (see the memory `gcp_project.md`). The
Terraform in the parent directory declares it as
`google_storage_bucket.raw`; import it into state before the first
`terraform apply` so the bucket isn't destroyed and recreated:

```bash
cd ..
terraform init -backend-config=backend.hcl
terraform import google_storage_bucket.raw ${PROJECT_ID}-airhealth-raw
terraform plan   # 0 changes expected on the raw bucket; new for silver/gold/BQ/SAs
terraform apply
```

The project itself (`urban-air-health-syz`) is *not* imported — that's
intentional. Terraform would expect to own the parent organization
binding and we don't want to manage that here.

## Tearing down

The state bucket is not in Terraform, so `terraform destroy` won't
touch it. To remove it manually:

```bash
gcloud storage rm --recursive gs://${PROJECT_ID}-airhealth-tfstate
```

Do this **after** `terraform destroy` has cleared the data buckets,
otherwise you lose the state file describing what still exists.
