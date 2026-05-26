# UrbanAirHealth

Building-level expected health burden (DALYs/yr) from chronic PM2.5
exposure, computed across CONUS. Same tech stack as
[UrbanFloodRisk](../UrbanFloodRisk) — Sedona on Dataproc Serverless →
BigQuery → dbt → Cloud Composer → Looker/Kepler — applied to a problem
with cleaner federal data sources and a daily Airflow refresh cadence.

See [CLAUDE.md](./CLAUDE.md) for the full design doc, methodology, and
data-source rationale.

## Status

Phase 2 — single-region cloud lift. **Done.**

- ✓ Sedona silver job `src/airhealth/spark/build_silver.py` — reads
  raw Overture + AirNow, computes IDW exposure + per-cause AF + DALYs,
  writes GeoParquet partitioned by H3 r7. Same scoring math as Phase 1
  (imports `airhealth.scoring.dalys`), so the existing unit tests cover
  the silver-job arithmetic.
- ✓ Local smoke `scripts/smoke_silver_local.py` — stages the Phase-1
  raw fixtures into a temp GCS-mirror layout and runs the Sedona job
  via a pip-installed pyspark (`--local`, pulls Sedona JARs from Maven
  Central). 2,000-building smoke completes in ~30 s on a laptop.
- ✓ dbt models (`dbt/`) — staging passthrough + `fct_building_dalys`
  mart, runs against DuckDB locally (`scripts/dbt build`) and is wired
  for BigQuery via a second profile target. 12 data tests pass on the
  smoke output: PK uniqueness, not-nulls, monitor_distance bucket
  enum, and a guard against negative DALYs.
- ✓ Terraform: bootstrap (tfstate) + main (services, buckets,
  BigQuery, Dataproc network). The manually-created raw bucket is
  `terraform import`-ed; the rest is managed by `infra/terraform/`.
- ✓ Dataproc Serverless submit script `scripts/submit_silver.sh` —
  first end-to-end cloud run green on the LA-basin slice.

Phase 1 — local prototype. **Done.**

- ✓ Scoring library `src/airhealth/scoring/dalys.py` — IDW + log-linear
  concentration-response + trapezoidal integrator (lifted from
  UrbanFloodRisk's EAD math).
- ✓ Pinned upstream data in `config/release.yaml` (AirNow window, EPA
  AQS year, Overture release, AOI bbox).
- ✓ GBD 2019 IER coefficients in `config/concentration_response.yaml`
  for five PM2.5-attributable causes.
- ✓ Ingest CLIs: `airhealth.ingest.{airnow,epa_aqs,overture}`. AOI is
  driven from `config/release.yaml`; retargeting is a one-line edit.
- ✓ Feature roll-ups `src/airhealth/features/` — hourly→daily,
  completeness, annual mean / 98pct / peak-week, H3 r6–r9 indexing.
- ✓ Local IO `src/airhealth/io/local.py` — AirNow window reader,
  Overture centroid extraction (DuckDB spatial), H3 aggregation,
  folium choropleth writer.
- ✓ End-to-end notebook
  `notebooks/exploration/01_la_basin_prototype.ipynb` —
  AirNow → daily means → annual IDW → CR → DALYs → folium map.
  Authored from `scripts/build_phase1_notebook.py` so cells stay terse.
- ✓ Unit tests: `pytest tests/unit/` (26 tests) covers IDW, AF
  formula, DALY aggregation, integrator, daily-mean, completeness,
  annual metrics.
- ⏳ Phase 2: Sedona job, Terraform, dbt models.

## Quickstart (local dev)

```bash
# venv (Python 3.14)
uv venv --python 3.14
source .venv/bin/activate
uv pip install duckdb h3 pandas pyarrow folium requests pyyaml numpy pytest nbformat nbclient ipykernel

# unit tests (tests/conftest.py adds src/ to sys.path)
pytest tests/unit -v

# dry-run ingest CLIs (set PYTHONPATH=src so `airhealth` resolves
# without a pyproject install — same pattern as UrbanFloodRisk)
export PYTHONPATH=src
python -m airhealth.ingest.airnow   --project-id PLACEHOLDER --dry-run
python -m airhealth.ingest.epa_aqs  --project-id PLACEHOLDER --dry-run
python -m airhealth.ingest.overture --project-id PLACEHOLDER --dry-run
```

For real ingest:

1. Get a free [AirNow API key](https://docs.airnowapi.org/) — needed
   only for Phase 3 incremental fetches; Phase 1 uses the public file
   dump.
2. `gcloud auth login` and `gcloud config set project <PROJECT_ID>`.
3. Bootstrap GCP infra (Phase 2 — Terraform pending).
4. Drop the `--dry-run` flags above.

## Phase 1 prototype run

```bash
# 1. Stage a day or three of AirNow + the Overture LA slice (one-time).
export PYTHONPATH=src
python -m airhealth.ingest.airnow   --project-id <PROJECT_ID> --max-days 7
python -m airhealth.ingest.overture --project-id <PROJECT_ID>

# 2. (Re)generate the notebook from scripts/ then execute it.
python scripts/build_phase1_notebook.py
jupyter execute notebooks/exploration/01_la_basin_prototype.ipynb

# 3. Open the resulting folium choropleth.
open data/la_basin_dalys_r8.html
```

The notebook reads whatever AirNow days are staged under
`data/raw/airnow/` (and tolerates the `.parquet.uploaded` suffix), so a
no-GCP run still works — populate the staging dir by hand if needed.

## Retargeting to a different AOI

Edit `config/release.yaml`:

```yaml
aoi:
  name: bay_area
  bbox: [-122.55, 37.20, -121.75, 38.10]
  state_fips: "06"
  state_abbr: "CA"
```

Every ingest CLI and (in Phase 2) the Sedona job picks up the new AOI
on the next run; the GCS path includes `aoi=<NAME>` so old and new
slices coexist.

## Phase 2 — local vertical slice

```bash
# 1. One-time: install pyspark + sedona into the project venv, then
#    patch pyspark 3.5's bundled cloudpickle for Python 3.14 compat.
uv pip install 'pyspark==3.5.0' 'apache-sedona==1.6.1' shapely 'cloudpickle==3.1.2'
bash scripts/fix_pyspark_py314.sh

# 2. Run the Sedona silver job over the LA-basin fixtures from Phase 1.
#    --limit caps the building count; --keep-tmp leaves silver/ on disk
#    so the next dbt step can read it.
PYTHONPATH=src python scripts/smoke_silver_local.py --limit 5000 --keep-tmp
#  → tmp dir: /var/folders/.../airhealth-silver-smoke-XXXXX

# 3. One-time: bootstrap the dbt venv. dbt-core doesn't import on
#    Python 3.14 yet, so dbt lives in its own 3.12 venv under dbt/.
uv venv --python 3.12 dbt/.venv
VIRTUAL_ENV=dbt/.venv uv pip install 'dbt-duckdb>=1.9,<2'

# 4. Build the dbt models against the silver output above.
SMOKE=/var/folders/.../airhealth-silver-smoke-XXXXX   # paste from step 2
DBT_SILVER_GLOB="${SMOKE}/silver/buildings/release=*/aoi=*/*.parquet" \
  scripts/dbt build
#  → 1 view (stg_building_silver) + 1 table (fct_building_dalys)
#    + 10 data tests pass. dbt/target/airhealth.duckdb holds the mart.
```

## Phase 2 — cloud invocation (Dataproc Serverless + BigQuery)

```bash
# 1. Bootstrap the Terraform state bucket (one-time).
PROJECT_ID=urban-air-health-syz infra/terraform/bootstrap/bootstrap.sh

# 2. Import the existing manually-created raw bucket so Terraform
#    doesn't try to destroy-and-recreate it on first apply.
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # edit if needed
terraform init -backend-config=backend.hcl
terraform import google_storage_bucket.raw \
  "${PROJECT_ID:-urban-air-health-syz}-airhealth-raw"
terraform plan    # raw bucket: no changes; silver/gold/BQ/SAs: create
terraform apply

# 3. Submit the Sedona silver job. Defaults to the AOI in
#    config/release.yaml (LA basin); add --limit for a smoke.
cd ../..
PROJECT_ID=urban-air-health-syz scripts/submit_silver.sh --limit 50000

# 4. Run dbt against the BigQuery target.
GCP_PROJECT_ID=urban-air-health-syz scripts/dbt build --target bigquery
```

See `infra/terraform/bootstrap/README.md` for the import rationale and
`scripts/submit_silver.sh` for the Dataproc Serverless property
incantations (Sedona Scala-2.13 build, autoBroadcast cap, executor
sizing).

## Relationship to UrbanFloodRisk

- The two repos are **siblings**, not forks. Lift-overs (Sedona session,
  ingest helpers, Terraform medallion buckets, EAD trapezoidal
  integrator) are clean copies. The flood repo is untouched.
- If you need to inspect a lifted file's original context, look at
  `../UrbanFloodRisk/src/floodpipe/...`.
