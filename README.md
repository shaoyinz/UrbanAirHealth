# UrbanAirHealth

Building-level expected health burden (DALYs/yr) from chronic PM2.5
exposure, computed across CONUS. Same tech stack as
[UrbanFloodRisk](../UrbanFloodRisk) — Sedona on Dataproc Serverless →
BigQuery → dbt → Cloud Composer → Looker/Kepler — applied to a problem
with cleaner federal data sources and a daily Airflow refresh cadence.

See [CLAUDE.md](./CLAUDE.md) for the full design doc, methodology, and
data-source rationale.

## Status

Phase 1 — local prototype. **In progress.**

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

## Relationship to UrbanFloodRisk

- The two repos are **siblings**, not forks. Lift-overs (Sedona session,
  ingest helpers, Terraform medallion buckets, EAD trapezoidal
  integrator) are clean copies. The flood repo is untouched.
- If you need to inspect a lifted file's original context, look at
  `../UrbanFloodRisk/src/floodpipe/...`.
