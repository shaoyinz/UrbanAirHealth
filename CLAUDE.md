# Urban Air Health — building-level PM2.5 burden pipeline

## Project goal

Build a scalable, end-to-end data pipeline on Google Cloud Platform that
computes a defensible, building-level **expected health burden**
(Disability-Adjusted Life Years per year) from chronic PM2.5 exposure,
across the continental United States.

The pipeline is designed to scale to ~130 M US buildings and ~2,000
PM2.5 monitor stations. Phase 1 runs locally over the Los Angeles basin
(~3 M Overture buildings) to keep cost bounded while exercising the same
architectural patterns the cloud phases use.

This project is the sibling pivot of UrbanFloodRisk: same tech stack
(Spark + Sedona on Dataproc Serverless → BigQuery → dbt → Cloud Composer
→ Looker/Kepler), different problem domain, deliberately chosen for
cleaner federal data sources and a daily Airflow refresh cadence.

## Output

A BigQuery table with one row per building × year:

- Building identifier (Overture GERS ID), geometry, archetype, footprint area, est. occupancy
- Exposure: annual mean PM2.5 (µg/m³), 98th-percentile-day, peak-week
- Hazard reference: nearest monitor ID, distance to monitor (km), IDW
  neighbor count, monitor data completeness
- Per-cause attributable fraction for {IHD, stroke, COPD, lung cancer, LRI}
- **Expected Annual DALYs** in years per building per year, total and per cause
- ML-predicted PM2.5 with per-building SHAP attributions for top drivers
- Equity overlay: CDC Social Vulnerability Index at the tract,
  equity-weighted human-DALYs
- H3 cell IDs at resolutions 6 through 9 for multi-scale aggregation

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Cloud Storage (raw zone)                                        │
│  ├── airnow/window=YYYYMMDD-YYYYMMDD/aoi=NAME/date=…/           │
│  ├── epa_aqs/year=YYYY/parameter=88101/{daily,annual}.parquet   │
│  ├── overture/release=YYYY-MM-DD.N/aoi=NAME/buildings.parquet   │
│  └── ancillary/{svi,acs,nlcd,...}                                │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│  Cloud Composer (Airflow 2.x)  ── orchestration                  │
│  Daily AirNow CSV → monthly exposure roll-up → annual DALYs      │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│  Dataproc Serverless (Spark 3.5 + Apache Sedona)                 │
│  - Reproject buildings to EPSG:4326, partition by H3 res-7       │
│  - IDW: monitor PM2.5 → building centroid (haversine kernel)     │
│  - Aggregate hourly → daily → monthly → annual exposure          │
│  - Write enriched GeoParquet to silver zone                      │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│  BigQuery (gold zone)                                            │
│  - External tables over silver GeoParquet, then materialize      │
│  - dbt models: AF per cause × DALY per death × population        │
│  - ML PM2.5 gap-fill: BigQuery ML or Vertex AI XGBoost           │
│  - Output: building_dalys date-partitioned fact table            │
└──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
       Looker Studio dashboard  +  Kepler.gl animated H3 heatmap
```

### Why this shape

- **AirNow's daily cadence is what makes Airflow earn its keep.** Hourly
  observations → daily roll-ups → monthly exposure refresh → annual
  DALY recompute is a textbook DAG with `{{ ds }}` idempotency.
- **BigQuery is the *right* tool here**, not a forced fit. Date-partitioned
  exposure facts, window functions for 30/90/365-day rolling exposure,
  dbt incremental models — this is BigQuery's home turf.
- **Sedona's IDW + spatial-join story is clean.** ~2,000 CONUS monitors
  broadcast to all H3 partitions; per-building IDW is embarrassingly
  parallel. Skips the raster pain that UrbanFloodRisk burned 6 hours on.
- **Medallion separation** (raw → silver → gold) keeps lineage explicit
  and lets us reprocess any stage without rerunning the others.

### Storage layer (decisions)

GCS layout is four buckets, all managed by Terraform except the state
bucket (chicken-and-egg). Naming: `${project_id}-airhealth-{zone}`.

| Bucket    | Location          | Versioning | Lifecycle                  |
|-----------|-------------------|------------|----------------------------|
| `tfstate` | `us-central1`     | on         | none                       |
| `raw`     | `US` multi-region | off        | Nearline @30d, Coldline @90d |
| `silver`  | `US` multi-region | off        | Nearline @60d              |
| `gold`    | `US` multi-region | on         | none                       |

Rationale is identical to UrbanFloodRisk: BigQuery US multi-region reads
US multi-region GCS with zero egress; raw/silver are reproducible from
pinned releases so versioning is wasted storage; gold/tfstate are small
and expensive to recompute. Bucket-level access only, public-access
prevention enforced.

### BigQuery datasets

| Dataset                 | Purpose                                              |
|-------------------------|------------------------------------------------------|
| `airhealth_silver_ext`  | External tables over silver-zone GeoParquet         |
| `airhealth_gold`        | dbt fact tables (`fct_building_dalys`, H3, time)    |

---

## Methodology — concentration-response × population

The headline metric is **expected annual DALYs per building**:

```
DALY/yr = Σ_cause AF_cause(PM2.5) × baseline_mortality_cause × population × DALY_per_death_cause
```

This mirrors UrbanFloodRisk's EAD framework so the conceptual machinery
transfers; the integrator in `airhealth/scoring/dalys.py` is the same
trapezoidal code, with the loss-frequency curve replaced by a
concentration-response curve.

### Component 1 — Spatial exposure (IDW)

Per building, compute annual mean PM2.5 via inverse-distance-weighted
interpolation from AirNow + EPA AQS monitors:

```
PM_b = Σ_m w_m · PM_m / Σ_m w_m,   w_m = 1 / d(b,m)^p · completeness_m
```

`p=2` (Shepard default); completeness weighting drops stations with
< 75 % hourly coverage in the year. Max neighbor radius defaults to
150 km (CONUS has gaps in the rural west).

Implementation: `airhealth.scoring.dalys.idw_interpolate`, vectorized
NumPy, called inside a Sedona pandas UDF that partitions by H3 res-7
so per-call broadcast size stays bounded.

### Component 2 — Concentration-response (CR)

Log-linear approximation of the GBD 2019 Integrated Exposure-Response
curves, parameterized as hazard ratio per 10 µg/m³:

```
AF_cause(PM) = 1 − exp(−β_cause · (PM − TMREL)),   TMREL = 2.4 µg/m³
β_cause = ln(HR_per_10) / 10
```

Five PM2.5-attributable causes (Burnett 2018, GBD 2019):

| Cause | HR per 10 µg/m³ |
|-------|-----------------|
| Ischemic heart disease (IHD) | 1.17 |
| Cerebrovascular disease (stroke) | 1.12 |
| Chronic obstructive pulmonary disease (COPD) | 1.13 |
| Tracheal/bronchus/lung cancer | 1.20 |
| Lower respiratory infections (LRI) | 1.18 |

Coefficients live in `config/concentration_response.yaml` so a new GBD
release is a one-file bump.

**Phase-1 simplification.** Log-linear is exact at low-to-moderate PM2.5
(≤ 50 µg/m³) — most of CONUS in non-fire conditions. Above that, GBD
IER bends sublinear and this overestimates. Phase 4 swaps for the full
nonlinear IER; flagged in `Pitfalls`.

### Component 3 — Population

Per building occupancy estimate:

```
pop_b = footprint_area_m² × stories × occupants_per_floor_m²(tract)
```

`occupants_per_floor_m²(tract)` derives from ACS tract population /
total tract building floor-area. Crude but defensible — refined in
Phase 4 with HUD residential-vs-commercial classification.

### Component 4 — DALY aggregation

```
DALY_cause = AF_cause × (baseline_mortality_cause / 100k) × pop × (YLL + DW × YLD) per fatal case
```

Baseline mortality is the US all-ages rate per cause from CDC WONDER
(2022); YLL/YLD/disability-weight from GBD 2019. All values in
`config/concentration_response.yaml`.

Total DALYs per building: `Σ_cause DALY_cause`. Per-cause breakdown
retained for the dashboard "top drivers" panel.

### Component 5 — ML PM2.5 gap-fill (Phase 4)

XGBoost predicts PM2.5 at unmonitored locations from:

- Topographic: elevation, distance to coast, elevation gradient
- Meteorological: ERA5 wind speed/direction, boundary-layer height
- Land cover: NLCD impervious surface, NDVI
- Anthropogenic: distance to major roads (OSM/Overture), HMS smoke plume

Trained against held-out monitors via 10-fold spatial CV (H3 res-6
block holdout). Target RMSE ≤ 3 µg/m³ on the holdout. SHAP per building
surfaces the top drivers. Optional — IDW is the baseline; ML is the
upgrade.

### Component 6 — Equity overlay

CDC Social Vulnerability Index at the 2022 tract level:

```
human_dalys = DALY × SVI_tract_overall
```

Output **both** physical DALYs and equity-weighted human-DALYs as
separate columns. Never collapse into one number — destroys the signal
(carried over from UrbanFloodRisk decision log).

### Component 7 — H3 multi-resolution aggregation

H3 cell IDs at resolutions 6, 7, 8, 9 per building. Aggregate
DALYs/counts to each resolution for dashboard zoom. Same machinery as
the flood project.

---

## Data sources

All free, federal, and re-distributable.

### PM2.5 observations — AirNow (near-real-time)

- Hourly CSVs: `https://files.airnowtech.org/airnow/<YYYY>/<YYYYMMDD>/`
- API: `https://www.airnowapi.org/aq/data/` (free API key)
- License: public domain (EPA / federal-state cooperative)
- Phase 1 ingests the LA-basin slice via the public file dump; Phase 3
  Airflow switches to the API with `{{ ds }}` parameterization.

### PM2.5 historical — EPA AQS

- `https://aqs.epa.gov/aqsweb/airdata/`
- Annual & daily aggregates per parameter & year, CSV-in-ZIP
- License: public domain
- Authoritative QC'd archive; finalized ~6 months behind AirNow.
- Phase 1 use: backfill 2024 LA-basin daily means for IDW validation.

### Building footprints — Overture Maps

- Path: `s3://overturemaps-us-west-2/release/{RELEASE}/theme=buildings/type=building/`
- Format: GeoParquet, partitioned, WKB geometry
- License: CDLA Permissive 2.0
- Same pin as UrbanFloodRisk (`2026-04-15.0`); we copy the AOI slice to
  our GCS bucket on ingest.

### Social vulnerability — CDC SVI

- 2022 SVI at tract level, downloaded as CSV
- Joined to buildings via tract intersection.

### Population — Census ACS

- 5-year ACS, vintage 2018–2022, tract-level total population
- Used for per-building occupancy estimate.

### Concentration-response — GBD 2019 IER

- Burnett 2018 (PNAS), GBD 2019 (Lancet)
- Coefficients hardcoded in `config/concentration_response.yaml`
- Reviewed annually by IHME; bump on new GBD release.

### NOT used (and why)

- **NASA MAIAC / MERRA-2 PM2.5 reanalysis** — global gridded product;
  out of scope for Phase 1. Worth adding in Phase 4 as a satellite-based
  cross-check on IDW.
- **Commercial monitors** (PurpleAir, etc.) — variable QC, not federal,
  license complications. Mention as benchmark only.

---

## Repo layout

```
.
├── CLAUDE.md                   # this file
├── README.md                   # public-facing
├── .python-version             # 3.14
├── .gitignore
├── config/
│   ├── release.yaml            # AirNow/EPA/Overture pins + AOI
│   └── concentration_response.yaml  # GBD IER coefficients
├── dags/                       # Airflow DAGs (Phase 3)
│   └── air_pipeline_dag.py
├── src/airhealth/
│   ├── ingest/                 # airnow, epa_aqs, overture, svi, acs
│   ├── spark/                  # Sedona session + silver job (Phase 2)
│   ├── features/               # rolling exposures, completeness
│   ├── scoring/                # IDW + CR + DALY integrator
│   ├── ml/                     # XGBoost gap-fill + SHAP (Phase 4)
│   └── io/                     # GCS, BigQuery helpers
├── dbt/
│   └── models/{staging,intermediate,marts}/
├── notebooks/exploration/
├── scripts/                    # fix_pyspark_py314.sh + submit wrappers
├── tests/{unit,integration,data}/
└── infra/terraform/            # GCP project, GCS, BQ, Composer (Phase 2+)
```

---

## Coding conventions

- **Python 3.14**, managed with `uv venv`. No `pyproject.toml` while the
  project is exploratory — package on PYTHONPATH via `tests/conftest.py`
  and `sys.path` in notebooks. Re-evaluate when Phase 2 Spark code needs
  packaging for Dataproc Serverless `--py-files`.
- **Linting**: `ruff` per pyproject.toml when one exists; until then,
  the editor's default config. `mypy --strict` on `src/`.
- **Imports**: absolute from `airhealth.*`. No `from x import *`.
- **Spark**: type-annotated PySpark with Sedona. Avoid `.toPandas()` on
  large DataFrames except for tiny lookups.
- **CRS**: everything reprojected to EPSG:4326 at ingest. Document any
  exception with a comment explaining why.
- **dbt**: snake_case, `stg_`, `int_`, `dim_`, `fct_` prefixes. Tests on
  every primary key (unique, not_null) and every foreign key.
- **Airflow**: DAGs idempotent and parameterized by `{{ ds }}` and the
  pinned AirNow window. Use TaskFlow API. No global state outside DAG
  context.
- **Secrets**: GCP Secret Manager via Airflow connections. AirNow API
  key, in particular, never in code.
- **Logging**: `structlog` with JSON output. Include `run_id`,
  `airnow_window`, `aoi` in every record.
- **Testing**: pytest. Spatial fixtures live in `tests/data/` as small
  parquet/GeoParquet files.
- **Commits**: Conventional Commits (`feat:`, `fix:`, `chore:`).
  PRs squash-merged to `main`.

---

## Phased build plan

### Phase 1 — Local prototype (≈ 1 week)
- LA basin only (bbox in `config/release.yaml`).
- DuckDB + `h3` + `folium`, no cloud.
- AirNow last-12-months CSV pull → IDW to Overture LA buildings →
  apply CR curves → render folium choropleth.
- **Goal:** prove the analytical chain before any GCP cost.

### Phase 2 — Single-region cloud pipeline (≈ 1–2 weeks)
- Lift Phase 1 to GCP, scope California.
- Sedona on Dataproc Serverless (reuses session + Terraform from
  UrbanFloodRisk almost verbatim).
- BigQuery with dbt models for DALY aggregation.
- Manual `gcloud` invocation; no Airflow yet.

### Phase 3 — Airflow orchestration (≈ 1 week)
- Cloud Composer DAG: daily AirNow pull → monthly exposure roll-up.
- Dynamic task mapping over states.
- Idempotency keyed on `{{ ds }}` + AirNow window string.
- Data-quality checks (Great Expectations or BQ assertions).

### Phase 4 — ML and CONUS scale (≈ 2 weeks)
- Train XGBoost PM2.5 gap-fill with SHAP.
- Add population (ACS) and equity (SVI) overlays.
- Run for full CONUS.
- Optimize: H3 partitioning, broadcast-join monitors per partition.

### Phase 5 — Visualization (≈ 1 week)
- Looker Studio dashboard (time-series, top-N tracts by human-DALYs).
- Kepler.gl static export: animated H3 heatmap of monthly PM2.5.
- Optional: per-address lookup tool (BigQuery REST + minimal HTML).

---

## Pitfalls and gotchas

- **Monitor sparsity in the rural west.** ~2,000 CONUS monitors is
  ~1 per 1,500 mi² nationally; the Mountain West has gaps of 200+ km.
  IDW's smoothing makes the gap invisible but wrong. Flag buildings
  whose nearest monitor exceeds 100 km and report exposure with a
  `monitor_distance_km` column; Phase 4 ML gap-fill is the fix.
- **Log-linear CR overestimates above 50 µg/m³.** The GBD IER bends
  sublinear in wildfire-smoke conditions. Document the regime where the
  estimate is reliable; Phase 4 swaps for full nonlinear IER.
- **AirNow vs EPA AQS reconciliation.** AirNow is real-time +
  preliminary; EPA AQS is finalized 6 months later. For overlapping
  periods the two won't agree exactly — pick AQS when both exist
  (the silver job's "promote AQS over AirNow" rule).
- **Stations come and go.** A monitor decommissioned mid-year drops a
  building's IDW neighbor count; year-over-year comparisons can shift
  for reasons that have nothing to do with air quality. Annotate
  exposure with completeness; flag drops in the dashboard.
- **BigQuery date-partition explosion.** A daily-partitioned table over
  10 years × 130 M buildings is 470 B rows, which BQ handles but is
  expensive to scan. Cluster by H3 res-6 (clustering, not partitioning)
  so spatial queries prune by hex.
- **Cost discipline.** GCP budget alerts at $5 / $10 / $20. Cap
  Dataproc Serverless executors. BigQuery on-demand. Never run CONUS
  without a cost estimate.

---

## Out of scope (explicitly)

- **Indoor PM2.5.** Indoor concentrations correlate with outdoor but
  depend on ventilation, smoking, cooking. Out of scope; mention as
  caveat in dashboard.
- **Sub-daily exposure dynamics** (acute exposure → asthma ED visits).
  Out of scope; we report chronic burden only.
- **Sources other than PM2.5** (O3, NO2, SO2) — AirNow publishes them
  but the CR literature is thinner. Future extension.
- **Wildfire-specific attribution.** HMS smoke plume layers can split
  PM2.5 into smoke vs. non-smoke; Phase 4+ extension.
- **Real-time / streaming.** Batch-only. Daily orchestration is
  sufficient for chronic-exposure use cases.
- **Web frontend beyond Looker Studio / Kepler.** No bespoke React app.

---

## Decision log

When in doubt, prefer the option that:

1. Produces a defensible health-unit output (DALYs), not abstract scores.
2. Is reproducible from a pinned data window.
3. Generalizes to CONUS even when run on a smaller AOI.
4. Has SHAP-style explainability for any ML output.
5. Keeps the medallion zones independently reproducible.

If a request conflicts with these principles, surface the tension before
implementing.
