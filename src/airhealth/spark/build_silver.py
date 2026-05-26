"""Sedona silver-zone job: raw AirNow + Overture → enriched building GeoParquet.

The Phase-2 step that lifts the Phase-1 notebook onto Dataproc
Serverless. For every Overture building footprint it derives:

  - centroid (lon/lat) and footprint area (m², CONUS Albers EPSG:5070)
  - H3 cell IDs at resolutions 6–9 — multi-scale aggregation keys
  - IDW-interpolated annual mean / 98pct-day / peak-week PM2.5 from the
    AirNow monitor network (completeness-weighted; Shepard p=2)
  - nearest_monitor_id, monitor_distance_km, monitor_neighbor_count for
    auditability (Pitfalls §monitor sparsity)
  - per-cause attributable fraction × baseline mortality × occupancy →
    expected annual DALYs for {IHD, stroke, COPD, lung cancer, LRI},
    plus the total
  - occupancy as ``area_m² × num_floors × 0.04 occupants/m²`` (Phase-1
    crude formula; Phase-4 swaps for ACS tract pop × HUD residential)

Scoring math comes from ``airhealth.scoring.dalys`` unchanged — the same
``idw_interpolate`` + ``expected_annual_dalys`` the notebook called are
imported into a pandas UDF here, so the unit tests in
``tests/unit/test_dalys.py`` cover the silver-job math too.

Run on Dataproc Serverless (Sedona JARs + the ``airhealth`` zip wired by
the submit command in ``scripts/submit_silver.sh``):

    python -m airhealth.spark.build_silver --project-id <gcp-project>

``--dry-run`` prints the resolved input/output URIs and exits.
``--limit N`` caps the building count for a smoke test.
``--local`` adds ``spark.jars.packages`` for a uv-provisioned pyspark.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airhealth.features.exposure import annual_metrics, daily_mean
from airhealth.ingest._common import (
    DEFAULT_RELEASE_YAML,
    AoiConfig,
    ReleaseConfig,
    load_release_config,
    raw_bucket,
    silver_bucket,
)
from airhealth.scoring.dalys import (
    CauseCR,
    CRConfig,
    idw_interpolate,
    load_concentration_response,
)
from airhealth.spark import paths
from airhealth.spark.session import sedona_session

# H3 resolutions emitted per building (CLAUDE.md §Component 7). r7 is
# also the spatial shuffle key — wide enough to balance dense urban
# centers, narrow enough to keep per-partition IDW work bounded.
H3_RESOLUTIONS = (6, 7, 8, 9)

# Default shuffle width for the H3-res-7 repartition. LA basin has ~3M
# buildings spread over ~5k populated res-7 hexes, so 256 partitions is
# already over-sharded — override with --partitions on a larger cluster.
DEFAULT_PARTITIONS = 256

# Phase-1 occupancy stand-in (notebook ``01_la_basin_prototype``). Refined
# in Phase 4 with ACS tract pop × HUD residential mask; keeping the
# constant here so Phase 2 reproduces Phase 1's DALY numbers exactly.
OCCUPANTS_PER_M2_FLOOR = 0.04

# Per CLAUDE.md §Pitfalls: completeness < 0.75 means EPA AQS would not
# call this monitor's year "valid"; we still emit it as a candidate for
# IDW but drop monitors below this bar.
MIN_MONITOR_COMPLETENESS = 0.75

# IDW search radius. CONUS is sparse in the rural West — 150 km is the
# largest gap any major populated area should ever face. Buildings whose
# nearest monitor exceeds 100 km are flagged in the dashboard, so this
# is a hard reachability cap, not a precision target.
IDW_MAX_KM = 150.0


# --------------------------------------------------------------------------
# Monitor-side aggregation (driver-side; the data is small)
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class MonitorStats:
    """Annual per-monitor exposure stats, broadcast into the IDW UDF.

    Held as numpy arrays so the UDF can call ``idw_interpolate`` (vectorized
    over both monitors and buildings) without per-call dataframe → array
    coercion. ``ids`` parallel to the arrays gives the "nearest monitor"
    column the silver schema requires for auditability.
    """

    ids: np.ndarray            # (M,) string
    lon: np.ndarray            # (M,) float64
    lat: np.ndarray            # (M,) float64
    annual_mean: np.ndarray    # (M,) float64
    p98_day: np.ndarray        # (M,) float64
    peak_week: np.ndarray      # (M,) float64


def compute_monitor_stats(
    sedona, airnow_glob: str, *, min_completeness: float = MIN_MONITOR_COMPLETENESS
) -> MonitorStats:
    """Read AirNow hourly observations and reduce to per-monitor annual stats.

    AirNow at LA-basin / CA scale is small enough (≤ ~1 M hourly rows /
    AOI / year) to reduce in the driver, where the existing
    pandas-based ``airhealth.features.exposure`` roll-ups already have
    test coverage. ``recursiveFileLookup`` lets Spark walk the
    ``date=YYYYMMDD/`` partitions even though the parquet schema doesn't
    embed the partition keys. The ``.toPandas()`` collect is bounded by
    ``MIN_MONITOR_COMPLETENESS`` post-aggregation, so even the CONUS
    pull would land at < 10 MB in memory.

    Returns arrays aligned by station so the IDW UDF can broadcast a
    single object. Monitors below the EPA completeness bar are dropped
    here, not in the UDF, so executors never see them.
    """
    hourly = (
        sedona.read.option("recursiveFileLookup", "true")
        .parquet(airnow_glob)
        .toPandas()
    )
    if hourly.empty:
        raise RuntimeError(f"no AirNow rows under {airnow_glob}")
    # Existing pandas roll-ups: parse MM/DD/YY + HH:MM → ts_utc, then
    # daily mean per (aqsid, date), then per-aqsid annual metrics.
    daily = daily_mean(hourly)
    annual = annual_metrics(daily)
    keep = annual["completeness"] >= min_completeness
    annual = annual.loc[keep].dropna(subset=["annual_mean", "lat", "lon"])
    if annual.empty:
        raise RuntimeError(
            f"no monitors meet completeness ≥ {min_completeness}"
        )
    return MonitorStats(
        ids=annual["aqsid"].to_numpy(dtype=object),
        lon=annual["lon"].to_numpy(dtype="float64"),
        lat=annual["lat"].to_numpy(dtype="float64"),
        annual_mean=annual["annual_mean"].to_numpy(dtype="float64"),
        p98_day=annual["p98_day"].to_numpy(dtype="float64"),
        peak_week=annual["peak_week_mean"].to_numpy(dtype="float64"),
    )


# --------------------------------------------------------------------------
# Building-side pipeline
# --------------------------------------------------------------------------
def load_buildings(sedona, uri: str, limit: int | None) -> DataFrame:
    """Read Overture buildings; add centroid, area, and H3 keys.

    Footprint area is measured in EPSG:5070 (CONUS Albers Equal Area)
    so areas are not latitude-distorted. The H3 cell per resolution is
    taken at the centroid; ``true`` asks Sedona for full-cover cells,
    which collapses to one cell for a point input.

    ``limit`` is applied to the raw scan **before** any spatial
    projection, mirroring the floodpipe-silver fix: applying limit after
    ``ST_Centroid`` / ``ST_Transform`` blocks LocalLimit pushdown and the
    executor OOMs decoding a whole multi-hundred-MB row group of WKB.
    """
    df = sedona.read.format("geoparquet").load(uri)
    if limit is not None:
        df = df.limit(limit)
    df = df.selectExpr(
        "id",
        "class AS building_class",
        "subtype AS building_subtype",
        "CAST(num_floors AS INT) AS num_floors",
        "CAST(height AS DOUBLE) AS height_m",
        "geometry",
        "ST_Centroid(geometry) AS centroid",
        "ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:5070')) "
        "AS footprint_area_m2",
    )
    df = df.withColumn("centroid_lon", F.expr("ST_X(centroid)"))
    df = df.withColumn("centroid_lat", F.expr("ST_Y(centroid)"))
    for res in H3_RESOLUTIONS:
        df = df.withColumn(
            f"h3_res{res}",
            F.expr(f"element_at(ST_H3CellIDs(centroid, {res}, true), 1)"),
        )
    return df


# --------------------------------------------------------------------------
# IDW + auditability (mapInPandas; per-partition broadcast monitors)
# --------------------------------------------------------------------------
# Output schema appended to each building partition. Sedona pandas UDFs
# can't easily return Struct columns alongside passthrough columns, so
# we use mapInPandas: each partition pandas DataFrame in, enriched
# pandas DataFrame out (same id column plus exposure columns).
EXPOSURE_COLS = [
    ("pm25_annual_mean", "double"),
    ("pm25_p98_day", "double"),
    ("pm25_peak_week", "double"),
    ("nearest_monitor_id", "string"),
    ("monitor_distance_km", "double"),
    ("monitor_neighbor_count", "int"),
]


def _haversine_km_to_all(
    b_lon: np.ndarray, b_lat: np.ndarray, m_lon: np.ndarray, m_lat: np.ndarray
) -> np.ndarray:
    """(N, M) great-circle distances. Mirrors scoring.dalys._haversine_km
    but exposed here so we can re-use the matrix for the nearest-monitor
    audit column without recomputing it inside ``idw_interpolate``.
    """
    r = 6371.0088
    b_lon_r, b_lat_r, m_lon_r, m_lat_r = (
        np.radians(b_lon[:, None]),
        np.radians(b_lat[:, None]),
        np.radians(m_lon[None, :]),
        np.radians(m_lat[None, :]),
    )
    dlon = m_lon_r - b_lon_r
    dlat = m_lat_r - b_lat_r
    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(b_lat_r) * np.cos(m_lat_r) * np.sin(dlon / 2) ** 2
    )
    return 2 * r * np.arcsin(np.sqrt(a))


def _exposure_for_partition(
    pdf: pd.DataFrame, stats: MonitorStats
) -> pd.DataFrame:
    """Compute IDW exposure + audit columns for one building partition.

    Re-uses ``idw_interpolate`` for the actual interpolation so the
    unit-tested math is the single source of truth. The audit columns
    (nearest monitor + distance + neighbor count within the search
    radius) are derived from the same haversine matrix.
    """
    b_lon = pdf["centroid_lon"].to_numpy(dtype="float64")
    b_lat = pdf["centroid_lat"].to_numpy(dtype="float64")
    out = pdf.copy()

    d_km = _haversine_km_to_all(b_lon, b_lat, stats.lon, stats.lat)
    in_radius = d_km <= IDW_MAX_KM
    out["monitor_neighbor_count"] = in_radius.sum(axis=1).astype("int32")

    # Nearest monitor (globally, not just within radius — the dashboard
    # uses this to display "distance to nearest monitor" even when the
    # building falls outside IDW_MAX_KM and exposure is null).
    nearest_idx = np.argmin(d_km, axis=1)
    out["nearest_monitor_id"] = stats.ids[nearest_idx]
    out["monitor_distance_km"] = d_km[np.arange(len(pdf)), nearest_idx]

    for col_name, values in (
        ("pm25_annual_mean", stats.annual_mean),
        ("pm25_p98_day", stats.p98_day),
        ("pm25_peak_week", stats.peak_week),
    ):
        out[col_name] = idw_interpolate(
            b_lon, b_lat, stats.lon, stats.lat, values,
            power=2.0, max_km=IDW_MAX_KM, min_neighbors=1,
        )
    return out


def interpolate_exposure(buildings: DataFrame, stats: MonitorStats) -> DataFrame:
    """Append IDW + audit columns to ``buildings``.

    Routes only ``(id, centroid_lon, centroid_lat)`` through
    ``mapInPandas`` and equi-joins the result back to ``buildings``.
    Two reasons not to carry every column through pandas:

    1. **Geometry round-trip.** Sedona's WKB GeometryType has no
       native pandas representation; mapInPandas would surface it as
       opaque bytes and we'd lose Sedona's type registration on the
       way back to Spark, breaking the GeoParquet writer.
    2. **Per-row payload size.** N × (geometry + 11 other columns)
       crossing the JVM↔Python serialization boundary dwarfs the
       three floats and one string IDW actually needs.

    Per-partition memory: an (N rows × M monitors) float64 haversine
    matrix. At H3 res-7 / LA-basin scale (~600 rows × ~30 monitors)
    this is a handful of KB; CONUS worst case (~50k rows × ~2000
    monitors) is ~800 MB, still below an executor's default heap.

    ``stats`` is closed over by ``_run``; Spark serializes it once per
    task. A ~few-thousand-row dataclass closes over with negligible
    task-serialization cost, so no explicit ``broadcast()`` lifecycle
    is needed.
    """
    slim = buildings.select("id", "centroid_lon", "centroid_lat")
    out_schema = "id string, " + ", ".join(f"{c} {t}" for c, t in EXPOSURE_COLS)

    def _run(iterator):
        for pdf in iterator:
            enriched = _exposure_for_partition(pdf, stats)
            yield enriched[
                ["id", *(c for c, _ in EXPOSURE_COLS)]
            ]

    exposure = slim.mapInPandas(_run, schema=out_schema)
    return buildings.join(exposure, "id", "left")


# --------------------------------------------------------------------------
# Concentration-response → DALYs (Spark SQL; one literal per cause)
# --------------------------------------------------------------------------
def _population_expr() -> F.Column:
    """Phase-1 crude occupancy: area × floors × 0.04 occupants/m²."""
    floors = F.coalesce(F.col("num_floors").cast("double"), F.lit(1.0))
    floors = F.greatest(floors, F.lit(1.0))
    return (
        F.coalesce(F.col("footprint_area_m2"), F.lit(0.0))
        * floors
        * F.lit(OCCUPANTS_PER_M2_FLOOR)
    )


def _af_expr(pm25: F.Column, cause: CauseCR, counterfactual: float) -> F.Column:
    """AF = 1 - exp(-β · max(PM - TMREL, 0))."""
    excess = F.greatest(pm25 - F.lit(counterfactual), F.lit(0.0))
    return F.lit(1.0) - F.exp(-F.lit(cause.beta_per_ugm3) * excess)


def attach_dalys(buildings: DataFrame, cr: CRConfig) -> DataFrame:
    """Append per-cause + total DALYs/yr per building.

    Mirrors ``airhealth.scoring.dalys.expected_annual_dalys`` but in
    pure Spark SQL — the YAML coefficients are baked into expression
    literals so executors never need the YAML loader. ``pm25_annual_mean``
    is the chronic-exposure driver per GBD methodology; episodic
    metrics (p98_day, peak_week) ride along in silver for the dashboard
    but do not feed the headline DALY.
    """
    out = buildings.withColumn("population", _population_expr())
    pm = F.col("pm25_annual_mean")
    daly_cols: list[str] = []
    for cause in cr.causes:
        af = _af_expr(pm, cause, cr.counterfactual_ugm3)
        attributable_deaths = (
            af
            * F.lit(cause.baseline_mortality_per_100k / 1e5)
            * F.col("population")
        )
        col_name = f"daly_{cause.key}"
        out = out.withColumn(
            col_name,
            attributable_deaths * F.lit(cause.daly_per_death()),
        )
        daly_cols.append(col_name)
    total = daly_cols[0]
    total_expr = F.col(total)
    for c in daly_cols[1:]:
        total_expr = total_expr + F.col(c)
    out = out.withColumn("daly_total", total_expr)
    return out


# --------------------------------------------------------------------------
# Driver entry point
# --------------------------------------------------------------------------
SILVER_COLUMNS = [
    "id",
    "geometry",
    "centroid_lon",
    "centroid_lat",
    "building_class",
    "building_subtype",
    "num_floors",
    "height_m",
    "footprint_area_m2",
    *(f"h3_res{res}" for res in H3_RESOLUTIONS),
    "pm25_annual_mean",
    "pm25_p98_day",
    "pm25_peak_week",
    "nearest_monitor_id",
    "monitor_distance_km",
    "monitor_neighbor_count",
    "population",
    "daly_ihd",
    "daly_stroke",
    "daly_copd",
    "daly_lung_cancer",
    "daly_lri",
    "daly_total",
]


def build(
    sedona,
    raw_uri: str,
    silver_uri: str,
    cfg: ReleaseConfig,
    cr: CRConfig,
    *,
    partitions: int,
    limit: int | None,
    min_completeness: float = MIN_MONITOR_COMPLETENESS,
) -> str:
    """Run the full silver pipeline and write GeoParquet. Returns output URI."""
    aoi: AoiConfig = cfg.aoi
    overture_uri = paths.overture_input(raw_uri, cfg.overture_release, aoi.name)
    airnow_glob = paths.airnow_input_glob(raw_uri, cfg.airnow_window, aoi.name)
    out_uri = paths.silver_buildings_output(silver_uri, cfg.overture_release, aoi.name)

    print(f"  overture in: {overture_uri}")
    print(f"  airnow glob: {airnow_glob}")
    print(f"  silver out : {out_uri}")

    stats = compute_monitor_stats(
        sedona, airnow_glob, min_completeness=min_completeness
    )
    print(f"  monitors:    {stats.ids.size} (after completeness filter)")

    buildings = load_buildings(sedona, overture_uri, limit)
    # Repartition by H3 res-7 before exposure — a handful of dense urban
    # hexes otherwise stall a single executor (CLAUDE.md §Pitfalls; same
    # reason floodpipe-silver repartitions on h3_res7).
    buildings = buildings.repartition(partitions, "h3_res7")

    enriched = interpolate_exposure(buildings, stats)
    enriched = attach_dalys(enriched, cr)
    out = enriched.select(*SILVER_COLUMNS)
    out.write.format("geoparquet").mode("overwrite").save(out_uri)
    return out_uri


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--bucket-prefix", default=None)
    parser.add_argument(
        "--partitions",
        type=int,
        default=DEFAULT_PARTITIONS,
        help="shuffle width for the H3 res-7 repartition",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="cap building count for a smoke test",
    )
    parser.add_argument(
        "--release-config",
        default=None,
        help=(
            "path to release.yaml. Defaults to the repo-relative "
            "config/release.yaml; on Dataproc Serverless, pass the bare "
            "filename and ship it via 'gcloud dataproc batches submit --files'."
        ),
    )
    parser.add_argument(
        "--cr-config",
        default=None,
        help="path to concentration_response.yaml (defaults to repo config/)",
    )
    parser.add_argument(
        "--min-completeness",
        type=float,
        default=MIN_MONITOR_COMPLETENESS,
        help=(
            "drop monitors below this hourly-coverage fraction. EPA AQS bar "
            "is 0.75; smoke tests with partial-year AirNow stage data may "
            "need to lower it."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--local",
        action="store_true",
        help=(
            "run against a local pip-installed pyspark; pulls Sedona JARs "
            "from Maven Central via spark.jars.packages. Do NOT pass this "
            "on Dataproc Serverless — the submit command wires the matching "
            "Scala-2.13 build there."
        ),
    )
    args = parser.parse_args(argv)

    cfg = (
        load_release_config(Path(args.release_config))
        if args.release_config
        else load_release_config()
    )
    cr_path = (
        Path(args.cr_config) if args.cr_config
        else DEFAULT_RELEASE_YAML.parent / "concentration_response.yaml"
    )
    cr = load_concentration_response(cr_path)
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    silver_uri = silver_bucket(args.project_id, args.bucket_prefix)

    print(f"  release:  {cfg.overture_release}")
    print(f"  window:   {cfg.airnow_window}")
    print(f"  aoi:      {cfg.aoi.name}  bbox={cfg.aoi.bbox}")
    print(f"  raw in:   {raw_uri}")
    print(
        "  out:      "
        + paths.silver_buildings_output(silver_uri, cfg.overture_release, cfg.aoi.name)
    )
    if args.dry_run:
        print("  dry run — no Spark session started")
        return 0

    sedona = sedona_session(
        "airhealth-silver-buildings", local_packages=args.local
    )
    try:
        out_uri = build(
            sedona,
            raw_uri,
            silver_uri,
            cfg,
            cr,
            partitions=args.partitions,
            limit=args.limit,
            min_completeness=args.min_completeness,
        )
        print(f"  done -> {out_uri}")
    finally:
        sedona.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
