"""Ingest AirNow hourly PM2.5 observations into the raw zone.

AirNow publishes two ways:

1. **Hourly CSVs** at ``https://files.airnowtech.org/airnow/`` under
   ``<YYYY>/<YYYYMMDD>/HourlyData_<YYYYMMDDHH>.dat`` — free, no key,
   national. We use this for bulk historical pulls (Phase 1).
2. **AQ Observations API** at ``airnowapi.org/aq/data/`` — requires a
   free API key; better for monitor metadata and incremental fetches.
   Phase 3 Airflow uses this with `{{ ds }}` parameterization.

Both paths write one parquet per date to::

    gs://<raw>/airnow/window=<W>/aoi=<NAME>/date=<YYYYMMDD>/observations.parquet

with the **same column schema**, so the silver job
(``compute_monitor_stats`` → ``features.exposure.daily_mean``) consumes a
window without caring which path produced it. The window string in the
GCS path is the idempotency key (matches the release.yaml field); a date
that already exists is a no-op, which is what makes a DAG re-run safe.

* The file-dump path (``main`` / ``_fetch_day``) is the Phase-1 bulk pull:
  no key, walks the 24 hourly CSVs per date over the whole window.
* The API path (``ingest_api_day`` / ``_fetch_api_day``) is the Phase-3
  per-``{{ ds }}`` pull the Airflow DAG calls (dags/air_pipeline_dag.py,
  task ``ingest_airnow``). Keyed; one request per UTC day over the AOI BBOX.
"""

from __future__ import annotations

import argparse
import io
import os
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from airhealth.ingest._common import (
    AoiConfig,
    ReleaseConfig,
    gcs_object_exists,
    gcs_upload,
    http_user_agent,
    load_release_config,
    raw_bucket,
)

# AirNow HourlyData column order is positional and undocumented in the
# files themselves. Source: https://docs.airnowapi.org/HourlyDataFactSheet
HOURLY_COLUMNS = [
    "valid_date", "valid_time", "aqsid", "site_name", "gmt_offset",
    "parameter", "units", "value", "data_source",
]


def parse_window(window: str) -> list[date]:
    """'YYYYMMDD-YYYYMMDD' → inclusive list of dates."""
    start_s, end_s = window.split("-")
    start = datetime.strptime(start_s, "%Y%m%d").date()
    end = datetime.strptime(end_s, "%Y%m%d").date()
    if end < start:
        raise ValueError(f"window {window!r}: end before start")
    n = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(n)]


def hourly_url(base_url: str, d: date, hour: int) -> str:
    return (
        f"{base_url.rstrip('/')}/{d.year:04d}/{d.strftime('%Y%m%d')}"
        f"/HourlyData_{d.strftime('%Y%m%d')}{hour:02d}.dat"
    )


def gcs_target_uri(raw_uri: str, window: str, aoi_name: str, d: date) -> str:
    return (
        f"{raw_uri.rstrip('/')}/airnow/window={window}"
        f"/aoi={aoi_name}/date={d.strftime('%Y%m%d')}/observations.parquet"
    )


def _fetch_day(
    session: requests.Session,
    base_url: str,
    d: date,
    parameters: set[str],
    bbox: tuple[float, float, float, float],
    monitor_meta: pd.DataFrame,
) -> pd.DataFrame:
    """Pull 24 hourly CSVs, filter to AOI+params, return one DataFrame.

    AirNow's hourly file is a tiny CSV (~1–3 MB); fetching 24 of them per
    day-of-pull is cheap. Missing hours (rare) are silently dropped.
    """
    frames: list[pd.DataFrame] = []
    for hour in range(24):
        url = hourly_url(base_url, d, hour)
        r = session.get(url, timeout=30)
        if r.status_code == 404:
            continue
        r.raise_for_status()
        df = pd.read_csv(
            io.StringIO(r.text),
            sep="|",
            names=HOURLY_COLUMNS,
            dtype={"aqsid": "string", "parameter": "string"},
        )
        df = df[df["parameter"].isin(parameters)]
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    day = pd.concat(frames, ignore_index=True)
    # Join monitor lat/lon (hourly file has no coordinates), then bbox-clip.
    day = day.merge(monitor_meta, on="aqsid", how="inner")
    w, s, e, n = bbox
    return day[
        (day["lon"] >= w) & (day["lon"] <= e)
        & (day["lat"] >= s) & (day["lat"] <= n)
    ].reset_index(drop=True)


def _fetch_monitor_metadata(
    session: requests.Session, base_url: str
) -> pd.DataFrame:
    """Pull the AirNow monitoring-site list with lat/lon.

    Published as a single pipe-delimited file under ``today/`` and
    refreshed daily. 23 positional fields, one row per (site × parameter)
    — we keep only {aqsid, lat, lon, site_name, state_code} and dedup on
    aqsid to use for spatially filtering hourly observations.
    """
    url = f"{base_url.rstrip('/')}/today/monitoring_site_locations.dat"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    cols = [
        "aqsid", "parameter", "site_code", "site_name", "status",
        "agency_id", "agency_name", "epa_region", "lat", "lon",
        "elevation", "gmt_offset", "country_code", "msa_code", "msa_name",
        "cbsa_id", "cbsa_name", "state_aqs_code", "state_code",
        "county_aqs_code", "county_name", "f22", "f23",
    ]
    meta = pd.read_csv(
        io.StringIO(r.text), sep="|", names=cols,
        dtype={"aqsid": "string"}, on_bad_lines="skip",
    )
    return meta[["aqsid", "lat", "lon", "site_name", "state_code"]].drop_duplicates("aqsid")


# --- AQ Observations API (Phase 3) -------------------------------------
# The columns `daily_mean` actually requires are {aqsid, valid_date,
# valid_time, value, lat, lon}; the rest ride along for parity with the
# file-dump output and dashboard provenance. Keep this dict the single
# source of truth for the parquet schema both paths emit.
NORMALIZED_DTYPES: dict[str, str] = {
    "aqsid": "string",
    "valid_date": "string",   # 'MM/DD/YY', UTC — see features.exposure
    "valid_time": "string",   # 'HH:MM', UTC
    "parameter": "string",
    "units": "string",
    "value": "float64",       # PM2.5 concentration, µg/m³
    "lat": "float64",
    "lon": "float64",
    "site_name": "string",
    "data_source": "string",
}

# AirNow encodes a missing hourly concentration as -999.
AIRNOW_MISSING = -999.0


def _api_parameter(p: str) -> str:
    """Config parameter token → API parameter code ('PM2.5' → 'PM25')."""
    return p.replace(".", "")


def _new_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = http_user_agent()
    return session


def _coerce_date(day: str | date) -> date:
    """Accept an ISO 'YYYY-MM-DD' string (Airflow `ds`) or a date."""
    return day if isinstance(day, date) else datetime.strptime(day, "%Y-%m-%d").date()


def _pick_col(df: pd.DataFrame, names: tuple[str, ...], default: object = pd.NA) -> pd.Series:
    """First present column among `names`, else a constant-`default` series.

    The aq/data field set shifts slightly across API versions / verbose
    flags, so we look up by a small candidate list rather than a fixed name.
    """
    for nm in names:
        if nm in df.columns:
            return df[nm]
    return pd.Series([default] * len(df), index=df.index)


def _empty_normalized() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=t) for c, t in NORMALIZED_DTYPES.items()})


def _normalize_api_records(
    records: list[dict],
    parameters: set[str],
    bbox: tuple[float, float, float, float],
) -> pd.DataFrame:
    """AirNow aq/data JSON rows → the file-dump-parity parquet schema.

    Pure (no I/O) so it's unit-testable without a key. Drops the -999
    missing sentinel, filters to the configured parameters (the API
    returns the human form, e.g. 'PM2.5', not the request code 'PM25'),
    and re-clips to the AOI bbox defensively even though the request
    already passed a BBOX.
    """
    if not records:
        return _empty_normalized()
    df = pd.DataFrame.from_records(records)
    value_col = next(
        (c for c in ("Value", "Concentration", "RawConcentration") if c in df.columns),
        None,
    )
    if value_col is None:
        raise ValueError(
            f"AirNow API response has no concentration column; got {list(df.columns)}"
        )
    ts = pd.to_datetime(_pick_col(df, ("UTC", "UTCTime")), utc=False)
    out = pd.DataFrame(
        {
            "aqsid": _pick_col(df, ("FullAQSCode", "IntlAQSCode", "AQSID")).astype("string"),
            "valid_date": ts.dt.strftime("%m/%d/%y"),
            "valid_time": ts.dt.strftime("%H:%M"),
            "parameter": _pick_col(df, ("Parameter",)).astype("string"),
            "units": _pick_col(df, ("Unit", "Units"), "UG/M3").astype("string"),
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "lat": pd.to_numeric(_pick_col(df, ("Latitude", "Lat")), errors="coerce"),
            "lon": pd.to_numeric(_pick_col(df, ("Longitude", "Lng", "Lon")), errors="coerce"),
            "site_name": _pick_col(df, ("SiteName", "Site_Name")).astype("string"),
            "data_source": "airnow_api",
        }
    )
    w, s, e, n = bbox
    keep = (
        out["value"].notna()
        & (out["value"] > AIRNOW_MISSING + 1.0)
        & out["parameter"].isin(parameters)
        & out["lon"].between(w, e)
        & out["lat"].between(s, n)
    )
    return out.loc[keep].reset_index(drop=True)


def _fetch_api_day(
    session: requests.Session,
    api_url: str,
    d: date,
    parameters: set[str],
    bbox: tuple[float, float, float, float],
    api_key: str,
) -> pd.DataFrame:
    """One UTC day of AirNow concentrations from aq/data, normalized.

    The API caps rows per request; one day over a metro/state BBOX is well
    under the cap. We request concentration-only JSON, verbose (for
    SiteName + FullAQSCode), permanent+mobile monitors.
    """
    w, s, e, n = bbox
    query = {
        "startDate": f"{d:%Y-%m-%d}T00",
        "endDate": f"{d:%Y-%m-%d}T23",
        "parameters": ",".join(sorted({_api_parameter(p) for p in parameters})),
        "BBOX": f"{w},{s},{e},{n}",
        "dataType": "C",
        "format": "application/json",
        "verbose": "1",
        "monitorType": "2",
        "includerawconcentrations": "0",
        "API_KEY": api_key,
    }
    r = session.get(api_url, params=query, timeout=60)
    r.raise_for_status()
    return _normalize_api_records(r.json(), parameters, bbox)


def ingest_api_day(
    *,
    project_id: str,
    window: str,
    day: str | date,
    api_key: str,
    bucket_prefix: str | None = None,
    cfg: ReleaseConfig | None = None,
    session: requests.Session | None = None,
    stage_dir: str | Path = "data/raw/airnow",
    force: bool = False,
    dry_run: bool = False,
) -> str:
    """Pull one day of AirNow PM2.5 via the keyed API into the raw zone.

    The Phase-3 Airflow entrypoint (dags/air_pipeline_dag.py task
    ``ingest_airnow``), parameterized by ``{{ ds }}``. Writes to the same
    path the file-dump CLI uses, so a window can be ingested by either path
    and the silver job doesn't care which produced it.

    Idempotent: a date that already exists in GCS is a no-op (returns the
    URI unchanged) unless ``force``. Returns the gs:// target URI, which the
    DAG pushes as an XCom for the downstream existence sensor.
    """
    d = _coerce_date(day)
    cfg = cfg or load_release_config()
    aoi = cfg.aoi
    params = set(cfg.airnow_parameters)
    raw_uri = raw_bucket(project_id, bucket_prefix)
    target = gcs_target_uri(raw_uri, window, aoi.name, d)

    if dry_run:
        print(f"  {d}: DRY RUN -> {target}")
        return target
    if not force and gcs_object_exists(target):
        print(f"  {d}: exists, skip -> {target}")
        return target

    session = session or _new_session()
    df = _fetch_api_day(session, cfg.airnow_api_url, d, params, aoi.bbox, api_key)
    if df.empty:
        # Still write the (zero-row, correctly-typed) partition so the
        # downstream GCS sensor passes and the monthly union sees a complete
        # set of date partitions. A genuinely data-less day is rare for a
        # metro AOI but not an error.
        print(f"  {d}: no in-AOI observations; writing empty partition")

    stage = Path(stage_dir)
    stage.mkdir(parents=True, exist_ok=True)
    stage_path = stage / f"airnow_api_{aoi.name}_{d.strftime('%Y%m%d')}.parquet"
    df.to_parquet(stage_path, compression="zstd", index=False)
    print(f"  {d}: {len(df):,} obs -> {target}")
    gcs_upload(stage_path, target)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True, help="GCP project ID owning the raw bucket")
    parser.add_argument("--bucket-prefix", default=None, help="Override bucket name prefix")
    parser.add_argument("--stage-dir", default="data/raw/airnow", help="Local staging dir (gitignored)")
    parser.add_argument("--force", action="store_true", help="Re-upload even if target exists")
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do nothing")
    parser.add_argument("--max-days", type=int, default=None, help="Smoke test: cap dates pulled")
    parser.add_argument(
        "--api", action="store_true",
        help="Use the keyed aq/data API (per-date) instead of the file dump. "
             "Mirrors what the Phase-3 Airflow DAG calls per {{ ds }}.",
    )
    parser.add_argument(
        "--date", default=None,
        help="API mode only: a single date YYYY-MM-DD. Defaults to every date in the window.",
    )
    parser.add_argument(
        "--api-key", default=None,
        help="AirNow API key (or set AIRNOW_API_KEY). Required with --api.",
    )
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    aoi: AoiConfig = cfg.aoi
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    days = [_coerce_date(args.date)] if args.date else parse_window(cfg.airnow_window)
    if args.max_days:
        days = days[: args.max_days]
    params = set(cfg.airnow_parameters)

    print(f"  source:     {'aq/data API' if args.api else 'hourly file dump'}")
    print(f"  window:     {cfg.airnow_window}  ({len(days)} days)")
    print(f"  parameters: {sorted(params)}")
    print(f"  aoi:        {aoi.name}  bbox={aoi.bbox}")
    print(f"  target:     {raw_uri}/airnow/window={cfg.airnow_window}/aoi={aoi.name}/…")

    # --- API path: per-date keyed pull (Phase 3) ------------------------
    if args.api:
        api_key = args.api_key or os.environ.get("AIRNOW_API_KEY")
        if not api_key and not args.dry_run:
            parser.error("--api requires --api-key or the AIRNOW_API_KEY env var")
        session = None if args.dry_run else _new_session()
        for d in days:
            ingest_api_day(
                project_id=args.project_id,
                window=cfg.airnow_window,
                day=d,
                api_key=api_key,
                bucket_prefix=args.bucket_prefix,
                cfg=cfg,
                session=session,
                stage_dir=args.stage_dir,
                force=args.force,
                dry_run=args.dry_run,
            )
        return 0

    if args.dry_run:
        return 0

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = http_user_agent()

    print("  fetching monitor metadata …")
    monitors = _fetch_monitor_metadata(session, cfg.airnow_base_url)
    print(f"    {len(monitors):,} monitors total")

    for d in days:
        target = gcs_target_uri(raw_uri, cfg.airnow_window, aoi.name, d)
        if not args.force and gcs_object_exists(target):
            print(f"  {d}: exists, skip")
            continue
        df = _fetch_day(session, cfg.airnow_base_url, d, params, aoi.bbox, monitors)
        if df.empty:
            print(f"  {d}: no in-AOI observations, skipping write")
            continue
        stage_path = stage_dir / f"airnow_{aoi.name}_{d.strftime('%Y%m%d')}.parquet"
        df.to_parquet(stage_path, compression="zstd", index=False)
        print(f"  {d}: {len(df):,} obs -> {target}")
        gcs_upload(stage_path, target)
        if not args.force:
            shutil.move(str(stage_path), str(stage_path.with_suffix(".parquet.uploaded")))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
