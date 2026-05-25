"""Ingest AirNow hourly PM2.5 observations into the raw zone.

AirNow publishes two ways:

1. **Hourly CSVs** at ``https://files.airnowtech.org/airnow/`` under
   ``<YYYY>/<YYYYMMDD>/HourlyData_<YYYYMMDDHH>.dat`` — free, no key,
   national. We use this for bulk historical pulls (Phase 1).
2. **AQ Observations API** at ``airnowapi.org/aq/data/`` — requires a
   free API key; better for monitor metadata and incremental fetches.
   Phase 3 Airflow uses this with `{{ ds }}` parameterization.

This Phase-1 CLI does (1): for each date in the configured window, pull
the 24 hourly CSVs, filter rows to the AOI bbox and configured parameter
list, concatenate, and write one parquet per date to::

    gs://<raw>/airnow/window=<W>/aoi=<NAME>/date=<YYYYMMDD>/observations.parquet

The window string in the GCS path is the idempotency key (matches the
release.yaml field). Bumping it forces a fresh pull.
"""

from __future__ import annotations

import argparse
import io
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True, help="GCP project ID owning the raw bucket")
    parser.add_argument("--bucket-prefix", default=None, help="Override bucket name prefix")
    parser.add_argument("--stage-dir", default="data/raw/airnow", help="Local staging dir (gitignored)")
    parser.add_argument("--force", action="store_true", help="Re-upload even if target exists")
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do nothing")
    parser.add_argument("--max-days", type=int, default=None, help="Smoke test: cap dates pulled")
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    aoi: AoiConfig = cfg.aoi
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    days = parse_window(cfg.airnow_window)
    if args.max_days:
        days = days[: args.max_days]
    params = set(cfg.airnow_parameters)

    print(f"  window:     {cfg.airnow_window}  ({len(days)} days)")
    print(f"  parameters: {sorted(params)}")
    print(f"  aoi:        {aoi.name}  bbox={aoi.bbox}")
    print(f"  target:     {raw_uri}/airnow/window={cfg.airnow_window}/aoi={aoi.name}/…")

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
