"""Ingest EPA AQS annual + daily PM2.5 aggregates into the raw zone.

EPA AQS is the authoritative US air-monitoring archive: validated,
QC'd, annually finalized aggregates with full station metadata. It
trails real-time AirNow by ~6 months (data through year Y-1 is final
by mid-Y), so this is the source for *backfill* — multi-year IDW
training, ML labels, dashboard "historical context" panels.

Files at https://aqs.epa.gov/aqsweb/airdata/ — one CSV-in-ZIP per
(product × parameter × year). We pull two products per parameter:

- ``daily_<param>_<year>.zip``   — site-day means (the main grain)
- ``annual_conc_by_monitor_<year>.zip`` — annual means, all parameters

Idempotency key is (year, parameter); GCS path::

    gs://<raw>/epa_aqs/year=<Y>/parameter=<P>/{daily,annual}.parquet
"""

from __future__ import annotations

import argparse
import io
import shutil
import zipfile
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


def daily_url(base_url: str, parameter: str, year: str) -> str:
    return f"{base_url.rstrip('/')}/daily_{parameter}_{year}.zip"


def annual_url(base_url: str, year: str) -> str:
    # AQS bundles all parameters into one annual file per year.
    return f"{base_url.rstrip('/')}/annual_conc_by_monitor_{year}.zip"


def gcs_target_daily(raw_uri: str, year: str, parameter: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/epa_aqs/year={year}"
        f"/parameter={parameter}/daily.parquet"
    )


def gcs_target_annual(raw_uri: str, year: str, parameter: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/epa_aqs/year={year}"
        f"/parameter={parameter}/annual.parquet"
    )


def _download_zip(session: requests.Session, url: str, dest: Path) -> Path:
    """Stream a ZIP to disk; AQS files run 50–300 MB."""
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return dest


def _read_zipped_csv(zip_path: Path, parameter: str | None = None) -> pd.DataFrame:
    """Open AQS CSV inside the ZIP; optionally filter to one parameter code."""
    with zipfile.ZipFile(zip_path) as z:
        # Each AQS ZIP contains exactly one CSV with the same stem.
        names = [n for n in z.namelist() if n.endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(f"{zip_path.name}: expected 1 CSV, found {names}")
        with z.open(names[0]) as fh:
            df = pd.read_csv(fh, low_memory=False)
    if parameter is not None:
        df = df[df["Parameter Code"] == int(parameter)]
    return df.reset_index(drop=True)


def _filter_to_aoi(df: pd.DataFrame, aoi: AoiConfig) -> pd.DataFrame:
    """AQS uses 'Latitude'/'Longitude' (decimal degrees) and 'State Code'."""
    w, s, e, n = aoi.bbox
    return df[
        (df["Longitude"] >= w) & (df["Longitude"] <= e)
        & (df["Latitude"] >= s) & (df["Latitude"] <= n)
    ].reset_index(drop=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True, help="GCP project ID owning the raw bucket")
    parser.add_argument("--bucket-prefix", default=None, help="Override bucket name prefix")
    parser.add_argument("--stage-dir", default="data/raw/epa_aqs", help="Local staging dir (gitignored)")
    parser.add_argument("--force", action="store_true", help="Re-upload even if target exists")
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do nothing")
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    aoi: AoiConfig = cfg.aoi
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    year = cfg.epa_aqs_year

    print(f"  year:       {year}")
    print(f"  parameters: {list(cfg.epa_aqs_parameters)}")
    print(f"  aoi:        {aoi.name}  bbox={aoi.bbox}")

    if args.dry_run:
        return 0

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = http_user_agent()

    for parameter in cfg.epa_aqs_parameters:
        # Daily means
        target = gcs_target_daily(raw_uri, year, parameter)
        if args.force or not gcs_object_exists(target):
            zip_path = stage_dir / f"daily_{parameter}_{year}.zip"
            print(f"  downloading {daily_url(cfg.epa_aqs_base_url, parameter, year)}")
            _download_zip(session, daily_url(cfg.epa_aqs_base_url, parameter, year), zip_path)
            df = _read_zipped_csv(zip_path, parameter=parameter)
            df = _filter_to_aoi(df, aoi)
            parquet_path = stage_dir / f"daily_{parameter}_{year}_{aoi.name}.parquet"
            df.to_parquet(parquet_path, compression="zstd", index=False)
            print(f"    daily: {len(df):,} rows -> {target}")
            gcs_upload(parquet_path, target)
            shutil.move(str(parquet_path), str(parquet_path.with_suffix(".parquet.uploaded")))
        else:
            print(f"  daily: exists, skip")

        # Annual means
        target = gcs_target_annual(raw_uri, year, parameter)
        if args.force or not gcs_object_exists(target):
            zip_path = stage_dir / f"annual_{year}.zip"
            if not zip_path.exists():
                print(f"  downloading {annual_url(cfg.epa_aqs_base_url, year)}")
                _download_zip(session, annual_url(cfg.epa_aqs_base_url, year), zip_path)
            df = _read_zipped_csv(zip_path, parameter=parameter)
            df = _filter_to_aoi(df, aoi)
            parquet_path = stage_dir / f"annual_{parameter}_{year}_{aoi.name}.parquet"
            df.to_parquet(parquet_path, compression="zstd", index=False)
            print(f"    annual: {len(df):,} rows -> {target}")
            gcs_upload(parquet_path, target)
            shutil.move(str(parquet_path), str(parquet_path.with_suffix(".parquet.uploaded")))
        else:
            print(f"  annual: exists, skip")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
