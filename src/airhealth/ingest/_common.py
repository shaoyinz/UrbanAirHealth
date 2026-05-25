"""Shared ingest helpers: config loading, AOI bbox, GCS shell-outs.

We use ``gcloud storage`` via subprocess rather than google-cloud-storage
to keep the Python deps minimal. The operator is already authenticated
with gcloud to run Terraform, so this borrows that auth.

Lifted from UrbanFloodRisk/src/floodpipe/ingest/_common.py; the AOI
constant is now driven from ``config/release.yaml`` rather than hardcoded
to a single state, so a one-line bbox edit retargets every ingest CLI.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RELEASE_YAML = REPO_ROOT / "config" / "release.yaml"


@dataclass(frozen=True)
class AoiConfig:
    """Area of interest — single source of truth for which rows we keep."""

    name: str
    bbox: tuple[float, float, float, float]   # (W, S, E, N) EPSG:4326
    state_fips: str
    state_abbr: str


@dataclass(frozen=True)
class ReleaseConfig:
    airnow_window: str
    airnow_parameters: tuple[str, ...]
    airnow_base_url: str
    airnow_api_url: str
    epa_aqs_year: str
    epa_aqs_parameters: tuple[str, ...]
    epa_aqs_base_url: str
    overture_release: str
    overture_s3_uri: str
    svi_year: int
    svi_url: str
    acs_year: int
    acs_product: str
    aoi: AoiConfig


def load_release_config(path: Path = DEFAULT_RELEASE_YAML) -> ReleaseConfig:
    raw = yaml.safe_load(path.read_text())
    aoi_raw = raw["aoi"]
    aoi = AoiConfig(
        name=str(aoi_raw["name"]),
        bbox=tuple(float(x) for x in aoi_raw["bbox"]),  # type: ignore[arg-type]
        state_fips=str(aoi_raw["state_fips"]),
        state_abbr=str(aoi_raw["state_abbr"]),
    )
    return ReleaseConfig(
        airnow_window=str(raw["airnow"]["window"]),
        airnow_parameters=tuple(str(p) for p in raw["airnow"]["parameters"]),
        airnow_base_url=str(raw["airnow"]["base_url"]),
        airnow_api_url=str(raw["airnow"]["api_url"]),
        epa_aqs_year=str(raw["epa_aqs"]["year"]),
        epa_aqs_parameters=tuple(str(p) for p in raw["epa_aqs"]["parameters"]),
        epa_aqs_base_url=str(raw["epa_aqs"]["base_url"]),
        overture_release=str(raw["overture"]["release"]),
        overture_s3_uri=str(raw["overture"]["s3_uri"]),
        svi_year=int(raw["cdc_svi"]["year"]),
        svi_url=str(raw["cdc_svi"]["url"]),
        acs_year=int(raw["census_acs"]["year"]),
        acs_product=str(raw["census_acs"]["product"]),
        aoi=aoi,
    )


def raw_bucket(project_id: str, bucket_prefix: str | None = None) -> str:
    prefix = bucket_prefix or f"{project_id}-airhealth"
    return f"gs://{prefix}-raw"


def silver_bucket(project_id: str, bucket_prefix: str | None = None) -> str:
    """Medallion silver-zone bucket — Spark-enriched GeoParquet lands here."""
    prefix = bucket_prefix or f"{project_id}-airhealth"
    return f"gs://{prefix}-silver"


def gcloud() -> str:
    path = shutil.which("gcloud")
    if not path:
        raise RuntimeError("gcloud CLI not found on PATH")
    return path


def gcs_object_exists(uri: str) -> bool:
    """Return True iff `gs://bucket/object` exists. Uses gcloud storage ls."""
    if not uri.startswith("gs://"):
        raise ValueError(f"expected gs:// URI, got {uri!r}")
    proc = subprocess.run(
        [gcloud(), "storage", "ls", uri],
        capture_output=True, text=True, check=False,
    )
    return proc.returncode == 0 and uri.rstrip("/") in proc.stdout


def gcs_upload(local: Path, gcs_uri: str, *, recursive: bool = False) -> None:
    """gcloud storage cp local -> gs:// path. Raises CalledProcessError on failure."""
    cmd = [gcloud(), "storage", "cp"]
    if recursive:
        cmd.append("--recursive")
    cmd += [str(local), gcs_uri]
    subprocess.run(cmd, check=True)


def http_user_agent() -> str:
    """Be a good citizen — identify ourselves on AirNow / EPA fetches."""
    return os.environ.get(
        "AIRHEALTH_USER_AGENT",
        "airhealth-ingest/0.1 (+https://github.com/shaoyinz/UrbanAirHealth)",
    )
