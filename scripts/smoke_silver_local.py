"""Local Sedona smoke test for ``airhealth.spark.build_silver``.

Stages the Phase-1 fixtures under ``data/raw/`` into a temp directory
whose subpaths mirror the GCS raw-zone layout, then invokes
``build_silver.build`` against that temp dir with ``--local`` mode
(pyspark pulls Sedona JARs from Maven Central). Output GeoParquet
lands in a sibling ``silver/`` under the same temp dir; we read it
back, print a 5-row peek + per-cause DALY totals so the operator can
eyeball-compare against Phase 1's notebook output.

Usage::

    bash scripts/fix_pyspark_py314.sh        # one-time, py314 cloudpickle
    PYTHONPATH=src python scripts/smoke_silver_local.py [--limit 5000]

Default ``--limit 5000`` keeps the run under ~30 s on a laptop; pass
``--limit 0`` for the full 3 M-row LA-basin slice (~10 min).

Why not pytest? The full silver job pulls a real pyspark session and
Sedona JARs from Maven Central on first run (~1 min Maven fetch
cached thereafter). That cost belongs in a developer-driven smoke
script, not the unit-test default. We *also* have unit tests for the
math in ``tests/unit/test_dalys.py`` — this script proves the
Sedona-side wiring (geometry decode, H3 cell IDs, mapInPandas
serialization, GeoParquet writer) over a real LA-basin slice.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

# Resolve repo root and put src/ on sys.path so we can import airhealth.*
# without installing the package. Matches the convention used in the
# Phase-1 ingest CLIs (README §Quickstart).
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from airhealth.ingest._common import (  # noqa: E402
    DEFAULT_RELEASE_YAML,
    load_release_config,
)
from airhealth.scoring.dalys import load_concentration_response  # noqa: E402
from airhealth.spark import paths  # noqa: E402
from airhealth.spark.build_silver import build, SILVER_COLUMNS  # noqa: E402
from airhealth.spark.session import sedona_session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


PHASE1_AIRNOW_DIR = REPO_ROOT / "data" / "raw" / "airnow"
PHASE1_OVERTURE_GLOB = "overture_*.parquet*"
PHASE1_OVERTURE_DIR = REPO_ROOT / "data" / "raw"


def _parse_airnow_date(filename: str) -> date:
    """``airnow_la_basin_20250103.parquet[.uploaded]`` → 2025-01-03."""
    stem = filename.split(".")[0]                      # drop .parquet[.uploaded]
    yyyymmdd = stem.rsplit("_", 1)[-1]
    return datetime.strptime(yyyymmdd, "%Y%m%d").date()


def stage_raw_zone(raw_dir: Path, window: str, aoi_name: str, overture_release: str) -> None:
    """Copy/symlink Phase-1 staged fixtures into the GCS-mirror layout.

    The silver job reads from ``<raw>/airnow/window=W/aoi=A/date=D/observations.parquet``
    and ``<raw>/overture/release=R/aoi=A/buildings.parquet`` regardless of
    whether ``<raw>`` is a ``gs://`` URI or a plain filesystem path. We
    symlink rather than copy — the Overture parquet is ~550 MB.
    """
    # AirNow: one parquet per date partition. Tolerate both .parquet and
    # the post-upload .parquet.uploaded suffix the Phase-1 ingest CLIs
    # rename to.
    airnow_files = sorted(
        list(PHASE1_AIRNOW_DIR.glob("airnow_*.parquet"))
        + list(PHASE1_AIRNOW_DIR.glob("airnow_*.parquet.uploaded"))
    )
    if not airnow_files:
        raise SystemExit(f"no AirNow fixtures under {PHASE1_AIRNOW_DIR}")

    days_staged: list[date] = []
    for src in airnow_files:
        d = _parse_airnow_date(src.name)
        dst_dir = (
            raw_dir
            / "airnow"
            / f"window={window}"
            / f"aoi={aoi_name}"
            / f"date={d.strftime('%Y%m%d')}"
        )
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "observations.parquet"
        if not dst.exists():
            dst.symlink_to(src.resolve())
        days_staged.append(d)
    print(f"  staged AirNow: {len(days_staged)} days "
          f"({min(days_staged)} → {max(days_staged)})")

    # Overture: a single parquet for the AOI slice.
    candidates = sorted(PHASE1_OVERTURE_DIR.glob(PHASE1_OVERTURE_GLOB))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        raise SystemExit(f"no Overture fixture under {PHASE1_OVERTURE_DIR}")
    src = candidates[0]  # only one per AOI; take it
    dst_dir = (
        raw_dir
        / "overture"
        / f"release={overture_release}"
        / f"aoi={aoi_name}"
    )
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "buildings.parquet"
    if not dst.exists():
        dst.symlink_to(src.resolve())
    print(f"  staged Overture: {src.name} → {dst}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Cap building count for a smoke test. 0 = no limit (~3M rows).",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="Spark shuffle width; 8 is plenty for a laptop smoke.",
    )
    parser.add_argument(
        "--min-completeness",
        type=float,
        default=0.0,
        help=(
            "Phase-1 fixtures stage only a handful of AirNow days, so "
            "completeness is ≪ EPA's 0.75 bar — default 0.0 here to keep "
            "the smoke usable. Production runs use the build_silver default."
        ),
    )
    parser.add_argument(
        "--keep-tmp",
        action="store_true",
        help="don't delete the temp raw/silver dir on exit (debug)",
    )
    parser.add_argument(
        "--peek-rows",
        type=int,
        default=5,
        help="print this many silver rows after the write",
    )
    args = parser.parse_args(argv)

    cfg = load_release_config(DEFAULT_RELEASE_YAML)
    cr = load_concentration_response(
        DEFAULT_RELEASE_YAML.parent / "concentration_response.yaml"
    )

    tmp = Path(tempfile.mkdtemp(prefix="airhealth-silver-smoke-"))
    raw_dir = tmp / "raw"
    silver_dir = tmp / "silver"
    raw_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    print(f"tmp dir: {tmp}")
    try:
        stage_raw_zone(
            raw_dir,
            window=cfg.airnow_window,
            aoi_name=cfg.aoi.name,
            overture_release=cfg.overture_release,
        )

        sedona = sedona_session("airhealth-silver-smoke", local_packages=True)
        try:
            out_uri = build(
                sedona,
                str(raw_dir),
                str(silver_dir),
                cfg,
                cr,
                partitions=args.partitions,
                limit=(args.limit or None),
                min_completeness=args.min_completeness,
            )
            print(f"  wrote -> {out_uri}")

            # Read silver back, print a peek and the per-cause DALY total.
            silver = sedona.read.format("geoparquet").load(out_uri)
            silver.printSchema()
            silver.select(
                "id", "centroid_lon", "centroid_lat",
                "pm25_annual_mean", "monitor_distance_km", "daly_total",
            ).show(args.peek_rows, truncate=False)
            agg = silver.agg(
                *[
                    F.sum(c).alias(c)
                    for c in [
                        "population", "daly_total",
                        "daly_ihd", "daly_stroke", "daly_copd",
                        "daly_lung_cancer", "daly_lri",
                    ]
                ]
            ).collect()[0].asDict()
            print("  aggregate (sum across smoke slice):")
            for k, v in agg.items():
                print(f"    {k:<22} = {v:,.4f}" if v is not None else f"    {k:<22} = NULL")
            # Smoke-pass: every silver row should have every advertised column.
            missing = set(SILVER_COLUMNS) - set(silver.columns)
            if missing:
                print(f"  ! missing columns: {sorted(missing)}", file=sys.stderr)
                return 2
        finally:
            sedona.stop()
    finally:
        if args.keep_tmp:
            print(f"  --keep-tmp set; left {tmp} on disk")
        else:
            shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
