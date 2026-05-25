"""Ingest Overture building footprints into the raw zone (AOI-scoped).

Lifted from ``floodpipe.ingest.overture``; the only material change is
that the bbox now comes from ``config/release.yaml`` AOI section rather
than a hardcoded FLORIDA_BBOX constant, so retargeting (LA basin → SF
Bay → CONUS) is a one-line config edit, not a code change.

DuckDB + spatial + httpfs reads the pinned Overture release directly
from public S3, with a bbox filter pushed down via the ``bbox`` struct
that every Overture row carries. The result is staged as a single local
parquet, then uploaded to::

    gs://<raw>/overture/release=<R>/aoi=<NAME>/buildings.parquet
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

# duckdb is only needed inside the CLI ``main`` (S3→GeoParquet slice).
# Keep it function-local so ``airhealth.spark.build_silver`` can import
# ``gcs_target_uri`` from this module without dragging duckdb onto the
# Dataproc Serverless image.
if TYPE_CHECKING:
    import duckdb  # noqa: F401

from airhealth.ingest._common import (
    AoiConfig,
    ReleaseConfig,
    gcs_object_exists,
    gcs_upload,
    load_release_config,
    raw_bucket,
)


def overture_s3_glob(release: str) -> str:
    return (
        f"s3://overturemaps-us-west-2/release/{release}"
        "/theme=buildings/type=building/*"
    )


def gcs_target_uri(raw_uri: str, release: str, aoi_name: str) -> str:
    return (
        f"{raw_uri.rstrip('/')}/overture/release={release}"
        f"/aoi={aoi_name}/buildings.parquet"
    )


def _run_query(
    con: "duckdb.DuckDBPyConnection",
    s3_glob: str,
    bbox: tuple[float, float, float, float],
    out_path: Path,
) -> None:
    min_lon, min_lat, max_lon, max_lat = bbox
    con.execute(f"""
        COPY (
            SELECT
                id,
                names.primary AS name,
                class,
                subtype,
                num_floors,
                height,
                bbox,
                geometry
            FROM read_parquet('{s3_glob}', filename=false, hive_partitioning=1)
            WHERE bbox.xmin <= {max_lon}
              AND bbox.xmax >= {min_lon}
              AND bbox.ymin <= {max_lat}
              AND bbox.ymax >= {min_lat}
        )
        TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-id", required=True, help="GCP project ID owning the raw bucket")
    parser.add_argument("--bucket-prefix", default=None, help="Override bucket name prefix")
    parser.add_argument("--stage-dir", default="data/raw", help="Local staging dir (gitignored)")
    parser.add_argument("--force", action="store_true", help="Re-upload even if target exists")
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do nothing")
    args = parser.parse_args(argv)

    cfg: ReleaseConfig = load_release_config()
    aoi: AoiConfig = cfg.aoi
    raw_uri = raw_bucket(args.project_id, args.bucket_prefix)
    target_uri = gcs_target_uri(raw_uri, cfg.overture_release, aoi.name)
    s3_glob = overture_s3_glob(cfg.overture_release)

    print(f"  release: {cfg.overture_release}")
    print(f"  source:  {s3_glob}")
    print(f"  aoi:     {aoi.name}  bbox={aoi.bbox}")
    print(f"  target:  {target_uri}")

    if args.dry_run:
        return 0

    if not args.force and gcs_object_exists(target_uri):
        print("  exists — skipping (re-run with --force to overwrite)")
        return 0

    stage_dir = Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    stage_path = stage_dir / f"overture_{aoi.name}_{cfg.overture_release}.parquet"

    import duckdb  # local import: ingest-CLI dep only.
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("PRAGMA threads=8;")

    print(f"  querying -> {stage_path}")
    _run_query(con, s3_glob, aoi.bbox, stage_path)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{stage_path}')").fetchone()[0]
    print(f"  staged:  {n:,} buildings, {stage_path.stat().st_size / 1e9:.2f} GB")

    print(f"  uploading -> {target_uri}")
    gcs_upload(stage_path, target_uri)
    print("  done")

    if not args.force:
        shutil.move(str(stage_path), str(stage_path.with_suffix(".parquet.uploaded")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
