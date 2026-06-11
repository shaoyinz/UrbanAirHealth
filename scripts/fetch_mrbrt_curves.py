"""Convert IHME GBD 2021 MR-BRT risk-curve draws into per-cause parquets.

The MR-BRT splines that ``config/concentration_response.yaml`` references
ship as a draws CSV on IHME's GHDx (Global Health Data Exchange):

    https://ghdx.healthdata.org/   (free account required)

The exact record at the time of writing:

    "Global Burden of Disease Study 2021 (GBD 2021) Air Pollution
    Exposure Estimates and Risk Curves" — particulate-matter risk-curve
    bundle, CSV format. (Newer GBD releases publish at the same URL; the
    column layout has been stable across 2019/2021.)

The downloaded CSV has roughly one row per (cause, exposure_µg/m³, draw)
with 1000 posterior draws per exposure bin per cause. This script
collapses draws → mean RR(z) and writes one tiny parquet per cause to
``config/cr_curves/gbd2021/<cause>.parquet`` so the parquet sits next to
the YAML that references it.

Usage:

    python scripts/fetch_mrbrt_curves.py \\
        --input ~/Downloads/gbd2021_pm25_risk_curves.csv \\
        --outdir config/cr_curves/gbd2021

The CSV column names in the IHME release have varied slightly across
years; pass ``--cause-col``, ``--exposure-col``, ``--rr-col`` if the
defaults below don't match. ``--list-causes`` prints the cause-name
inventory of the input CSV without writing anything, which is the
simplest way to discover the exact strings IHME used in this release.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Mapping from the canonical cause keys used in our YAML/config to the
# substrings the IHME CSV typically uses. Matching is case-insensitive
# substring; override with --cause-map if a future release renames a
# cause.
# Patterns include both the human-readable cause names (used by some
# combined-CSV releases) and the GBD `label`/file-name codes used by the
# GBD 2019 multi-file summary bundle (cvd_ihd_*, cvd_stroke_*, resp_copd,
# neo_lung, lri, t2_dm). Matching is case-insensitive substring.
DEFAULT_CAUSE_MAP: dict[str, list[str]] = {
    "ihd": ["ischemic heart", "cvd_ihd", "ihd"],
    "stroke": ["cvd_stroke", "stroke", "cerebrovascular"],
    "copd": ["resp_copd", "copd", "chronic obstructive"],
    "lung_cancer": ["neo_lung", "lung cancer", "tracheal", "tbl"],
    "lri": ["lower respiratory", "lri"],
    "t2d": ["t2_dm", "diabetes", "t2dm", "type 2"],
}


def _match_cause(label: str, patterns: list[str]) -> bool:
    label_l = label.lower()
    return any(p.lower() in label_l for p in patterns)


def _resolve_columns(
    df: pd.DataFrame,
    cause_col: str | None,
    exposure_col: str | None,
    rr_col: str | None,
) -> tuple[str, str, str]:
    """Auto-detect column names when the user hasn't pinned them."""
    cols = {c.lower(): c for c in df.columns}

    def _pick(candidate_keys: list[str], explicit: str | None, label: str) -> str:
        if explicit:
            if explicit not in df.columns:
                raise ValueError(f"--{label}-col {explicit!r} not in CSV columns: {list(df.columns)}")
            return explicit
        for k in candidate_keys:
            if k in cols:
                return cols[k]
        raise ValueError(
            f"could not auto-detect {label} column among {list(df.columns)} — "
            f"pass --{label}-col explicitly"
        )

    return (
        _pick(["cause", "cause_name", "outcome"], cause_col, "cause"),
        _pick(["exposure", "exposure_ugm3", "pm25", "z"], exposure_col, "exposure"),
        _pick(["rr", "rr_mean", "mean", "value"], rr_col, "rr"),
    )


def collapse_curves(
    df: pd.DataFrame,
    *,
    cause_col: str,
    exposure_col: str,
    rr_col: str,
) -> dict[str, pd.DataFrame]:
    """Reduce raw rows to one mean RR(z) curve per cause key.

    Two reductions happen here:

    * Over draws — a draws release has many rows per (cause, exposure);
      grouping on exposure and taking the mean collapses them. A
      pre-collapsed summary release already has one row per
      (cause, exposure), so the groupby is a no-op.
    * Over age groups — GBD 2019 ships IHD and stroke as 15 age-specific
      curves each (``cvd_ihd_25`` … ``cvd_ihd_95``). All 15 match the same
      cause key, and we average them **unweighted** into a single
      all-ages curve. This is a Phase-1 simplification: GBD itself
      estimates age-specific PAFs and death-weights them when aggregating
      burden. A death-weighted collapse is the Phase-4 refinement. The
      other four PM2.5 causes have exactly one source curve, so the
      average is an identity.
    """
    matched: dict[str, list[pd.DataFrame]] = {k: [] for k in DEFAULT_CAUSE_MAP}
    sources: dict[str, list[str]] = {k: [] for k in DEFAULT_CAUSE_MAP}
    for cause_label, group in df.groupby(cause_col):
        for key, patterns in DEFAULT_CAUSE_MAP.items():
            if _match_cause(str(cause_label), patterns):
                tidy = (
                    group.groupby(exposure_col, as_index=False)[rr_col]
                    .mean()
                    .rename(columns={exposure_col: "pm25_ugm3", rr_col: "rr_mean"})
                )
                matched[key].append(tidy)
                sources[key].append(str(cause_label))
                break

    out: dict[str, pd.DataFrame] = {}
    for key, curves in matched.items():
        if not curves:
            continue
        # Concatenate the (one or many) source curves and average RR at
        # each shared exposure value — the age-group collapse for CVD.
        tidy = (
            pd.concat(curves, ignore_index=True)
            .groupby("pm25_ugm3", as_index=False)["rr_mean"]
            .mean()
            .sort_values("pm25_ugm3")
            .reset_index(drop=True)
        )
        tidy["pm25_ugm3"] = tidy["pm25_ugm3"].astype("float64")
        tidy["rr_mean"] = tidy["rr_mean"].astype("float64")
        out[key] = tidy
        if len(curves) > 1:
            print(
                f"  note: {key} averaged (unweighted) from {len(curves)} "
                f"age-specific curves: {', '.join(sorted(sources[key]))}",
                file=sys.stderr,
            )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--input",
        type=Path,
        help="path to an IHME risk-curve CSV, or a directory of per-cause "
        "CSVs (e.g. the unzipped GBD 2019 PM_RISK_SUMM bundle)",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("config/cr_curves/gbd2021"),
        help="destination directory for per-cause parquets",
    )
    parser.add_argument("--cause-col", default=None)
    parser.add_argument("--exposure-col", default=None)
    parser.add_argument("--rr-col", default=None)
    parser.add_argument(
        "--list-causes",
        action="store_true",
        help="print the unique cause labels in the CSV and exit",
    )
    args = parser.parse_args(argv)

    if args.input is None:
        parser.error("--input is required (download the CSV from GHDx first)")
    if not args.input.exists():
        parser.error(f"input not found: {args.input}")

    if args.input.is_dir():
        csvs = sorted(
            p for ext in ("*.CSV", "*.csv") for p in args.input.glob(ext)
        )
        if not csvs:
            parser.error(f"no CSV files found in directory {args.input}")
        df = pd.concat((pd.read_csv(p) for p in csvs), ignore_index=True)
    else:
        df = pd.read_csv(args.input)
    cause_col, exposure_col, rr_col = _resolve_columns(
        df, args.cause_col, args.exposure_col, args.rr_col
    )

    if args.list_causes:
        for label in sorted(df[cause_col].astype(str).unique()):
            print(label)
        return 0

    curves = collapse_curves(
        df, cause_col=cause_col, exposure_col=exposure_col, rr_col=rr_col
    )
    missing = set(DEFAULT_CAUSE_MAP) - set(curves)
    if missing:
        print(
            f"warning: no curve matched for {sorted(missing)} — "
            f"check label spelling or extend DEFAULT_CAUSE_MAP",
            file=sys.stderr,
        )

    args.outdir.mkdir(parents=True, exist_ok=True)
    for key, tidy in curves.items():
        out_path = args.outdir / f"{key}.parquet"
        table = pa.Table.from_pandas(tidy, preserve_index=False)
        pq.write_table(table, out_path, compression="snappy")
        print(f"  {key:12s} -> {out_path}  ({len(tidy)} bins, max_z={tidy['pm25_ugm3'].max():.1f})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
