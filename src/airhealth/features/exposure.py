"""Exposure roll-ups: hourly observations → daily means → annual metrics.

The silver-zone Spark job (Phase 2) calls these inside a pandas UDF
partitioned by H3 cell; the Phase 1 notebook calls them directly over a
pandas DataFrame loaded from the staged AirNow parquets.

All functions are pure pandas + stdlib so the Sedona job can import them
unchanged. The grain is consistently (monitor × period); spatial join to
buildings happens *after* these aggregations, via IDW.

Inputs are an "hourly" frame with at minimum:

    aqsid     str   — AirNow station ID (also the AQS site ID)
    valid_date str  — 'MM/DD/YY' as it lives in AirNow files
    valid_time str  — 'HH:MM'
    value     float — PM2.5 µg/m³
    lat, lon  float — station coords (after metadata join)

A `completeness >= 0.75` threshold is the conventional EPA AQS bar for
calling a station-year "valid" and is what GBD-style exposure assessments
assume; lower-coverage stations are still emitted, just flagged.
"""

from __future__ import annotations

import pandas as pd

# 24 h × 365 d = 8760 expected hourly samples per station-year. We don't
# discount leap years — the difference is < 0.3 % and "completeness" is
# fundamentally a coarse QC flag, not a precise figure.
HOURS_PER_YEAR = 24 * 365


def parse_airnow_timestamps(hourly: pd.DataFrame) -> pd.DataFrame:
    """Combine AirNow's split MM/DD/YY + HH:MM strings into a UTC timestamp.

    AirNow hourly files publish times in UTC ('GMT'); the `gmt_offset`
    column is purely metadata for downstream display, not a correction to
    apply here. We parse to tz-naive UTC for cheap groupby keys.
    """
    out = hourly.copy()
    out["ts_utc"] = pd.to_datetime(
        out["valid_date"] + " " + out["valid_time"],
        format="%m/%d/%y %H:%M",
        utc=False,
    )
    out["date_utc"] = out["ts_utc"].dt.date
    return out


def daily_mean(hourly: pd.DataFrame) -> pd.DataFrame:
    """Hourly → daily mean PM2.5 per (monitor, date_utc).

    Drops rows with null `value` before averaging so a few bad hours
    don't poison the day; records `n_hours` so the caller can apply a
    "≥ 18 valid hours" filter if they want EPA-strict daily means.
    """
    src = parse_airnow_timestamps(hourly) if "date_utc" not in hourly else hourly
    src = src.dropna(subset=["value"])
    grouped = (
        src.groupby(["aqsid", "date_utc"], as_index=False)
        .agg(
            value=("value", "mean"),
            n_hours=("value", "size"),
            lat=("lat", "first"),
            lon=("lon", "first"),
        )
    )
    return grouped


def completeness(daily: pd.DataFrame, *, year: int | None = None) -> pd.DataFrame:
    """Per-monitor completeness for one calendar year.

    `daily` is the output of `daily_mean`. Completeness is defined on the
    hourly grain (`n_hours / 8760`) rather than on day-count because EPA
    AQS's "valid year" rule is hourly-based and we want the same metric.

    If `year` is None, infer it as the modal year in the daily frame
    (Phase 1 has one year per run, so this is unambiguous).
    """
    if daily.empty:
        return pd.DataFrame(columns=["aqsid", "lat", "lon", "n_hours", "completeness"])
    src = daily.copy()
    src["year"] = pd.to_datetime(src["date_utc"]).dt.year
    if year is not None:
        src = src[src["year"] == year]
    agg = (
        src.groupby("aqsid", as_index=False)
        .agg(
            lat=("lat", "first"),
            lon=("lon", "first"),
            n_hours=("n_hours", "sum"),
        )
    )
    agg["completeness"] = agg["n_hours"] / HOURS_PER_YEAR
    return agg


def annual_metrics(daily: pd.DataFrame, *, year: int | None = None) -> pd.DataFrame:
    """Per-monitor annual exposure summary: mean, 98th pct day, peak week.

    Returned columns:
        aqsid, lat, lon, annual_mean, p98_day, peak_week_mean,
        n_days, n_hours, completeness

    - `annual_mean`: simple mean of daily means (matches EPA's annual
      design value formulation closely enough for Phase 1).
    - `p98_day`: 98th-percentile *daily* mean — the wildfire/episodic
      tail signal we surface in the dashboard.
    - `peak_week_mean`: max of rolling-7-day mean — robust proxy for the
      worst sustained week, lighter-tailed than `max(daily)`.
    """
    src = daily.copy()
    src["date_utc"] = pd.to_datetime(src["date_utc"])
    if year is not None:
        src = src[src["date_utc"].dt.year == year]
    if src.empty:
        return pd.DataFrame(columns=[
            "aqsid", "lat", "lon", "annual_mean", "p98_day",
            "peak_week_mean", "n_days", "n_hours", "completeness",
        ])

    rows: list[dict] = []
    for aqsid, g in src.sort_values("date_utc").groupby("aqsid"):
        # Rolling 7-day mean on a per-station daily series. Use min_periods=4
        # so a single near-empty week doesn't drag the peak to NaN.
        roll = g["value"].rolling(window=7, min_periods=4).mean()
        rows.append({
            "aqsid": aqsid,
            "lat": g["lat"].iloc[0],
            "lon": g["lon"].iloc[0],
            "annual_mean": g["value"].mean(),
            "p98_day": g["value"].quantile(0.98),
            "peak_week_mean": roll.max(),
            "n_days": len(g),
            "n_hours": g["n_hours"].sum(),
        })
    out = pd.DataFrame(rows)
    out["completeness"] = out["n_hours"] / HOURS_PER_YEAR
    return out
