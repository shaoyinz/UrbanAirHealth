"""Unit tests for exposure roll-ups (hourly → daily → annual).

These guard the pure-pandas primitives the Phase 1 notebook calls and
the Phase 2 Sedona pandas UDF will call unchanged.
"""

from __future__ import annotations

import pandas as pd
import pytest

from airhealth.features.exposure import (
    HOURS_PER_YEAR,
    annual_metrics,
    completeness,
    daily_mean,
    parse_airnow_timestamps,
)


def _hourly_row(aqsid: str, date: str, hour: int, value: float) -> dict:
    """One AirNow-format row, mirroring the staged parquet schema."""
    return {
        "aqsid": aqsid,
        "valid_date": date,                   # 'MM/DD/YY'
        "valid_time": f"{hour:02d}:00",
        "parameter": "PM2.5",
        "value": value,
        "lat": 34.0,
        "lon": -118.0,
    }


# ---------------------------------------------------------------------------
# parse_airnow_timestamps
# ---------------------------------------------------------------------------


def test_parse_timestamps_combines_split_fields():
    df = pd.DataFrame([_hourly_row("A", "01/02/25", 7, 12.0)])
    out = parse_airnow_timestamps(df)
    assert out["ts_utc"].iloc[0] == pd.Timestamp("2025-01-02 07:00:00")
    assert out["date_utc"].iloc[0].isoformat() == "2025-01-02"


# ---------------------------------------------------------------------------
# daily_mean
# ---------------------------------------------------------------------------


def test_daily_mean_averages_hours_per_station():
    df = pd.DataFrame([
        _hourly_row("A", "01/01/25", 0, 10.0),
        _hourly_row("A", "01/01/25", 1, 20.0),
        _hourly_row("A", "01/01/25", 2, 30.0),
        _hourly_row("B", "01/01/25", 0, 5.0),
    ])
    out = daily_mean(df).sort_values("aqsid").reset_index(drop=True)
    assert out.loc[0, "aqsid"] == "A"
    assert out.loc[0, "value"] == pytest.approx(20.0)
    assert out.loc[0, "n_hours"] == 3
    assert out.loc[1, "aqsid"] == "B"
    assert out.loc[1, "value"] == pytest.approx(5.0)
    assert out.loc[1, "n_hours"] == 1


def test_daily_mean_drops_null_values_before_averaging():
    """One bad hour shouldn't contaminate the day mean — but the count
    should reflect only the kept hours."""
    df = pd.DataFrame([
        _hourly_row("A", "01/01/25", 0, 10.0),
        _hourly_row("A", "01/01/25", 1, float("nan")),
        _hourly_row("A", "01/01/25", 2, 30.0),
    ])
    out = daily_mean(df)
    assert out["value"].iloc[0] == pytest.approx(20.0)
    assert out["n_hours"].iloc[0] == 2


def test_daily_mean_splits_by_date():
    df = pd.DataFrame([
        _hourly_row("A", "01/01/25", 0, 10.0),
        _hourly_row("A", "01/02/25", 0, 20.0),
    ])
    out = daily_mean(df).sort_values("date_utc").reset_index(drop=True)
    assert len(out) == 2
    assert out["value"].tolist() == [10.0, 20.0]


# ---------------------------------------------------------------------------
# completeness
# ---------------------------------------------------------------------------


def test_completeness_is_hours_over_year():
    """Completeness uses the hourly grain (n_hours / 8760), not days,
    to match EPA AQS's "valid station-year" rule."""
    daily = pd.DataFrame([
        {"aqsid": "A", "date_utc": pd.Timestamp("2025-01-01").date(),
         "value": 10.0, "n_hours": 24, "lat": 34.0, "lon": -118.0},
        {"aqsid": "A", "date_utc": pd.Timestamp("2025-01-02").date(),
         "value": 12.0, "n_hours": 12, "lat": 34.0, "lon": -118.0},
    ])
    comp = completeness(daily)
    assert comp.loc[0, "n_hours"] == 36
    assert comp.loc[0, "completeness"] == pytest.approx(36 / HOURS_PER_YEAR)


def test_completeness_year_filter_drops_other_years():
    daily = pd.DataFrame([
        {"aqsid": "A", "date_utc": pd.Timestamp("2024-12-31").date(),
         "value": 1.0, "n_hours": 24, "lat": 0.0, "lon": 0.0},
        {"aqsid": "A", "date_utc": pd.Timestamp("2025-01-01").date(),
         "value": 1.0, "n_hours": 24, "lat": 0.0, "lon": 0.0},
    ])
    comp = completeness(daily, year=2025)
    assert comp.loc[0, "n_hours"] == 24


def test_completeness_empty_input_returns_empty_frame():
    out = completeness(pd.DataFrame())
    assert list(out.columns) == ["aqsid", "lat", "lon", "n_hours", "completeness"]
    assert out.empty


# ---------------------------------------------------------------------------
# annual_metrics
# ---------------------------------------------------------------------------


def _synthetic_daily(aqsid: str, values: list[float], start: str = "2025-01-01") -> pd.DataFrame:
    dates = pd.date_range(start, periods=len(values), freq="D")
    return pd.DataFrame({
        "aqsid": aqsid,
        "date_utc": [d.date() for d in dates],
        "value": values,
        "n_hours": [24] * len(values),
        "lat": [34.0] * len(values),
        "lon": [-118.0] * len(values),
    })


def test_annual_mean_and_p98_match_pandas():
    """`annual_mean` is the straight mean of daily means; `p98_day` is
    pandas .quantile(0.98) on the same series."""
    vals = [float(v) for v in range(1, 101)]  # 1..100
    daily = _synthetic_daily("A", vals)
    out = annual_metrics(daily)
    assert out.loc[0, "annual_mean"] == pytest.approx(50.5)
    # Pandas linear interpolation: 98th percentile of 1..100.
    assert out.loc[0, "p98_day"] == pytest.approx(pd.Series(vals).quantile(0.98))


def test_peak_week_is_max_of_rolling_7():
    """Peak week is the max 7-day rolling mean — robust to a single
    spiked day. Spike at the end, surrounded by zeros."""
    vals = [0.0] * 20 + [70.0] + [0.0] * 6  # spike on day 21
    daily = _synthetic_daily("A", vals)
    out = annual_metrics(daily)
    # Any 7-day window containing the spike averages 70/7 = 10.
    assert out.loc[0, "peak_week_mean"] == pytest.approx(10.0)


def test_annual_metrics_separates_stations():
    daily = pd.concat([
        _synthetic_daily("A", [10.0] * 30),
        _synthetic_daily("B", [20.0] * 30),
    ], ignore_index=True)
    out = annual_metrics(daily).sort_values("aqsid").reset_index(drop=True)
    assert out["aqsid"].tolist() == ["A", "B"]
    assert out["annual_mean"].tolist() == [pytest.approx(10.0), pytest.approx(20.0)]


def test_annual_metrics_empty_returns_empty_frame():
    out = annual_metrics(pd.DataFrame(columns=["aqsid", "date_utc", "value", "n_hours", "lat", "lon"]))
    assert out.empty
    assert "annual_mean" in out.columns
