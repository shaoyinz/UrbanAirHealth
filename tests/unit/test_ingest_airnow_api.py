"""Unit tests for the Phase-3 AirNow aq/data API ingest path.

These cover the pure normalization (`_normalize_api_records`) — no network
and no API key — which is where every schema-parity and filtering decision
lives. The HTTP/GCS orchestration (`ingest_api_day`) wraps this with a
fetch + idempotency check that needs a live key, so it's left for a future
integration test; here we guard that whatever the API returns lands in the
exact schema the silver job's `features.exposure.daily_mean` consumes.
"""

from __future__ import annotations

import pandas as pd

from airhealth.features.exposure import parse_airnow_timestamps
from airhealth.ingest.airnow import (
    NORMALIZED_DTYPES,
    _api_parameter,
    _normalize_api_records,
)

# LA-basin bbox (W, S, E, N), matching config/release.yaml.
BBOX = (-118.95, 33.70, -117.65, 34.35)
PARAMS = {"PM2.5"}


def _record(**kw) -> dict:
    """An aq/data verbose-JSON concentration row with sane defaults."""
    base = {
        "Latitude": 34.05,
        "Longitude": -118.24,
        "UTC": "2025-06-01T05:00",
        "Parameter": "PM2.5",
        "Unit": "UG/M3",
        "Value": 12.3,
        "SiteName": "Los Angeles - N. Main",
        "FullAQSCode": "060371103",
        "IntlAQSCode": "840060371103",
    }
    base.update(kw)
    return base


def test_api_parameter_code():
    assert _api_parameter("PM2.5") == "PM25"
    assert _api_parameter("PM10") == "PM10"


def test_normalize_happy_path_schema_and_values():
    out = _normalize_api_records([_record()], PARAMS, BBOX)

    assert list(out.columns) == list(NORMALIZED_DTYPES)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["aqsid"] == "060371103"          # FullAQSCode preferred
    assert row["valid_date"] == "06/01/25"      # %m/%d/%y, UTC
    assert row["valid_time"] == "05:00"         # %H:%M
    assert row["value"] == 12.3
    assert row["data_source"] == "airnow_api"

    # The normalized frame must round-trip through the downstream parser
    # that the silver reducer calls — this is the real contract.
    parsed = parse_airnow_timestamps(out)
    assert str(parsed["date_utc"].iloc[0]) == "2025-06-01"


def test_normalize_drops_sentinel_param_and_bbox_misses():
    records = [
        _record(),                                   # keep
        _record(Value=-999.0),                       # missing sentinel → drop
        _record(Parameter="OZONE"),                  # wrong parameter → drop
        _record(Latitude=40.0, Longitude=-118.24),   # north of bbox → drop
        _record(Longitude=-120.0),                   # west of bbox → drop
    ]
    out = _normalize_api_records(records, PARAMS, BBOX)
    assert len(out) == 1
    assert out.iloc[0]["value"] == 12.3


def test_normalize_empty_returns_typed_empty_frame():
    out = _normalize_api_records([], PARAMS, BBOX)
    assert out.empty
    assert list(out.columns) == list(NORMALIZED_DTYPES)


def test_normalize_falls_back_to_concentration_column():
    # Some aq/data variants label the value column 'Concentration'.
    rec = _record()
    del rec["Value"]
    rec["Concentration"] = 9.9
    out = _normalize_api_records([rec], PARAMS, BBOX)
    assert out.iloc[0]["value"] == 9.9
