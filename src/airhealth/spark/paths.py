"""Raw-zone input + silver-zone output URIs for the Sedona silver job.

Reuses the ingest modules' own builders for raw paths so the raw layout
has a single source of truth — change a path in ``airhealth.ingest`` and
this job follows. Silver layout is defined here, since the silver job is
what creates it.
"""

from __future__ import annotations

from airhealth.ingest import airnow, overture


def overture_input(raw_uri: str, release: str, aoi_name: str) -> str:
    """Overture buildings GeoParquet written by ``airhealth.ingest.overture``."""
    return overture.gcs_target_uri(raw_uri, release, aoi_name)


def airnow_input_glob(raw_uri: str, window: str, aoi_name: str) -> str:
    """Glob across every day-partitioned AirNow parquet for a window/AOI.

    ``airnow.gcs_target_uri`` builds one URI per date; the silver job
    reads them all at once, so we derive the shared directory and append
    a recursive glob across the ``date=YYYYMMDD`` partitions.
    """
    sample = airnow.gcs_target_uri(raw_uri, window, aoi_name, _PLACEHOLDER_DATE)
    # .../airnow/window=W/aoi=NAME/date=YYYYMMDD/observations.parquet
    # → .../airnow/window=W/aoi=NAME/date=*/observations.parquet
    head, _, _ = sample.rpartition("date=")
    return f"{head}date=*/observations.parquet"


def silver_buildings_output(silver_uri: str, release: str, aoi_name: str) -> str:
    """Release-tagged silver-zone directory for enriched building GeoParquet.

    Keyed on the Overture release (not the AirNow window): the building
    set is what a re-ingest changes. AirNow window is a column on the
    output rows, so a window bump rewrites in place under the same path.
    """
    return (
        f"{silver_uri.rstrip('/')}/buildings"
        f"/release={release}/aoi={aoi_name}"
    )


# Sentinel used only by ``airnow_input_glob`` to invoke the ingest URI
# builder; the date value never appears in the returned glob.
from datetime import date as _date  # noqa: E402

_PLACEHOLDER_DATE = _date(2000, 1, 1)
