"""H3 multi-resolution indexing for building centroids.

We tag every building with H3 cell IDs at resolutions 6–9 so the
dashboard can roll DALYs up or down without re-querying the fact table.

Resolution reference (avg edge length, area):
    r6  ~ 3.7 km edge,  36 km²   — county-scale heatmap
    r7  ~ 1.4 km edge,   5 km²   — neighborhood
    r8  ~ 530 m edge,   0.74 km² — block group; default partition key
    r9  ~ 200 m edge,   0.10 km² — block / facility cluster

H3 r7 is the Spark partition key for the silver job (matches
UrbanFloodRisk); we keep r6/r8/r9 around for visualization joins.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

H3_RESOLUTIONS: tuple[int, ...] = (6, 7, 8, 9)


def _latlng_to_cell(lat: float, lng: float, res: int) -> str:
    # Local import so a missing h3 doesn't break `airhealth.features`
    # import for callers that only need exposure roll-ups.
    import h3

    return h3.latlng_to_cell(lat, lng, res)


def assign_h3_cells(
    df: pd.DataFrame,
    *,
    lat_col: str = "lat",
    lon_col: str = "lon",
    resolutions: tuple[int, ...] = H3_RESOLUTIONS,
) -> pd.DataFrame:
    """Add `h3_r{res}` columns to `df` (one per resolution).

    Operates row-wise in Python — H3's vectorized API isn't stable across
    minor versions and per-building cost is ~µs. For Phase 1 LA-basin
    scale (~3 M rows) this is ~seconds. Phase 2 Sedona swaps for the
    JVM `ST_H3CellIDs` call inside the silver job.
    """
    import h3  # fail fast with a clear ImportError before the loop.

    if df.empty:
        out = df.copy()
        for r in resolutions:
            out[f"h3_r{r}"] = pd.Series(dtype="string")
        return out

    lat = df[lat_col].to_numpy()
    lon = df[lon_col].to_numpy()
    bad = np.isnan(lat) | np.isnan(lon)

    out = df.copy()
    for r in resolutions:
        col = np.empty(len(df), dtype=object)
        for i in range(len(df)):
            col[i] = None if bad[i] else h3.latlng_to_cell(float(lat[i]), float(lon[i]), r)
        out[f"h3_r{r}"] = pd.array(col, dtype="string")
    return out
