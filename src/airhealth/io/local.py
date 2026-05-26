"""Local-disk IO for the Phase 1 prototype.

Phase 1 lives entirely on the operator's laptop — staged AirNow parquets
under `data/raw/airnow/`, one Overture parquet, no GCS round-trips. This
module is the thin seam between those file conventions and the pure
pandas/numpy code in `airhealth.features` and `airhealth.scoring`.

Phase 2 swaps these for the silver-zone Spark reader; the function
*signatures* are kept narrow (return a DataFrame, take a Path) so the
notebook driver doesn't have to change.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from airhealth.ingest._common import REPO_ROOT, AoiConfig

# Conventional layout written by `airhealth.ingest.airnow`. The CLI
# moves uploaded files to `.parquet.uploaded`; for a no-cloud Phase 1
# run we accept both suffixes so the notebook works whether or not the
# operator has run the upload step.
AIRNOW_STAGE_DIR = REPO_ROOT / "data" / "raw" / "airnow"
OVERTURE_STAGE_DIR = REPO_ROOT / "data" / "raw"


def discover_airnow_parquets(
    aoi: AoiConfig, *, stage_dir: Path = AIRNOW_STAGE_DIR
) -> list[Path]:
    """All staged AirNow parquets for one AOI, sorted by filename (date).

    Matches both `airnow_<aoi>_<YYYYMMDD>.parquet` and the
    `.parquet.uploaded` variant the ingest CLI renames to post-upload.
    """
    patterns = [
        f"airnow_{aoi.name}_*.parquet",
        f"airnow_{aoi.name}_*.parquet.uploaded",
    ]
    found: list[Path] = []
    for pat in patterns:
        found.extend(stage_dir.glob(pat))
    return sorted(found)


def read_airnow_window(
    aoi: AoiConfig, *, stage_dir: Path = AIRNOW_STAGE_DIR
) -> pd.DataFrame:
    """Concatenate every staged AirNow day for `aoi` into one DataFrame.

    The ingest step already bbox-filtered and parameter-filtered, so this
    is a straight concat. Empty result is returned as an empty DataFrame
    with the expected columns so downstream code doesn't need to guard.
    """
    paths = discover_airnow_parquets(aoi, stage_dir=stage_dir)
    if not paths:
        return pd.DataFrame(columns=[
            "valid_date", "valid_time", "aqsid", "parameter",
            "value", "lat", "lon",
        ])
    frames = [pd.read_parquet(p) for p in paths]
    return pd.concat(frames, ignore_index=True)


def read_overture_centroids(
    parquet_path: Path,
    *,
    sample: int | None = None,
    seed: int = 0,
) -> pd.DataFrame:
    """Building centroids + light attributes via DuckDB spatial.

    Returns columns: id, class, num_floors, height, area_m2, lon, lat.
    `area_m2` is approximate (planar area on EPSG:4326); accurate enough
    for the per-building occupancy estimate at LA latitudes. Phase 2
    Spark reprojects to an equal-area CRS before the same calc.

    `sample`: if set, return a random N-row sample. Useful in the
    notebook to keep folium rendering snappy on a 3.5 M-row parquet.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    limit_clause = f"USING SAMPLE {int(sample)} ROWS (reservoir, {int(seed)})" if sample else ""
    sql = f"""
        SELECT
            id,
            class,
            num_floors,
            height,
            ST_Area(geometry) * 111139.0 * 111139.0 AS area_m2,
            ST_X(ST_Centroid(geometry)) AS lon,
            ST_Y(ST_Centroid(geometry)) AS lat
        FROM read_parquet('{parquet_path}')
        {limit_clause}
    """
    return con.execute(sql).fetchdf()


def aggregate_to_h3(
    buildings: pd.DataFrame,
    *,
    h3_col: str = "h3_r8",
    value_cols: tuple[str, ...] = ("daly_total",),
    count_col: str | None = "n_buildings",
) -> pd.DataFrame:
    """Roll per-building values up to an H3 cell for choropleth display.

    Sums the requested value columns, optionally adds a building count.
    Folium can comfortably render ~10 K H3 r8 cells but chokes on
    millions of individual polygons, so this is the bridge.
    """
    agg_spec: dict = {c: "sum" for c in value_cols}
    out = buildings.groupby(h3_col, as_index=False).agg(agg_spec)
    if count_col:
        sizes = buildings.groupby(h3_col).size().rename(count_col).reset_index()
        out = out.merge(sizes, on=h3_col)
    return out


def h3_to_geojson_features(
    cells: pd.DataFrame,
    *,
    h3_col: str = "h3_r8",
    properties: tuple[str, ...] = ("daly_total", "n_buildings"),
) -> list[dict]:
    """Convert an H3-keyed DataFrame to GeoJSON Features for folium.

    Uses `h3.cell_to_boundary` (lat, lng order in v4); we swap to
    (lng, lat) here because GeoJSON is lon-first.
    """
    import h3

    feats: list[dict] = []
    for row in cells.itertuples(index=False):
        cell = getattr(row, h3_col)
        if cell is None or (isinstance(cell, float) and cell != cell):  # NaN
            continue
        boundary = h3.cell_to_boundary(cell)  # [(lat, lng), ...]
        ring = [[lng, lat] for (lat, lng) in boundary] + [[boundary[0][1], boundary[0][0]]]
        props = {p: getattr(row, p) for p in properties if hasattr(row, p)}
        props[h3_col] = cell
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": props,
        })
    return feats


def write_folium_choropleth(
    cells: pd.DataFrame,
    *,
    value_col: str,
    h3_col: str = "h3_r8",
    out_html: Path,
    aoi: AoiConfig,
    bins: int = 7,
    legend: str | None = None,
) -> Path:
    """Render an H3-cell choropleth of `value_col` and write to HTML."""
    import folium
    from folium.plugins import HeatMap  # noqa: F401  (imported for plugin registration)

    w, s, e, n = aoi.bbox
    center = [(s + n) / 2, (w + e) / 2]
    m = folium.Map(location=center, zoom_start=10, tiles="cartodbpositron")

    feats = h3_to_geojson_features(
        cells, h3_col=h3_col, properties=(value_col, "n_buildings"),
    )
    if not feats:
        m.save(out_html)
        return out_html

    vals = cells[value_col].to_numpy()
    finite = vals[~pd.isna(vals)]
    if finite.size:
        vmin, vmax = float(finite.min()), float(finite.max())
    else:
        vmin, vmax = 0.0, 1.0

    def _style(feat: dict) -> dict:
        v = feat["properties"].get(value_col)
        if v is None or v != v:
            return {"fillOpacity": 0.0, "weight": 0}
        # Linear stretch into a magma-ish ramp via folium's built-in.
        frac = 0.0 if vmax == vmin else (v - vmin) / (vmax - vmin)
        # Manual five-stop ramp (yellow → red → purple); avoids the
        # branca dep beyond what folium already pulls in.
        stops = ["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"]
        idx = min(int(frac * (len(stops) - 1) + 0.5), len(stops) - 1)
        return {
            "fillColor": stops[idx],
            "color": stops[idx],
            "weight": 0.2,
            "fillOpacity": 0.7,
        }

    folium.GeoJson(
        {"type": "FeatureCollection", "features": feats},
        style_function=_style,
        tooltip=folium.GeoJsonTooltip(
            fields=[h3_col, value_col, "n_buildings"],
            aliases=["H3 cell", legend or value_col, "buildings"],
            localize=True,
        ),
        name=legend or value_col,
    ).add_to(m)

    folium.LayerControl().add_to(m)
    m.save(str(out_html))
    return out_html
