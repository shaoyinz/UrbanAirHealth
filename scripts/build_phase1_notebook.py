"""Generate notebooks/exploration/01_la_basin_prototype.ipynb from source.

We commit the notebook in JSON form for diffs, but author it from this
Python script so cells stay terse and the heavy logic lives in
`airhealth.features` / `airhealth.io` (testable, importable).

Run:  PYTHONPATH=src python scripts/build_phase1_notebook.py
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT = REPO_ROOT / "notebooks" / "exploration" / "01_la_basin_prototype.ipynb"


CELLS: list[tuple[str, str]] = [
    ("md", """# Phase 1 — LA-basin PM2.5 → building DALYs (local prototype)

End-to-end demonstration of the analytical chain that the cloud phases
will scale up:

1. Load staged AirNow hourly observations (LA basin AOI) + Overture
   building centroids.
2. Hourly → daily mean per monitor; compute completeness.
3. Annual mean / 98th-percentile day / peak-week per monitor.
4. IDW interpolation of annual mean PM2.5 to every building centroid.
5. Concentration-response → expected annual DALYs per building, per
   cause (IHD, stroke, COPD, lung cancer, LRI).
6. Aggregate to H3 r8 cells; render folium choropleth.

All heavy lifting lives in `airhealth.{features,io,scoring}` so the
Phase 2 Sedona job can reuse the same primitives unchanged.

**Note on coverage.** This notebook runs on whatever AirNow days you've
staged under `data/raw/airnow/`. With only a partial window staged,
"annual" metrics are really window-mean — fine for plumbing validation,
not for headline numbers.
"""),

    ("code", """from pathlib import Path
import sys
import numpy as np
import pandas as pd

REPO_ROOT = Path.cwd()
while not (REPO_ROOT / "config" / "release.yaml").exists():
    REPO_ROOT = REPO_ROOT.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from airhealth.ingest._common import load_release_config
from airhealth.features import annual_metrics, assign_h3_cells, completeness, daily_mean
from airhealth.io import (
    aggregate_to_h3,
    read_airnow_window,
    read_overture_centroids,
    write_folium_choropleth,
)
from airhealth.scoring import (
    expected_annual_dalys,
    idw_interpolate,
    load_concentration_response,
)

cfg = load_release_config()
cr = load_concentration_response(REPO_ROOT / "config" / "concentration_response.yaml")
print("AOI:", cfg.aoi.name, cfg.aoi.bbox)
print("AirNow window:", cfg.airnow_window)
print("CR causes:", [c.key for c in cr.causes])"""),

    ("md", "## 1. Load AirNow hourly observations"),

    ("code", """hourly = read_airnow_window(cfg.aoi)
print(f"hourly rows: {len(hourly):,}  monitors: {hourly['aqsid'].nunique()}")
hourly.head(3)"""),

    ("md", "## 2. Daily means + completeness per monitor"),

    ("code", """daily = daily_mean(hourly)
comp = completeness(daily)
print(f"daily rows: {len(daily):,}  unique dates: {daily['date_utc'].nunique()}")
comp.sort_values("completeness", ascending=False)"""),

    ("md", """## 3. Annual exposure metrics per monitor

`annual_mean` feeds the chronic-exposure DALY calc; `p98_day` and
`peak_week_mean` are kept for the dashboard's acute-tail panel.
"""),

    ("code", """monitors = annual_metrics(daily)
monitors"""),

    ("md", """## 4. Load Overture building centroids

3.5 M LA-basin buildings is too many for folium; we sample for the
choropleth but keep the IDW + DALY math vectorized at the sample size.
The Spark job (Phase 2) runs the same math at full scale.
"""),

    ("code", """OVERTURE_PARQUET = REPO_ROOT / "data" / "raw" / "overture_la_basin_2026-04-15.0.parquet.uploaded"
SAMPLE_BUILDINGS = 50_000  # ~50 K is plenty for an H3-r8 choropleth.

buildings = read_overture_centroids(OVERTURE_PARQUET, sample=SAMPLE_BUILDINGS)
print(f"buildings sampled: {len(buildings):,}")
buildings.head(3)"""),

    ("md", """## 5. IDW: monitor PM2.5 → building PM2.5

Power 2 (Shepard default), max neighbor radius 150 km (CONUS rural
gaps); LA basin has 9 monitors so every building has plenty.
"""),

    ("code", """buildings["pm25_ugm3"] = idw_interpolate(
    buildings["lon"].to_numpy(),
    buildings["lat"].to_numpy(),
    monitors["lon"].to_numpy(),
    monitors["lat"].to_numpy(),
    monitors["annual_mean"].to_numpy(),
    power=2.0,
    max_km=150.0,
)
buildings["pm25_ugm3"].describe()"""),

    ("md", """## 6. Per-building DALYs

Population estimate uses Overture `num_floors × area_m²` × a crude
occupants-per-m² constant (refined in Phase 4 with ACS). Folded into
the CR × baseline-mortality math from `airhealth.scoring`.
"""),

    ("code", """# Crude per-building occupancy: 0.04 occupants/m² floor area
# (≈ US residential average of 25 m²/person, mixes commercial in).
buildings["floor_area_m2"] = (
    buildings["area_m2"].fillna(0)
    * buildings["num_floors"].fillna(1).clip(lower=1)
)
buildings["population"] = buildings["floor_area_m2"] * 0.04

daly_by_cause = expected_annual_dalys(
    buildings["pm25_ugm3"].to_numpy(),
    buildings["population"].to_numpy(),
    cr,
)
for k, v in daly_by_cause.items():
    buildings[f"daly_{k}"] = v
buildings["daly_total"] = sum(daly_by_cause.values())

print("Total DALYs in sample:", buildings["daly_total"].sum())
print("Per-cause share:")
for k in daly_by_cause:
    share = buildings[f"daly_{k}"].sum() / buildings["daly_total"].sum()
    print(f"  {k:12s} {share:.1%}")"""),

    ("md", "## 7. H3 indexing + aggregation for the choropleth"),

    ("code", """buildings = assign_h3_cells(buildings)
cells_r8 = aggregate_to_h3(
    buildings,
    h3_col="h3_r8",
    value_cols=("daly_total", "population"),
)
cells_r8["dalys_per_capita"] = cells_r8["daly_total"] / cells_r8["population"].replace(0, np.nan)
print(f"H3 r8 cells: {len(cells_r8):,}")
cells_r8.sort_values("daly_total", ascending=False).head()"""),

    ("md", "## 8. Folium choropleth — annual DALYs per H3 r8 cell"),

    ("code", """OUT = REPO_ROOT / "data" / "la_basin_dalys_r8.html"
write_folium_choropleth(
    cells_r8,
    value_col="daly_total",
    h3_col="h3_r8",
    out_html=OUT,
    aoi=cfg.aoi,
    legend="Expected annual DALYs (per H3 r8 cell)",
)
print(f"wrote {OUT}  ({OUT.stat().st_size/1024:.0f} KB)")
print("open in a browser:  open", OUT)"""),

    ("md", """## Caveats (Phase 1)

- **Coverage:** if only a partial window is staged under
  `data/raw/airnow/`, every "annual" number is a window-mean. Pull the
  full year via `python -m airhealth.ingest.airnow ...` before quoting.
- **Population:** flat 0.04 occupants/m² is a crude stand-in; Phase 4
  replaces with ACS tract population × HUD residential mask.
- **Sample bias:** we choropleth a 50 K-building reservoir sample. The
  Spark job runs the same math at full 3.5 M scale.
- **Log-linear CR:** reliable up to ~50 µg/m³; sublinear above (wildfire
  smoke). Flag `pm25_ugm3 > 50` rows; Phase 4 swaps for full IER.
"""),
]


def build() -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    for kind, src in CELLS:
        if kind == "md":
            nb.cells.append(nbf.v4.new_markdown_cell(src))
        elif kind == "code":
            nb.cells.append(nbf.v4.new_code_cell(src))
        else:
            raise ValueError(kind)
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3 (ipykernel)",
        "language": "python",
        "name": "python3",
    }
    nb.metadata["language_info"] = {"name": "python"}
    return nb


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(build(), OUT)
    print(f"wrote {OUT}  ({len(CELLS)} cells)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
