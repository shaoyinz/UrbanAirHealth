"""Per-building expected health burden from PM2.5 exposure.

Mirrors the EAD framework from UrbanFloodRisk:

    risk     = exposure × population × concentration_response
    DALY/yr  = Σ_cause AF_cause(PM2.5) × baseline_incidence × DW × population

with the trapezoidal integrator lifted from
``floodpipe.scoring.ead.expected_annual_damage`` and generalized to any
two-point loss-frequency curve. The Gumbel interpolation step from the
flood project is replaced by **inverse-distance-weighted interpolation
of monitor PM2.5 to building centroids**: same role in the pipeline
(turn sparse anchor points into a per-building intensity field), entirely
different physics.

All functions are pure NumPy + stdlib so a pandas UDF on Sedona can
call them with zero extra deps.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import yaml

# ---------------------------------------------------------------------------
# Concentration-response config (loaded once per Spark task)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CauseCR:
    """One disease/cause: log-linear CR around PM2.5 + outcome weights.

    ``beta_per_ugm3`` is the slope of ln(HR) vs. PM2.5; it derives from
    the published hazard-ratio-per-10-µg/m³ as ``ln(HR_per_10) / 10``.
    """

    key: str
    name: str
    beta_per_ugm3: float
    baseline_mortality_per_100k: float
    yll_per_death: float
    yld_per_death: float
    disability_weight: float

    def daly_per_death(self) -> float:
        """YLL + YLD weighted by disability — both already per fatal case."""
        return self.yll_per_death + self.disability_weight * self.yld_per_death


@dataclass(frozen=True)
class CRConfig:
    counterfactual_ugm3: float
    causes: tuple[CauseCR, ...]


def load_concentration_response(path: Path) -> CRConfig:
    raw = yaml.safe_load(Path(path).read_text())
    causes = tuple(
        CauseCR(
            key=key,
            name=str(v["name"]),
            beta_per_ugm3=float(np.log(v["hazard_ratio_per_10ugm3"]) / 10.0),
            baseline_mortality_per_100k=float(v["baseline_mortality_per_100k"]),
            yll_per_death=float(v["yll_per_death"]),
            yld_per_death=float(v["yld_per_death"]),
            disability_weight=float(v["disability_weight"]),
        )
        for key, v in raw["causes"].items()
    )
    return CRConfig(
        counterfactual_ugm3=float(raw["counterfactual_ugm3"]),
        causes=causes,
    )


# ---------------------------------------------------------------------------
# Spatial: monitor → building IDW
# ---------------------------------------------------------------------------


def _haversine_km(
    lon1: np.ndarray, lat1: np.ndarray, lon2: np.ndarray, lat2: np.ndarray
) -> np.ndarray:
    """Great-circle distance in km. Vectorized over both sides."""
    r = 6371.0088
    lon1, lat1, lon2, lat2 = (np.radians(x) for x in (lon1, lat1, lon2, lat2))
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def idw_interpolate(
    building_lon: np.ndarray,
    building_lat: np.ndarray,
    monitor_lon: np.ndarray,
    monitor_lat: np.ndarray,
    monitor_value: np.ndarray,
    *,
    power: float = 2.0,
    max_km: float | None = None,
    min_neighbors: int = 1,
) -> np.ndarray:
    """Inverse-distance-weighted interpolation, monitors → buildings.

    Returns one value per building. Buildings with no monitors inside
    ``max_km`` get NaN — caller decides whether to fall back to a regional
    mean or drop them.

    ``power=2`` is the conventional Shepard default; tuning per AOI is
    Phase 4 work (cross-validate against held-out monitors).
    """
    if monitor_lon.shape != monitor_lat.shape or monitor_lon.shape != monitor_value.shape:
        raise ValueError("monitor arrays must share shape")
    if building_lon.shape != building_lat.shape:
        raise ValueError("building lon/lat must share shape")

    out = np.full(building_lon.shape, np.nan, dtype="float64")
    if monitor_lon.size == 0:
        return out

    # Broadcasting (n_buildings, n_monitors) — fine for AOI-scale runs.
    # The Spark job calls this inside a pandas UDF that already partitions
    # by H3 cell, so n_buildings per call is bounded.
    b_lon = building_lon[:, None]
    b_lat = building_lat[:, None]
    m_lon = monitor_lon[None, :]
    m_lat = monitor_lat[None, :]
    d_km = _haversine_km(b_lon, b_lat, m_lon, m_lat)

    # Co-located monitor (d ≈ 0) wins outright; avoids div-by-zero.
    coincident = d_km < 1e-6
    if coincident.any():
        # First exact match per row.
        idx = np.argmax(coincident, axis=1)
        rows_exact = coincident.any(axis=1)
        out[rows_exact] = monitor_value[idx[rows_exact]]

    mask = ~np.isnan(out)  # rows still to fill
    if max_km is not None:
        d_km = np.where(d_km > max_km, np.inf, d_km)

    w = np.where(np.isfinite(d_km), 1.0 / (d_km**power + 1e-12), 0.0)
    neighbors_per_row = (w > 0).sum(axis=1)
    enough = neighbors_per_row >= min_neighbors

    rows = (~mask) & enough
    num = (w[rows] * monitor_value[None, :]).sum(axis=1)
    den = w[rows].sum(axis=1)
    out[rows] = num / den
    return out


# ---------------------------------------------------------------------------
# Health: PM2.5 → attributable fraction → DALYs
# ---------------------------------------------------------------------------


def attributable_fraction(
    pm25_ugm3: np.ndarray, cause: CauseCR, counterfactual_ugm3: float
) -> np.ndarray:
    """AF = 1 − exp(−β · (PM − TMREL)), clipped at zero below TMREL.

    Log-linear approximation. Exact enough for ≤ 50 µg/m³ exposures (most
    of CONUS in non-fire conditions). Above that, the GBD IER bends
    sublinear and this overestimates; Phase 4 swaps for the full IER.
    """
    excess = np.maximum(np.asarray(pm25_ugm3, dtype="float64") - counterfactual_ugm3, 0.0)
    return 1.0 - np.exp(-cause.beta_per_ugm3 * excess)


def expected_annual_dalys(
    pm25_ugm3: np.ndarray,
    population: np.ndarray,
    cr: CRConfig,
) -> dict[str, np.ndarray]:
    """Per-cause expected DALYs/yr per building.

        DALY_cause = AF_cause × (baseline_mortality/100k × pop) × DALY_per_death

    Returns a dict keyed by cause; caller sums for the total or keeps the
    breakdown for the dashboard's "top drivers" panel (analogous to the
    flood project's SHAP top-3).
    """
    pop = np.asarray(population, dtype="float64")
    result: dict[str, np.ndarray] = {}
    for c in cr.causes:
        af = attributable_fraction(pm25_ugm3, c, cr.counterfactual_ugm3)
        attributable_deaths = af * (c.baseline_mortality_per_100k / 1e5) * pop
        result[c.key] = attributable_deaths * c.daly_per_death()
    return result


def integrate_health_burden(
    burden_by_period: dict[int, np.ndarray],
) -> np.ndarray:
    """Trapezoidal integration of burden over inverse-period (probability).

    Lifted from ``floodpipe.scoring.ead.expected_annual_damage``. Keys are
    return periods in years (or any frequency unit); values are the burden
    at that frequency. For air quality the typical use is integrating
    *exceedance burden* over multiple PM2.5 thresholds (e.g. annual mean,
    98th-percentile-day, peak-week) — the same trapezoid that turned
    flood depth-frequency into EAD turns concentration-frequency into
    annual expected DALYs.
    """
    if len(burden_by_period) < 2:
        raise ValueError("need >=2 return periods to integrate")
    ts = sorted(burden_by_period)
    total = np.zeros_like(burden_by_period[ts[0]], dtype="float64")
    for t_lo, t_hi in zip(ts, ts[1:]):
        b_lo = burden_by_period[t_lo]
        b_hi = burden_by_period[t_hi]
        dp = (1.0 / t_lo) - (1.0 / t_hi)
        total += 0.5 * dp * (b_lo + b_hi)
    return total
