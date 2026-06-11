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

Two concentration-response engines ship side-by-side:

* ``mrbrt_gbd2021`` — the SOTA path. Tabulated mean RR(z) splines from
  IHME's MR-BRT (Meta-Regression Bayesian, Regularized, Trimmed) tool
  per Brauer et al. (Lancet 2024). AF derives from the population
  attributable formula ``1 − RR(TMREL) / RR(PM)`` so supra-linearity at
  low PM and the sub-linear bend above ~50 µg/m³ are both preserved.
* ``log_linear_gbd2019`` — the legacy approximation. ``AF = 1 − exp(−β · ΔPM)``
  with ``β = ln(HR_per_10) / 10``. Kept available for regression-testing
  and as the loader fallback when the bundled MR-BRT parquet files are
  absent (CI before the GHDx download lands).

All functions are pure NumPy + stdlib so a pandas UDF on Sedona can
call them with zero extra deps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import numpy as np
import pyarrow.parquet as pq
import yaml

ENGINE_MRBRT = "mrbrt_gbd2021"
ENGINE_MRBRT_GBD2019 = "mrbrt_gbd2019"
ENGINE_LOG_LINEAR = "log_linear_gbd2019"

# MR-BRT spline engines. The AF math (1 − RR(TMREL)/RR(PM)) is identical
# across GBD vintages — only the bundled curve parquets differ — so both
# route through ``attributable_fraction_mrbrt``. The engine string records
# *which* GBD release the curves under ``curve_path`` came from, so a
# config is never silently mislabeled (2019 curves stay named 2019).
MRBRT_ENGINES = (ENGINE_MRBRT, ENGINE_MRBRT_GBD2019)


# ---------------------------------------------------------------------------
# Concentration-response config (loaded once per Spark task)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class CauseCR:
    """One disease/cause: CR curve + outcome weights.

    Exactly one of ``beta_per_ugm3`` or ``(rr_pm25_ugm3, rr_mean)`` is the
    primary CR signal, depending on the engine the loader was asked to
    build. The legacy ``beta_per_ugm3`` field is always populated when a
    ``hazard_ratio_per_10ugm3`` is present in YAML so log-linear tests
    can keep pinning the legacy math even when the active engine is
    MR-BRT.
    """

    key: str
    name: str
    baseline_mortality_per_100k: float
    yll_per_death: float
    yld_per_death: float
    disability_weight: float
    beta_per_ugm3: float | None = None
    # MR-BRT tabulated curve: parallel arrays, PM2.5 in µg/m³ (ascending)
    # and the mean of the 1000 posterior MR-BRT draws for RR(z).
    rr_pm25_ugm3: np.ndarray | None = field(default=None, repr=False)
    rr_mean: np.ndarray | None = field(default=None, repr=False)

    def daly_per_death(self) -> float:
        """YLL + YLD weighted by disability — both already per fatal case."""
        return self.yll_per_death + self.disability_weight * self.yld_per_death


@dataclass(frozen=True, eq=False)
class CRConfig:
    engine: str
    counterfactual_ugm3: float
    causes: tuple[CauseCR, ...]


def _synthesize_curve_from_hr(
    hr_per_10: float, counterfactual_ugm3: float, max_ugm3: int = 500
) -> tuple[np.ndarray, np.ndarray]:
    """Tabulate ``RR(z) = HR_per_10 ** ((z − TMREL) / 10)`` at integer z.

    Loader fallback for the MR-BRT engine when the bundled GHDx parquet
    is missing. The resulting curve is mathematically identical to the
    log-linear engine, so the synthesized-curve mode is *not* SOTA — it
    just keeps the new code path runnable in CI before the data lands.
    Above TMREL only; below TMREL the loader RR(z) = 1.0 so AF clamps
    to 0 in ``attributable_fraction_mrbrt``.
    """
    z = np.arange(0, max_ugm3 + 1, dtype="float64")
    excess = np.maximum(z - counterfactual_ugm3, 0.0)
    rr = np.power(float(hr_per_10), excess / 10.0)
    return z, rr


def _load_curve_parquet(path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Read a 2-column (pm25_ugm3, rr_mean) parquet into sorted arrays."""
    table = pq.read_table(path, columns=["pm25_ugm3", "rr_mean"])
    z = table.column("pm25_ugm3").to_numpy().astype("float64")
    rr = table.column("rr_mean").to_numpy().astype("float64")
    order = np.argsort(z)
    return z[order], rr[order]


def load_concentration_response(
    path: Path, *, curve_root: Path | None = None
) -> CRConfig:
    """Parse the CR YAML into a CRConfig.

    ``curve_root`` is the base for relative ``curve_path`` entries. When
    omitted it defaults to the YAML file's directory, so the shipped
    ``config/concentration_response.yaml`` resolves
    ``cr_curves/gbd2021/<cause>.parquet`` against ``config/``.
    """
    yaml_path = Path(path)
    raw = yaml.safe_load(yaml_path.read_text())
    engine = str(raw.get("engine", ENGINE_LOG_LINEAR))
    if engine not in (*MRBRT_ENGINES, ENGINE_LOG_LINEAR):
        raise ValueError(f"unknown CR engine: {engine!r}")
    counterfactual = float(raw["counterfactual_ugm3"])
    root = Path(curve_root) if curve_root else yaml_path.parent

    causes: list[CauseCR] = []
    for key, v in raw["causes"].items():
        hr = v.get("hazard_ratio_per_10ugm3")
        beta = float(np.log(hr) / 10.0) if hr is not None else None

        rr_z: np.ndarray | None = None
        rr_y: np.ndarray | None = None
        if engine in MRBRT_ENGINES:
            curve_path = v.get("curve_path")
            resolved: Path | None = None
            if curve_path is not None:
                cp = Path(curve_path)
                resolved = cp if cp.is_absolute() else root / cp
            if resolved is not None and resolved.exists():
                rr_z, rr_y = _load_curve_parquet(resolved)
            elif hr is not None:
                rr_z, rr_y = _synthesize_curve_from_hr(hr, counterfactual)
            else:
                raise ValueError(
                    f"cause {key!r}: engine={engine} needs either an existing "
                    f"curve_path or hazard_ratio_per_10ugm3 fallback"
                )

        causes.append(
            CauseCR(
                key=key,
                name=str(v["name"]),
                baseline_mortality_per_100k=float(v["baseline_mortality_per_100k"]),
                yll_per_death=float(v["yll_per_death"]),
                yld_per_death=float(v["yld_per_death"]),
                disability_weight=float(v["disability_weight"]),
                beta_per_ugm3=beta,
                rr_pm25_ugm3=rr_z,
                rr_mean=rr_y,
            )
        )
    return CRConfig(
        engine=engine,
        counterfactual_ugm3=counterfactual,
        causes=tuple(causes),
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
    """Log-linear (GBD 2019) AF = 1 − exp(−β · (PM − TMREL)).

    Legacy engine. Exact enough for ≤ 50 µg/m³ exposures; over-estimates
    in wildfire-smoke regimes where the IER bends sub-linear. Prefer
    ``attributable_fraction_mrbrt`` when a tabulated MR-BRT curve is
    available on the cause.
    """
    if cause.beta_per_ugm3 is None:
        raise ValueError(
            f"cause {cause.key!r} has no beta_per_ugm3 — built for the MR-BRT "
            f"engine? Use attributable_fraction_mrbrt instead."
        )
    excess = np.maximum(np.asarray(pm25_ugm3, dtype="float64") - counterfactual_ugm3, 0.0)
    return 1.0 - np.exp(-cause.beta_per_ugm3 * excess)


def attributable_fraction_mrbrt(
    pm25_ugm3: np.ndarray, cause: CauseCR, counterfactual_ugm3: float
) -> np.ndarray:
    """MR-BRT spline AF = 1 − RR(TMREL) / RR(PM).

    ``np.interp`` is the linear interpolator on the tabulated curve;
    MR-BRT publishes the spline as ≈1-µg/m³ resolution so linear
    interpolation between bins is within sub-percent of the spline
    itself. Returns 0 when PM ≤ TMREL (no excess risk by construction)
    and clips into ``[0, 1)`` to keep downstream multiplications well
    behaved if a curve hiccup drives RR(PM) < RR(TMREL).
    """
    if cause.rr_pm25_ugm3 is None or cause.rr_mean is None:
        raise ValueError(
            f"cause {cause.key!r} has no MR-BRT curve — was the YAML loaded "
            f"with the mrbrt_gbd2021 engine?"
        )
    pm = np.asarray(pm25_ugm3, dtype="float64")
    rr_pm = np.interp(pm, cause.rr_pm25_ugm3, cause.rr_mean)
    rr_tm = float(np.interp(counterfactual_ugm3, cause.rr_pm25_ugm3, cause.rr_mean))
    # Guard against divide-by-zero on degenerate curves; MR-BRT RRs are
    # always > 0 in practice but be defensive in case a fixture is sparse.
    with np.errstate(divide="ignore", invalid="ignore"):
        af = 1.0 - rr_tm / np.where(rr_pm > 0, rr_pm, np.nan)
    af = np.where(pm <= counterfactual_ugm3, 0.0, af)
    return np.clip(af, 0.0, 0.9999999)


def _af_fn_for_engine(engine: str) -> Callable[..., np.ndarray]:
    if engine in MRBRT_ENGINES:
        return attributable_fraction_mrbrt
    if engine == ENGINE_LOG_LINEAR:
        return attributable_fraction
    raise ValueError(f"unknown CR engine: {engine!r}")


def expected_annual_dalys(
    pm25_ugm3: np.ndarray,
    population: np.ndarray,
    cr: CRConfig,
) -> dict[str, np.ndarray]:
    """Per-cause expected DALYs/yr per building.

        DALY_cause = AF_cause × (baseline_mortality/100k × pop) × DALY_per_death

    The AF function is selected from ``cr.engine`` so callers don't need
    to know which curve shape is in play. Returns a dict keyed by cause
    so the caller can sum for the total or keep the breakdown for the
    dashboard's "top drivers" panel (analogous to the flood project's
    SHAP top-3).
    """
    af_fn = _af_fn_for_engine(cr.engine)
    pop = np.asarray(population, dtype="float64")
    result: dict[str, np.ndarray] = {}
    for c in cr.causes:
        af = af_fn(pm25_ugm3, c, cr.counterfactual_ugm3)
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
