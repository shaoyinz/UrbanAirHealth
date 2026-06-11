"""Unit tests for the DALY scoring library.

Covers the pure-math primitives that the Spark UDF will call: IDW
interpolation, attributable-fraction log-linear CR, per-cause DALY
aggregation, and the trapezoidal integrator lifted from
``floodpipe.scoring.ead.expected_annual_damage``.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pytest

import pyarrow as pa
import pyarrow.parquet as pq

from airhealth.scoring.dalys import (
    ENGINE_MRBRT_GBD2019,
    CauseCR,
    CRConfig,
    attributable_fraction,
    attributable_fraction_mrbrt,
    expected_annual_dalys,
    idw_interpolate,
    integrate_health_burden,
    load_concentration_response,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CR_YAML = REPO_ROOT / "config" / "concentration_response.yaml"


# ---------------------------------------------------------------------------
# IDW
# ---------------------------------------------------------------------------


def test_idw_coincident_monitor_wins_exactly():
    """A monitor at the building's lon/lat returns its value verbatim,
    not blended with farther monitors."""
    b_lon = np.array([-118.25])
    b_lat = np.array([34.05])
    m_lon = np.array([-118.25, -117.00])
    m_lat = np.array([34.05, 34.00])
    m_val = np.array([12.0, 999.0])
    out = idw_interpolate(b_lon, b_lat, m_lon, m_lat, m_val)
    assert out[0] == pytest.approx(12.0)


def test_idw_two_equidistant_monitors_averages():
    """Inverse-distance from equidistant monitors collapses to the
    simple mean — guards against numerical drift in the IDW kernel."""
    # Place building at midpoint, monitors symmetric on either side.
    b_lon = np.array([-118.0])
    b_lat = np.array([34.0])
    m_lon = np.array([-118.1, -117.9])
    m_lat = np.array([34.0, 34.0])
    m_val = np.array([10.0, 20.0])
    out = idw_interpolate(b_lon, b_lat, m_lon, m_lat, m_val)
    assert out[0] == pytest.approx(15.0, rel=1e-6)


def test_idw_max_km_drops_far_monitors():
    """A monitor outside ``max_km`` must not contribute, even at power=2."""
    b_lon = np.array([-118.0])
    b_lat = np.array([34.0])
    # Two monitors: one ~10 km, one ~500 km (across CA).
    m_lon = np.array([-118.1, -122.0])
    m_lat = np.array([34.0, 37.0])
    m_val = np.array([10.0, 1000.0])
    out = idw_interpolate(b_lon, b_lat, m_lon, m_lat, m_val, max_km=50.0)
    assert out[0] == pytest.approx(10.0)


def test_idw_no_monitors_in_range_returns_nan():
    b_lon = np.array([-118.0])
    b_lat = np.array([34.0])
    m_lon = np.array([-122.0])
    m_lat = np.array([37.0])
    m_val = np.array([10.0])
    out = idw_interpolate(b_lon, b_lat, m_lon, m_lat, m_val, max_km=10.0)
    assert math.isnan(out[0])


def test_idw_empty_monitor_set_returns_nan_array():
    b_lon = np.array([-118.0, -117.0])
    b_lat = np.array([34.0, 33.0])
    out = idw_interpolate(b_lon, b_lat, np.array([]), np.array([]), np.array([]))
    assert out.shape == (2,)
    assert np.all(np.isnan(out))


# ---------------------------------------------------------------------------
# Concentration-response
# ---------------------------------------------------------------------------


def _toy_cause(hr_per_10: float = 1.10) -> CauseCR:
    """Minimal cause for AF tests: HR=1.10 per 10 µg/m³."""
    return CauseCR(
        key="toy",
        name="Toy disease",
        beta_per_ugm3=float(np.log(hr_per_10) / 10.0),
        baseline_mortality_per_100k=100.0,
        yll_per_death=10.0,
        yld_per_death=1.0,
        disability_weight=0.5,
    )


def test_af_below_counterfactual_is_zero():
    cause = _toy_cause()
    pm = np.array([0.0, 1.0, 2.0])
    af = attributable_fraction(pm, cause, counterfactual_ugm3=2.4)
    assert np.allclose(af, 0.0)


def test_af_matches_log_linear_formula():
    """Spot-check AF at PM = 12.4 µg/m³, TMREL = 2.4, HR = 1.10/10:
    AF = 1 - exp(-ln(1.10)/10 * 10) = 1 - 1/1.10 ≈ 0.0909."""
    cause = _toy_cause(hr_per_10=1.10)
    pm = np.array([12.4])
    af = attributable_fraction(pm, cause, counterfactual_ugm3=2.4)
    expected = 1.0 - 1.0 / 1.10
    assert af[0] == pytest.approx(expected, rel=1e-6)


def test_load_concentration_response_yaml_parses():
    """The shipped YAML must round-trip into a CRConfig with the GBD 2019
    MR-BRT engine, all six PM2.5 causes (including T2D), and β still
    derivable from the HR fallback field so the legacy log-linear path
    keeps working for regression tests."""
    cr = load_concentration_response(CR_YAML)
    assert cr.engine == ENGINE_MRBRT_GBD2019
    assert cr.counterfactual_ugm3 == pytest.approx(2.4)
    keys = {c.key for c in cr.causes}
    assert keys == {"ihd", "stroke", "copd", "lung_cancer", "lri", "t2d"}
    ihd = next(c for c in cr.causes if c.key == "ihd")
    assert ihd.beta_per_ugm3 == pytest.approx(np.log(1.17) / 10.0, rel=1e-9)
    # The MR-BRT curve must be populated — from the bundled GBD 2019
    # parquet when present, else the HR-synthesized fallback. Either way
    # the curve starts at exposure 0.
    assert ihd.rr_pm25_ugm3 is not None and ihd.rr_mean is not None
    assert ihd.rr_pm25_ugm3[0] == pytest.approx(0.0)
    # Guard against a silent regression to the HR-synthesized fallback
    # (0..500 grid): the bundled GBD 2019 spline runs to 2500 µg/m³ and
    # has the genuine supra-linear shape (RR@12 ≈ 1.22, distinct from the
    # log-linear 1.17**0.96 ≈ 1.16).
    assert ihd.rr_pm25_ugm3[-1] == pytest.approx(2500.0)
    assert 1.18 < float(np.interp(12.0, ihd.rr_pm25_ugm3, ihd.rr_mean)) < 1.26


# ---------------------------------------------------------------------------
# MR-BRT engine (GBD 2021 splines)
# ---------------------------------------------------------------------------


def _mrbrt_cause(curve_z: np.ndarray, curve_rr: np.ndarray) -> CauseCR:
    """Minimal MR-BRT-style cause: hand-shaped RR(z) curve, no β."""
    return CauseCR(
        key="mrbrt_toy",
        name="MR-BRT toy disease",
        baseline_mortality_per_100k=100.0,
        yll_per_death=10.0,
        yld_per_death=1.0,
        disability_weight=0.5,
        rr_pm25_ugm3=curve_z,
        rr_mean=curve_rr,
    )


def test_af_mrbrt_zero_below_tmrel():
    z = np.array([0.0, 5.0, 10.0, 20.0, 50.0])
    rr = np.array([1.0, 1.3, 1.5, 1.7, 1.9])
    cause = _mrbrt_cause(z, rr)
    af = attributable_fraction_mrbrt(np.array([0.0, 1.0, 2.4]), cause, counterfactual_ugm3=2.4)
    assert np.allclose(af, 0.0)


def test_af_mrbrt_matches_paf_from_rr_formula():
    """Spot-check AF = 1 − RR(TMREL)/RR(PM) against hand-computed values
    on a curve where the interp targets are exactly at tabulated bins
    (so np.interp returns the tabulated RR without any blending error)."""
    z = np.array([0.0, 2.4, 5.0, 10.0, 20.0, 50.0])
    rr = np.array([1.0, 1.0, 1.20, 1.45, 1.60, 1.70])
    cause = _mrbrt_cause(z, rr)
    af = attributable_fraction_mrbrt(np.array([10.0, 20.0]), cause, counterfactual_ugm3=2.4)
    assert af[0] == pytest.approx(1.0 - 1.0 / 1.45, rel=1e-9)
    assert af[1] == pytest.approx(1.0 - 1.0 / 1.60, rel=1e-9)


def test_af_mrbrt_monotonic_on_increasing_curve():
    """AF must be monotonic non-decreasing on a monotonic non-decreasing
    RR curve — a basic property the spline + PAF formula should preserve."""
    z = np.arange(0, 51, dtype="float64")
    rr = 1.0 + 0.02 * z  # strictly increasing
    cause = _mrbrt_cause(z, rr)
    pm = np.linspace(2.5, 50.0, 30)
    af = attributable_fraction_mrbrt(pm, cause, counterfactual_ugm3=2.4)
    assert np.all(np.diff(af) >= 0)


def test_af_mrbrt_clipped_to_unit_interval():
    """AF should never exceed 1 even on a degenerate curve where RR(PM)
    > RR(TMREL) but the ratio is unusually large."""
    z = np.array([0.0, 5.0, 50.0])
    rr = np.array([1.0, 1.05, 100.0])
    cause = _mrbrt_cause(z, rr)
    af = attributable_fraction_mrbrt(np.array([50.0]), cause, counterfactual_ugm3=2.4)
    assert 0.0 <= af[0] < 1.0


def test_load_concentration_response_picks_up_real_curve(tmp_path):
    """When a curve_path resolves to an existing parquet, the loader must
    use the tabulated values verbatim rather than the HR fallback."""
    # Build a tiny YAML pointing at a curve we control.
    curves_dir = tmp_path / "cr_curves" / "gbd2021"
    curves_dir.mkdir(parents=True)
    z = np.arange(0, 11, dtype="float64")
    rr = 1.0 + 0.05 * z  # distinct from any HR-synthesized shape
    table = pa.table({"pm25_ugm3": z, "rr_mean": rr})
    pq.write_table(table, curves_dir / "ihd.parquet")

    yaml_path = tmp_path / "cr.yaml"
    yaml_path.write_text(
        "engine: mrbrt_gbd2021\n"
        "counterfactual_ugm3: 2.4\n"
        "causes:\n"
        "  ihd:\n"
        "    name: 'Ischemic heart disease'\n"
        "    hazard_ratio_per_10ugm3: 1.17\n"
        "    curve_path: cr_curves/gbd2021/ihd.parquet\n"
        "    baseline_mortality_per_100k: 92.2\n"
        "    yll_per_death: 12.8\n"
        "    yld_per_death: 0.6\n"
        "    disability_weight: 0.224\n"
    )
    cr = load_concentration_response(yaml_path)
    ihd = next(c for c in cr.causes if c.key == "ihd")
    # Curve must match the parquet, not the HR-synthesized 0..500 grid.
    assert ihd.rr_pm25_ugm3.shape == (11,)
    assert ihd.rr_mean[5] == pytest.approx(1.0 + 0.05 * 5)


# ---------------------------------------------------------------------------
# DALY aggregation
# ---------------------------------------------------------------------------


def test_expected_annual_dalys_zero_pop_is_zero():
    cr = load_concentration_response(CR_YAML)
    pm = np.array([20.0, 30.0])
    pop = np.array([0.0, 0.0])
    out = expected_annual_dalys(pm, pop, cr)
    for arr in out.values():
        assert np.allclose(arr, 0.0)


def test_expected_annual_dalys_scales_linearly_with_pop():
    cr = load_concentration_response(CR_YAML)
    pm = np.array([15.0, 15.0])
    out_a = expected_annual_dalys(pm, np.array([100.0, 100.0]), cr)
    out_b = expected_annual_dalys(pm, np.array([200.0, 200.0]), cr)
    for k in out_a:
        assert np.allclose(out_b[k], 2 * out_a[k])


def test_expected_annual_dalys_per_cause_keys_match_config():
    cr = load_concentration_response(CR_YAML)
    pm = np.array([10.0])
    out = expected_annual_dalys(pm, np.array([1.0]), cr)
    assert set(out) == {c.key for c in cr.causes}


# ---------------------------------------------------------------------------
# Trapezoidal integrator (lifted from floodpipe.scoring.ead)
# ---------------------------------------------------------------------------


def test_integrate_health_burden_matches_hand_worked_two_point():
    """Trapezoidal area between two return periods.

        EAD-like = 0.5 * (1/T_lo - 1/T_hi) * (D_lo + D_hi)

    With T_lo=10, T_hi=100, D_lo=1.0, D_hi=2.0:
        0.5 * (0.1 - 0.01) * (1.0 + 2.0) = 0.135
    """
    burden = {10: np.array([1.0]), 100: np.array([2.0])}
    out = integrate_health_burden(burden)
    assert out[0] == pytest.approx(0.135, rel=1e-9)


def test_integrate_health_burden_three_point_sums_segments():
    """Three-period integration is the sum of two trapezoids; verify by
    constructing one explicitly."""
    burden = {
        10: np.array([1.0]),
        50: np.array([2.0]),
        100: np.array([3.0]),
    }
    seg1 = 0.5 * (1 / 10 - 1 / 50) * (1.0 + 2.0)
    seg2 = 0.5 * (1 / 50 - 1 / 100) * (2.0 + 3.0)
    out = integrate_health_burden(burden)
    assert out[0] == pytest.approx(seg1 + seg2, rel=1e-12)


def test_integrate_health_burden_unsorted_keys_handled():
    """Caller may pass dict in any order; integrator sorts by T ascending."""
    sorted_ = integrate_health_burden({10: np.array([1.0]), 100: np.array([2.0])})
    shuffled = integrate_health_burden({100: np.array([2.0]), 10: np.array([1.0])})
    assert sorted_[0] == pytest.approx(shuffled[0], rel=1e-12)


def test_integrate_health_burden_requires_two_points():
    with pytest.raises(ValueError, match=">=2"):
        integrate_health_burden({100: np.array([1.0])})
