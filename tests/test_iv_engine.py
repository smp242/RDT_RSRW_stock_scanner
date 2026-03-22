"""
Tests for src/utils/iv_engine.py

Covers:
  - compute_hv: annualized historical volatility
  - compute_iv_metrics: IVR, IV percentile, IV/HV ratios, signal quality, elevated flag
  - assess_vix_environment: green / caution / standby classification
  - compute_universe_iv_metrics: integration path (mocked loader)
"""

import numpy as np
import pandas as pd
import pytest

from src.utils.iv_engine import (
    compute_hv,
    compute_iv_metrics,
    assess_vix_environment,
    compute_universe_iv_metrics,
    MIN_IV_PERCENTILE,
    MIN_IV_RANK,
    MIN_IV_HV_RATIO,
    VIX_CAUTION_LEVEL,
    VIX_STANDBY_LEVEL,
    HV_SHORT_LB,
    HV_LONG_LB,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_price_df(n: int, daily_return: float = 0.01) -> pd.DataFrame:
    """Build a simple price DataFrame with compounding daily returns."""
    closes = [100.0]
    for _ in range(n - 1):
        closes.append(closes[-1] * (1 + daily_return))
    return pd.DataFrame({"close": closes})


def _flat_price_df(n: int, price: float = 100.0) -> pd.DataFrame:
    """All closes identical — zero volatility."""
    return pd.DataFrame({"close": [price] * n})


def _make_iv_history(n: int, base_iv: float = 0.25, step: float = 0.001) -> pd.DataFrame:
    """Build a synthetic IV history DataFrame."""
    dates = pd.date_range("2024-01-01", periods=n, freq="B")
    ivs = [base_iv + i * step for i in range(n)]
    return pd.DataFrame({"date": dates, "iv": ivs})


# ---------------------------------------------------------------------------
# compute_hv
# ---------------------------------------------------------------------------

class TestComputeHv:
    def test_returns_float_with_sufficient_data(self):
        df = _make_price_df(HV_SHORT_LB + 5)
        result = compute_hv(df, HV_SHORT_LB)
        assert isinstance(result, float)
        assert not np.isnan(result)
        assert result > 0

    def test_returns_nan_when_insufficient_data(self):
        df = _make_price_df(HV_SHORT_LB)  # need lookback + 1
        result = compute_hv(df, HV_SHORT_LB)
        assert np.isnan(result)

    def test_zero_volatility_returns_zero(self):
        df = _flat_price_df(HV_SHORT_LB + 5)
        result = compute_hv(df, HV_SHORT_LB)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_long_lookback_nan_when_insufficient(self):
        df = _make_price_df(50)  # far less than 252 + 1
        result = compute_hv(df, HV_LONG_LB)
        assert np.isnan(result)

    def test_annualization(self):
        # Alternating +1% / -1% to produce actual variance in log returns
        closes = [100.0]
        for i in range(HV_SHORT_LB + 1):
            closes.append(closes[-1] * (1.01 if i % 2 == 0 else 0.99))
        df = pd.DataFrame({"close": closes})
        result = compute_hv(df, HV_SHORT_LB)
        # log(1.01) ≈ 0.00995 and log(0.99) ≈ -0.01005; std ≈ 0.01; annualized ≈ 0.159
        assert 0.10 < result < 0.25


# ---------------------------------------------------------------------------
# compute_iv_metrics
# ---------------------------------------------------------------------------

class TestComputeIvMetrics:
    def test_no_history_returns_base(self):
        result = compute_iv_metrics("AAPL", 0.30, pd.DataFrame(columns=["iv"]), 0.20, 0.18)
        assert result["signal_quality"] == "no_history"
        assert result["elevated"] is False
        assert np.isnan(result["iv_rank"])
        assert np.isnan(result["iv_percentile"])

    def test_too_short_history(self):
        hist = _make_iv_history(3)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.20, 0.18)
        assert result["signal_quality"] == "too_short"

    def test_early_quality_label(self):
        hist = _make_iv_history(30)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.20, 0.18)
        assert result["signal_quality"] == "early"

    def test_developing_quality_label(self):
        hist = _make_iv_history(90)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.20, 0.18)
        assert result["signal_quality"] == "developing"

    def test_maturing_quality_label(self):
        hist = _make_iv_history(180)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.20, 0.18)
        assert result["signal_quality"] == "maturing"

    def test_full_quality_label(self):
        hist = _make_iv_history(252)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.20, 0.18)
        assert result["signal_quality"] == "full"

    def test_iv_percentile_at_max(self):
        # current IV higher than all history → 100%
        hist = _make_iv_history(60, base_iv=0.20)
        result = compute_iv_metrics("AAPL", 0.50, hist, 0.20, 0.18)
        assert result["iv_percentile"] == pytest.approx(100.0)

    def test_iv_percentile_at_min(self):
        # current IV lower than all history → 0%
        hist = _make_iv_history(60, base_iv=0.30)
        result = compute_iv_metrics("AAPL", 0.10, hist, 0.20, 0.18)
        assert result["iv_percentile"] == pytest.approx(0.0)

    def test_iv_rank_at_max(self):
        # current IV = historical max
        hist = _make_iv_history(60, base_iv=0.20, step=0.001)
        max_iv = hist["iv"].max()
        result = compute_iv_metrics("AAPL", max_iv, hist, 0.20, 0.18)
        assert result["iv_rank"] == pytest.approx(100.0, abs=0.1)

    def test_iv_rank_at_min(self):
        hist = _make_iv_history(60, base_iv=0.20, step=0.001)
        min_iv = hist["iv"].min()
        result = compute_iv_metrics("AAPL", min_iv, hist, 0.20, 0.18)
        assert result["iv_rank"] == pytest.approx(0.0, abs=0.1)

    def test_iv_hv_ratio_computed(self):
        hist = _make_iv_history(60)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.20, 0.18)
        assert result["iv_hv_ratio_20d"] == pytest.approx(0.30 / 0.20, rel=0.01)

    def test_iv_hv_ratio_nan_when_hv_zero(self):
        hist = _make_iv_history(60)
        result = compute_iv_metrics("AAPL", 0.30, hist, 0.0, 0.18)
        assert np.isnan(result["iv_hv_ratio_20d"])

    def test_elevated_true_all_filters_pass(self):
        # Build history so current_iv sits at the top of the range
        hist = _make_iv_history(60, base_iv=0.15, step=0.001)
        max_iv = hist["iv"].max()
        current_iv = max_iv * 1.05  # slightly above max → percentile=100, IVR>100 (clamped by formula)
        result = compute_iv_metrics(
            "AAPL",
            current_iv=current_iv,
            iv_history=hist,
            hv_20d=current_iv / 2,    # iv/hv = 2.0 > 1.30 ✓
            hv_252d=current_iv / 2,
        )
        assert result["iv_percentile"] == pytest.approx(100.0)
        # IVR above 100 when current_iv > historical max — still passes MIN_IV_RANK
        assert result["iv_rank"] >= MIN_IV_RANK
        assert result["iv_hv_ratio_20d"] == pytest.approx(2.0, rel=0.01)
        assert result["elevated"] == True

    def test_elevated_false_when_iv_hv_ratio_too_low(self):
        hist = _make_iv_history(60, base_iv=0.15)
        result = compute_iv_metrics(
            "AAPL",
            current_iv=0.50,
            iv_history=hist,
            hv_20d=0.45,   # iv/hv = 0.50/0.45 ≈ 1.11 < 1.30 ✗
            hv_252d=0.20,
        )
        assert result["elevated"] is False

    def test_elevated_false_when_iv_percentile_too_low(self):
        # current_iv below median of history → percentile < 50
        hist = _make_iv_history(60, base_iv=0.30)
        result = compute_iv_metrics(
            "AAPL",
            current_iv=0.10,  # below all history
            iv_history=hist,
            hv_20d=0.05,   # ratio is high but percentile fails
            hv_252d=0.04,
        )
        assert result["elevated"] is False


# ---------------------------------------------------------------------------
# assess_vix_environment
# ---------------------------------------------------------------------------

class TestAssessVixEnvironment:
    def test_nan_input_returns_unknown(self):
        result = assess_vix_environment(float("nan"))
        assert result["environment"] == "unknown"
        assert result["trade_ok"] is False

    def test_green_below_caution(self):
        result = assess_vix_environment(VIX_CAUTION_LEVEL - 1)
        assert result["environment"] == "green"
        assert result["trade_ok"] is True

    def test_caution_between_levels(self):
        mid = (VIX_CAUTION_LEVEL + VIX_STANDBY_LEVEL) / 2
        result = assess_vix_environment(mid)
        assert result["environment"] == "caution"
        assert result["trade_ok"] is True

    def test_standby_above_standby_level(self):
        result = assess_vix_environment(VIX_STANDBY_LEVEL + 5)
        assert result["environment"] == "standby"
        assert result["trade_ok"] is False

    def test_vix_proxy_expressed_as_percentage(self):
        # vix_proxy should equal spy_iv * 100
        result = assess_vix_environment(0.18)
        assert result["vix_proxy"] == pytest.approx(18.0, abs=0.01)

    def test_returns_all_keys(self):
        result = assess_vix_environment(0.15)
        for key in ["spy_iv", "vix_proxy", "environment", "trade_ok", "note"]:
            assert key in result


# ---------------------------------------------------------------------------
# compute_universe_iv_metrics
# ---------------------------------------------------------------------------

class TestComputeUniverseIvMetrics:
    def _make_snapshots(self):
        return {
            "SPY": {"iv": 0.15, "spread_ok": True},
            "AAPL": {"iv": 0.35, "spread_ok": True},
            "MSFT": {"iv": 0.28, "spread_ok": False},
        }

    def _make_daily_data(self, n=260):
        return {
            "SPY":  _make_price_df(n),
            "AAPL": _make_price_df(n, daily_return=0.012),
            "MSFT": _make_price_df(n, daily_return=0.008),
        }

    def test_returns_tuple(self):
        snaps = self._make_snapshots()
        daily = self._make_daily_data()
        loader = lambda sym: _make_iv_history(60)
        vix_env, df = compute_universe_iv_metrics(snaps, daily, loader)
        assert isinstance(vix_env, dict)
        assert isinstance(df, pd.DataFrame)

    def test_spy_excluded_from_metrics_df(self):
        snaps = self._make_snapshots()
        daily = self._make_daily_data()
        loader = lambda sym: _make_iv_history(60)
        _, df = compute_universe_iv_metrics(snaps, daily, loader)
        assert "SPY" not in df["symbol"].values

    def test_stock_symbols_in_metrics_df(self):
        snaps = self._make_snapshots()
        daily = self._make_daily_data()
        loader = lambda sym: _make_iv_history(60)
        _, df = compute_universe_iv_metrics(snaps, daily, loader)
        assert set(df["symbol"].values) == {"AAPL", "MSFT"}

    def test_vix_env_reflects_spy_iv(self):
        snaps = self._make_snapshots()
        daily = self._make_daily_data()
        loader = lambda sym: _make_iv_history(60)
        vix_env, _ = compute_universe_iv_metrics(snaps, daily, loader)
        assert vix_env["spy_iv"] == pytest.approx(0.15, abs=0.001)

    def test_spread_ok_carried_through(self):
        snaps = self._make_snapshots()
        daily = self._make_daily_data()
        loader = lambda sym: _make_iv_history(60)
        _, df = compute_universe_iv_metrics(snaps, daily, loader)
        msft_row = df[df["symbol"] == "MSFT"].iloc[0]
        assert not msft_row["spread_ok"]

    def test_sorted_by_iv_percentile_descending(self):
        snaps = self._make_snapshots()
        daily = self._make_daily_data()
        loader = lambda sym: _make_iv_history(60)
        _, df = compute_universe_iv_metrics(snaps, daily, loader)
        if len(df) > 1:
            percentiles = df["iv_percentile"].dropna().tolist()
            assert percentiles == sorted(percentiles, reverse=True)

    def test_empty_snapshots_returns_empty_df(self):
        loader = lambda sym: pd.DataFrame(columns=["date", "iv"])
        vix_env, df = compute_universe_iv_metrics({}, {}, loader)
        assert df.empty
        assert np.isnan(vix_env["spy_iv"])

    def test_no_daily_data_still_computes(self):
        snaps = {"SPY": {"iv": 0.15, "spread_ok": True}, "AAPL": {"iv": 0.35, "spread_ok": True}}
        loader = lambda sym: _make_iv_history(60)
        vix_env, df = compute_universe_iv_metrics(snaps, {}, loader)
        # HV will be nan but metrics should still return
        assert "AAPL" in df["symbol"].values
        assert np.isnan(df[df["symbol"] == "AAPL"]["hv_20d"].iloc[0])
