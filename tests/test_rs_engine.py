"""
Unit tests for the RS scoring engine.

Uses synthetic price data so no API calls are required.
Run with:  pytest tests/
"""

import numpy as np
import pandas as pd
import pytest

from src.utils.rs_engine import (
    compute_slope,
    compute_relative_strength,
    compute_relative_volume,
    compute_volatility_ratio,
    sign,
    zscore,
    compute_stock_rs,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_price_series(values: list[float], name: str = "close") -> pd.Series:
    return pd.Series(values, name=name)


def _make_ohlcv(close_values: list[float]) -> pd.DataFrame:
    """Minimal OHLCV DataFrame from a close series."""
    close = pd.Series(close_values)
    return pd.DataFrame({
        "open":   close * 0.999,
        "high":   close * 1.005,
        "low":    close * 0.995,
        "close":  close,
        "volume": [1_000_000] * len(close),
    })


def _make_data_dict(symbols_prices: dict[str, list[float]]) -> dict[str, pd.DataFrame]:
    """Build a {symbol: DataFrame} dict for compute_stock_rs."""
    return {sym: _make_ohlcv(prices) for sym, prices in symbols_prices.items()}


# ---------------------------------------------------------------------------
# compute_slope
# ---------------------------------------------------------------------------

class TestComputeSlope:
    def test_uptrend_positive_slope(self):
        # Steadily rising prices → positive slope
        prices = [100 * (1.01 ** i) for i in range(20)]
        slope = compute_slope(_make_price_series(prices), lookback=10)
        assert slope > 0

    def test_downtrend_negative_slope(self):
        prices = [100 * (0.99 ** i) for i in range(20)]
        slope = compute_slope(_make_price_series(prices), lookback=10)
        assert slope < 0

    def test_flat_near_zero(self):
        prices = [100.0] * 20
        slope = compute_slope(_make_price_series(prices), lookback=10)
        assert abs(slope) < 1e-10

    def test_insufficient_data_returns_nan(self):
        prices = [100.0] * 5
        result = compute_slope(_make_price_series(prices), lookback=10)
        assert np.isnan(result)


# ---------------------------------------------------------------------------
# compute_relative_strength
# ---------------------------------------------------------------------------

class TestComputeRelativeStrength:
    def test_outperforming_stock_positive(self):
        # Stock up 10%, benchmark up 5% → RS > 0
        stock = _make_price_series([100.0] * 9 + [110.0])
        bench = _make_price_series([100.0] * 9 + [105.0])
        rs = compute_relative_strength(stock, bench, lookback=10)
        assert rs > 0

    def test_underperforming_stock_negative(self):
        stock = _make_price_series([100.0] * 9 + [95.0])
        bench = _make_price_series([100.0] * 9 + [105.0])
        rs = compute_relative_strength(stock, bench, lookback=10)
        assert rs < 0

    def test_equal_performance_near_zero(self):
        prices = _make_price_series([100.0] * 9 + [105.0])
        rs = compute_relative_strength(prices, prices, lookback=10)
        assert abs(rs) < 1e-10

    def test_insufficient_data_returns_nan(self):
        stock = _make_price_series([100.0, 105.0])
        bench = _make_price_series([100.0, 103.0])
        assert np.isnan(compute_relative_strength(stock, bench, lookback=10))


# ---------------------------------------------------------------------------
# compute_relative_volume
# ---------------------------------------------------------------------------

class TestComputeRelativeVolume:
    def test_above_average_greater_than_one(self):
        volume = _make_price_series([1_000_000] * 19 + [3_000_000])
        rvol = compute_relative_volume(volume, lookback=20)
        assert rvol > 1.0

    def test_below_average_less_than_one(self):
        volume = _make_price_series([1_000_000] * 19 + [100_000])
        rvol = compute_relative_volume(volume, lookback=20)
        assert rvol < 1.0

    def test_zero_average_returns_nan(self):
        volume = _make_price_series([0.0] * 20)
        assert np.isnan(compute_relative_volume(volume, lookback=20))

    def test_insufficient_data_returns_nan(self):
        volume = _make_price_series([1_000_000] * 5)
        assert np.isnan(compute_relative_volume(volume, lookback=20))


# ---------------------------------------------------------------------------
# sign
# ---------------------------------------------------------------------------

class TestSign:
    def test_positive(self):   assert sign(1.5) == 1
    def test_negative(self):   assert sign(-0.5) == -1
    def test_zero(self):       assert sign(0.0) == 0
    def test_nan(self):        assert sign(float("nan")) == 0


# ---------------------------------------------------------------------------
# zscore
# ---------------------------------------------------------------------------

class TestZscore:
    def test_output_mean_near_zero(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        z = zscore(s)
        assert abs(z.mean()) < 1e-10

    def test_output_std_near_one(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        z = zscore(s)
        assert abs(z.std() - 1.0) < 0.01

    def test_constant_series_returns_zeros(self):
        s = pd.Series([5.0] * 10)
        z = zscore(s)
        assert (z == 0.0).all()

    def test_preserves_rank_order(self):
        s = pd.Series([10.0, 20.0, 30.0])
        z = zscore(s)
        assert z.iloc[0] < z.iloc[1] < z.iloc[2]


# ---------------------------------------------------------------------------
# compute_stock_rs — end-to-end
# ---------------------------------------------------------------------------

class TestComputeStockRS:
    def _make_universe(self) -> tuple[dict, dict]:
        """
        Three stocks with clearly different trend profiles + SPY as benchmark.
        Strong: consistently up 1.5% per bar
        Neutral: flat
        Weak: consistently down 1% per bar
        """
        n = 30
        spy    = [100 * (1.005 ** i) for i in range(n)]
        strong = [100 * (1.015 ** i) for i in range(n)]
        neutral= [100.0] * n
        weak   = [100 * (0.990 ** i) for i in range(n)]

        daily = _make_data_dict({"SPY": spy, "STRONG": strong, "NEUTRAL": neutral, "WEAK": weak})
        weekly = _make_data_dict({"SPY": spy[:10], "STRONG": strong[:10], "NEUTRAL": neutral[:10], "WEAK": weak[:10]})
        return daily, weekly

    def test_returns_dataframe(self):
        daily, weekly = self._make_universe()
        result = compute_stock_rs(daily, weekly)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_sorted_descending(self):
        daily, weekly = self._make_universe()
        result = compute_stock_rs(daily, weekly)
        scores = result["composite_score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_strong_ranks_above_weak(self):
        daily, weekly = self._make_universe()
        result = compute_stock_rs(daily, weekly)
        result = result.set_index("symbol")
        assert result.loc["STRONG", "composite_score"] > result.loc["WEAK", "composite_score"]

    def test_required_columns_present(self):
        daily, weekly = self._make_universe()
        result = compute_stock_rs(daily, weekly)
        for col in ["symbol", "composite_score", "weekly_score", "daily_score", "aligned"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_spy_not_in_results(self):
        """SPY is the benchmark — it should not appear as a scored stock."""
        daily, weekly = self._make_universe()
        result = compute_stock_rs(daily, weekly)
        assert "SPY" not in result["symbol"].values

    def test_missing_spy_raises(self):
        daily = _make_data_dict({"AAPL": [100.0] * 20})
        weekly = _make_data_dict({"AAPL": [100.0] * 10})
        with pytest.raises(ValueError, match="SPY missing"):
            compute_stock_rs(daily, weekly)

    def test_empty_result_when_no_stocks(self):
        """Only SPY in the data → no scoreable stocks → empty DataFrame."""
        daily = _make_data_dict({"SPY": [100.0] * 20})
        weekly = _make_data_dict({"SPY": [100.0] * 10})
        result = compute_stock_rs(daily, weekly)
        assert result.empty
