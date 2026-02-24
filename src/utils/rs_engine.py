# MODEL v1.3 — z-score normalized components

from src.models.sector_map import SECTOR_MAP
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# -------------------------
# Helpers
# -------------------------

def compute_slope(series: pd.Series, lookback: int) -> float:
    """Log-linear slope over lookback bars."""
    if len(series) < lookback:
        return np.nan
    y = np.log(series.iloc[-lookback:].to_numpy(dtype=float))
    x = np.arange(len(y), dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


def compute_relative_strength(stock_close: pd.Series,
                              bench_close: pd.Series,
                              lookback: int) -> float:
    """Log-return of stock minus log-return of benchmark."""
    if len(stock_close) < lookback or len(bench_close) < lookback:
        return np.nan
    stock_ret = np.log(stock_close.iloc[-1]) - np.log(stock_close.iloc[-lookback])
    bench_ret = np.log(bench_close.iloc[-1]) - np.log(bench_close.iloc[-lookback])
    return stock_ret - bench_ret


def compute_relative_volume(volume: pd.Series, lookback: int) -> float:
    """Current bar volume / average volume over lookback. >1 = above-average."""
    if len(volume) < lookback:
        return np.nan
    avg_vol = volume.iloc[-lookback:].mean()
    if avg_vol == 0:
        return np.nan
    return float(volume.iloc[-1] / avg_vol)


def compute_volatility_ratio(stock_close: pd.Series,
                             bench_close: pd.Series,
                             lookback: int) -> float:
    """
    Stock realized vol / benchmark realized vol.
    <1 = stock is calmer than benchmark.
    """
    if len(stock_close) < lookback or len(bench_close) < lookback:
        return np.nan
    stock_log_returns = np.log(stock_close / stock_close.shift(1)).iloc[-lookback:] # pyright: ignore[reportAttributeAccessIssue]
    bench_log_returns = np.log(bench_close / bench_close.shift(1)).iloc[-lookback:] # pyright: ignore[reportAttributeAccessIssue]
    stock_vol = stock_log_returns.std()
    bench_vol = bench_log_returns.std()
    if bench_vol == 0:
        return np.nan
    return float(stock_vol / bench_vol)


def sign(x: float) -> int:
    if pd.isna(x):
        return 0
    return 1 if x > 0 else (-1 if x < 0 else 0)


def zscore(series: pd.Series) -> pd.Series:
    """Z-score normalize a series. Returns 0 for NaN or zero-std."""
    mean = series.mean()
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    return (series - mean) / std


# -------------------------
# Scoring
# -------------------------

W_SLOPE   = 0.35
W_RS      = 0.35
W_RVOL    = 0.15
W_VOL_ADJ = 0.15

W_WEEKLY  = 0.40
W_DAILY   = 0.35
W_HOURLY  = 0.25

BENCHMARK = "SPY"


def _collect_raw_components(stock_df: pd.DataFrame,
                            bench_df: pd.DataFrame,
                            slope_lb: int,
                            rs_lb: int,
                            rvol_lb: int,
                            vol_lb: int) -> dict:
    """Compute raw indicator values for a single symbol + timeframe."""
    return {
        "slope": compute_slope(stock_df["close"], slope_lb),
        "rs": compute_relative_strength(stock_df["close"], bench_df["close"], rs_lb),
        "rvol": compute_relative_volume(stock_df["volume"], rvol_lb),
        "vol_ratio": compute_volatility_ratio(stock_df["close"], bench_df["close"], vol_lb),
    }


def _zscore_and_score(raw_df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Take a DataFrame of raw components, z-score them across the universe,
    then compute a blended score per row.
    """
    df = raw_df.copy()

    # Z-score each component across the universe
    z_slope = zscore(df[f"{prefix}_slope"])
    z_rs    = zscore(df[f"{prefix}_rs"])
    z_rvol  = zscore(df[f"{prefix}_rvol"])
    # Invert vol_ratio: lower relative vol = better → negate before z-scoring
    z_vol   = zscore(-df[f"{prefix}_vol_ratio"])

    df[f"{prefix}_z_slope"]  = z_slope
    df[f"{prefix}_z_rs"]     = z_rs
    df[f"{prefix}_z_rvol"]   = z_rvol
    df[f"{prefix}_z_vol"]    = z_vol

    df[f"{prefix}_score"] = (
        W_SLOPE   * z_slope.fillna(0)
      + W_RS      * z_rs.fillna(0)
      + W_RVOL    * z_rvol.fillna(0)
      + W_VOL_ADJ * z_vol.fillna(0)
    )

    df[f"{prefix}_bias"] = df[f"{prefix}_score"].apply(sign)

    return df


# -------------------------
# Main Engine
# -------------------------

def compute_stock_rs(data_daily: dict,
                     data_weekly: dict,
                     data_hourly: dict | None = None) -> pd.DataFrame:

    spy_daily  = data_daily.get(BENCHMARK)
    spy_weekly = data_weekly.get(BENCHMARK)
    spy_hourly = data_hourly.get(BENCHMARK) if data_hourly else None

    if spy_daily is None or spy_weekly is None:
        raise ValueError(f"{BENCHMARK} missing from daily or weekly data")

    has_hourly = data_hourly is not None and spy_hourly is not None

    # --- Pass 1: Collect raw components for every symbol ---
    rows = []
    symbols_used = []

    for symbol in data_daily:
        if symbol == BENCHMARK:
            continue
        if symbol not in data_weekly:
            logger.warning(f"{symbol}: no weekly data — skipping")
            continue

        daily_df  = data_daily[symbol]
        weekly_df = data_weekly[symbol]

        w = _collect_raw_components(weekly_df, spy_weekly,
                                    slope_lb=4, rs_lb=4, rvol_lb=4, vol_lb=4)
        d = _collect_raw_components(daily_df, spy_daily,
                                    slope_lb=5, rs_lb=10, rvol_lb=10, vol_lb=10)

        row = {
            "symbol": symbol,
            "sector": SECTOR_MAP.get(symbol),
            "weekly_slope": w["slope"],
            "weekly_rs": w["rs"],
            "weekly_rvol": w["rvol"],
            "weekly_vol_ratio": w["vol_ratio"],
            "daily_slope": d["slope"],
            "daily_rs": d["rs"],
            "daily_rvol": d["rvol"],
            "daily_vol_ratio": d["vol_ratio"],
        }

        if has_hourly and data_hourly is not None and symbol in data_hourly and spy_hourly is not None:
            h = _collect_raw_components(
                data_hourly[symbol], spy_hourly,
                slope_lb=10, rs_lb=20, rvol_lb=20, vol_lb=20,
            )
            row.update({
                "hourly_slope": h["slope"],
                "hourly_rs": h["rs"],
                "hourly_rvol": h["rvol"],
                "hourly_vol_ratio": h["vol_ratio"],
            })

        rows.append(row)
        symbols_used.append(symbol)

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # --- Pass 2: Z-score normalize across the universe, then score ---
    df = _zscore_and_score(df, "weekly")
    df = _zscore_and_score(df, "daily")

    if has_hourly and "hourly_slope" in df.columns:
        df = _zscore_and_score(df, "hourly")
        df["composite_score"] = (
            W_WEEKLY * df["weekly_score"]
          + W_DAILY  * df["daily_score"]
          + W_HOURLY * df["hourly_score"]
        )
        biases = df[["weekly_bias", "daily_bias", "hourly_bias"]]
    else:
        df["composite_score"] = (
            0.55 * df["weekly_score"]
          + 0.45 * df["daily_score"]
        )
        biases = df[["weekly_bias", "daily_bias"]]

    # Aligned = all biases agree and none are zero
    df["aligned"] = biases.apply(
        lambda r: len(set(r)) == 1 and 0 not in set(r), axis=1
    )

    df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)

    return df