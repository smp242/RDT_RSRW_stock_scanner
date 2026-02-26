"""
Spot scan engine — intraday momentum analysis for a single symbol
or the full universe.

Computes:
  - Multi-timeframe intraday RS vs SPY (1H, 15m, 5m)
  - Weekly / Daily / Hourly ATR
  - Range consumed (daily + weekly)
  - Intraday relative volume
  - Distance from key levels
"""

import numpy as np
import pandas as pd
import logging

from src.models.sector_map import SECTOR_MAP

logger = logging.getLogger(__name__)

BENCHMARK = "SPY"


# -------------------------
# ATR
# -------------------------

def compute_atr(df: pd.DataFrame, lookback: int = 14) -> float:
    """
    Average True Range over `lookback` bars.
    Expects DataFrame with 'high', 'low', 'close' columns.
    """
    if len(df) < lookback + 1:
        return np.nan

    high = df["high"].iloc[-(lookback + 1):]
    low = df["low"].iloc[-(lookback + 1):]
    close = df["close"].iloc[-(lookback + 1):]

    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Drop the first NaN from shift
    tr = tr.iloc[1:]

    return float(tr.rolling(lookback).mean().iloc[-1])


# -------------------------
# Range Consumed
# -------------------------

def compute_range_consumed(df: pd.DataFrame, atr: float) -> float:
    """
    How much of the expected range (ATR) has been consumed today/this period.
    (current_price - period_low) / ATR
    """
    if pd.isna(atr) or atr == 0 or df.empty:
        return np.nan

    current = float(df["close"].iloc[-1])
    period_low = float(df["low"].iloc[-1]) if len(df) == 1 else float(df["low"].min())

    return float((current - period_low) / atr)


def compute_daily_range_consumed(daily_df: pd.DataFrame, daily_atr: float) -> float:
    """Range consumed for the most recent trading day."""
    if daily_df.empty or pd.isna(daily_atr) or daily_atr == 0:
        return np.nan

    last_row = daily_df.iloc[-1]
    current = float(last_row["close"])
    day_low = float(last_row["low"])

    return float((current - day_low) / daily_atr)


def compute_weekly_range_consumed(weekly_df: pd.DataFrame, weekly_atr: float) -> float:
    """Range consumed for the most recent trading week."""
    if weekly_df.empty or pd.isna(weekly_atr) or weekly_atr == 0:
        return np.nan

    last_row = weekly_df.iloc[-1]
    current = float(last_row["close"])
    week_low = float(last_row["low"])

    return float((current - week_low) / weekly_atr)


# -------------------------
# Intraday RS vs Benchmark
# -------------------------

def compute_intraday_rs(stock_close: pd.Series,
                        bench_close: pd.Series,
                        lookback: int) -> float:
    """Log-return RS over lookback bars (intraday timeframe)."""
    if len(stock_close) < lookback or len(bench_close) < lookback:
        return np.nan
    stock_ret = np.log(stock_close.iloc[-1]) - np.log(stock_close.iloc[-lookback])  # pyright: ignore[reportAttributeAccessIssue]
    bench_ret = np.log(bench_close.iloc[-1]) - np.log(bench_close.iloc[-lookback])  # pyright: ignore[reportAttributeAccessIssue]
    return float(stock_ret - bench_ret)


# -------------------------
# Intraday Score (per timeframe)
# -------------------------

def _score_intraday_tf(stock_df: pd.DataFrame,
                       bench_df: pd.DataFrame,
                       rs_lb: int,
                       atr_lb: int = 14) -> dict:
    """Score a single intraday timeframe."""
    rs = compute_intraday_rs(stock_df["close"], bench_df["close"], rs_lb)
    atr = compute_atr(stock_df, atr_lb)

    # Relative volume: last bar vs average
    if len(stock_df) >= rs_lb:
        avg_vol = stock_df["volume"].iloc[-rs_lb:].mean()
        last_vol = float(stock_df["volume"].iloc[-1])
        rvol = last_vol / avg_vol if avg_vol > 0 else np.nan
    else:
        rvol = np.nan

    return {
        "rs": rs,
        "atr": atr,
        "rvol": rvol,
    }


# -------------------------
# Distance from Key Levels
# -------------------------

def compute_levels(daily_df: pd.DataFrame, lookback_20d: int = 20) -> dict:
    """
    Compute distance from key price levels.
    """
    if daily_df.empty:
        return {
            "price": np.nan,
            "daily_high": np.nan,
            "daily_low": np.nan,
            "pct_from_daily_high": np.nan,
            "pct_from_daily_low": np.nan,
            "high_20d": np.nan,
            "low_20d": np.nan,
            "pct_from_20d_high": np.nan,
            "pct_from_20d_low": np.nan,
        }

    current = float(daily_df["close"].iloc[-1])
    last_day = daily_df.iloc[-1]
    daily_high = float(last_day["high"])
    daily_low = float(last_day["low"])

    recent = daily_df.iloc[-lookback_20d:] if len(daily_df) >= lookback_20d else daily_df
    high_20d = float(recent["high"].max())
    low_20d = float(recent["low"].min())

    return {
        "price": current,
        "daily_high": daily_high,
        "daily_low": daily_low,
        "pct_from_daily_high": (current - daily_high) / current * 100 if current else np.nan,
        "pct_from_daily_low": (current - daily_low) / current * 100 if current else np.nan,
        "high_20d": high_20d,
        "low_20d": low_20d,
        "pct_from_20d_high": (current - high_20d) / current * 100 if current else np.nan,
        "pct_from_20d_low": (current - low_20d) / current * 100 if current else np.nan,
    }


# -------------------------
# Intraday Momentum Composite
# -------------------------

W_1H  = 0.50
W_15M = 0.30
W_5M  = 0.20


def _sign(x: float) -> int:
    if pd.isna(x):
        return 0
    return 1 if x > 0 else (-1 if x < 0 else 0)


# -------------------------
# Single Symbol Spot Scan
# -------------------------

def spot_scan_symbol(
    symbol: str,
    data_daily: dict,
    data_weekly: dict,
    data_1h: dict,
    data_15m: dict,
    data_5m: dict,
) -> dict | None:
    """
    Full spot scan for a single symbol.
    Returns a dict of all metrics, or None if data is missing.
    """
    spy_daily  = data_daily.get(BENCHMARK)
    spy_weekly = data_weekly.get(BENCHMARK)
    spy_1h     = data_1h.get(BENCHMARK)
    spy_15m    = data_15m.get(BENCHMARK)
    spy_5m     = data_5m.get(BENCHMARK)

    stock_daily  = data_daily.get(symbol)
    stock_weekly = data_weekly.get(symbol)
    stock_1h     = data_1h.get(symbol)
    stock_15m    = data_15m.get(symbol)
    stock_5m     = data_5m.get(symbol)

    if stock_daily is None or spy_daily is None:
        logger.warning(f"{symbol}: missing daily data — skipping")
        return None

    # --- ATR ---
    weekly_atr = compute_atr(stock_weekly, 14) if stock_weekly is not None else np.nan
    daily_atr  = compute_atr(stock_daily, 14)
    hourly_atr = compute_atr(stock_1h, 14) if stock_1h is not None else np.nan

    # --- Range Consumed ---
    daily_consumed  = compute_daily_range_consumed(stock_daily, daily_atr)
    weekly_consumed = compute_weekly_range_consumed(stock_weekly, weekly_atr) if stock_weekly is not None else np.nan

    # --- Key Levels ---
    levels = compute_levels(stock_daily)

    # --- Intraday RS + Score per timeframe ---
    h1 = None
    if stock_1h is not None and spy_1h is not None:
        h1 = _score_intraday_tf(stock_1h, spy_1h, rs_lb=10)

    m15 = None
    if stock_15m is not None and spy_15m is not None:
        m15 = _score_intraday_tf(stock_15m, spy_15m, rs_lb=16)

    m5 = None
    if stock_5m is not None and spy_5m is not None:
        m5 = _score_intraday_tf(stock_5m, spy_5m, rs_lb=12)

    # --- Intraday Momentum Composite ---
    scores = []
    if h1 is not None and not pd.isna(h1["rs"]):
        scores.append(("1h", h1["rs"], W_1H))
    if m15 is not None and not pd.isna(m15["rs"]):
        scores.append(("15m", m15["rs"], W_15M))
    if m5 is not None and not pd.isna(m5["rs"]):
        scores.append(("5m", m5["rs"], W_5M))

    if scores:
        total_weight = sum(w for _, _, w in scores)
        intraday_composite = sum(rs * w for _, rs, w in scores) / total_weight
    else:
        intraday_composite = np.nan

    # --- Intraday Volume (most recent bar vs average) ---
    if stock_5m is not None and len(stock_5m) >= 12:
        today_vol = float(stock_daily["volume"].iloc[-1]) if not stock_daily.empty else np.nan
        avg_vol_20d = float(stock_daily["volume"].iloc[-20:].mean()) if len(stock_daily) >= 20 else np.nan
        rvol_daily = today_vol / avg_vol_20d if avg_vol_20d and avg_vol_20d > 0 else np.nan
    else:
        rvol_daily = np.nan

    # --- Bias alignment ---
    biases = []
    if h1 is not None:
        biases.append(_sign(h1["rs"]))
    if m15 is not None:
        biases.append(_sign(m15["rs"]))
    if m5 is not None:
        biases.append(_sign(m5["rs"]))

    aligned = len(set(biases)) == 1 and 0 not in biases and len(biases) > 0

    return {
        "symbol": symbol,
        "sector": SECTOR_MAP.get(symbol),
        "price": levels["price"],

        # ATR
        "weekly_atr": weekly_atr,
        "daily_atr": daily_atr,
        "hourly_atr": hourly_atr,

        # Range consumed
        "daily_range_consumed": daily_consumed,
        "weekly_range_consumed": weekly_consumed,

        # Intraday RS
        "1h_rs": h1["rs"] if h1 else np.nan,
        "15m_rs": m15["rs"] if m15 else np.nan,
        "5m_rs": m5["rs"] if m5 else np.nan,

        # Intraday RVOL per timeframe
        "1h_rvol": h1["rvol"] if h1 else np.nan,
        "15m_rvol": m15["rvol"] if m15 else np.nan,
        "5m_rvol": m5["rvol"] if m5 else np.nan,

        # Intraday ATR
        "1h_atr": h1["atr"] if h1 else np.nan,
        "15m_atr": m15["atr"] if m15 else np.nan,
        "5m_atr": m5["atr"] if m5 else np.nan,

        # Daily volume context
        "rvol_daily": rvol_daily,

        # Composite
        "intraday_composite": intraday_composite,
        "intraday_bias": _sign(intraday_composite),
        "intraday_aligned": aligned,

        # Levels
        "daily_high": levels["daily_high"],
        "daily_low": levels["daily_low"],
        "pct_from_daily_high": levels["pct_from_daily_high"],
        "pct_from_daily_low": levels["pct_from_daily_low"],
        "high_20d": levels["high_20d"],
        "low_20d": levels["low_20d"],
        "pct_from_20d_high": levels["pct_from_20d_high"],
        "pct_from_20d_low": levels["pct_from_20d_low"],
    }


# -------------------------
# Full Universe Spot Scan
# -------------------------

def spot_scan_universe(
    data_daily: dict,
    data_weekly: dict,
    data_1h: dict,
    data_15m: dict,
    data_5m: dict,
) -> pd.DataFrame:
    """
    Run spot scan across the entire universe.
    Returns DataFrame sorted by intraday_composite descending.
    """
    results = []

    for symbol in data_daily:
        if symbol == BENCHMARK:
            continue

        row = spot_scan_symbol(
            symbol, data_daily, data_weekly,
            data_1h, data_15m, data_5m,
        )
        if row is not None:
            results.append(row)

    df = pd.DataFrame(results)

    if df.empty:
        return df

    df = df.sort_values("intraday_composite", ascending=False).reset_index(drop=True)
    return df