"""
IV metrics engine.

Takes raw IV snapshots and historical price data and computes:
  - HV (historical/realized volatility) — 20-day and 252-day
  - IVR (IV Rank) — where current IV sits in its 52-week range
  - IV Percentile — % of days in history where IV was lower than today
  - IV/HV ratio — market overpayment vs realized behavior

Also computes a VIX proxy from SPY's IV snapshot, used as the
environment gate before stock-level filters are applied.
"""

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.models.universe import BENCHMARK

logger = logging.getLogger(__name__)

# -------------------------
# Signal quality thresholds
# -------------------------
# These are the filter thresholds for the credit spread candidate screen.
# All three must pass for a stock to be flagged as a candidate.

MIN_IV_PERCENTILE = 50.0   # primary screen — IV elevated vs own history
MIN_IV_RANK       = 50.0   # confirms percentile isn't an outlier artifact
MIN_IV_HV_RATIO   = 1.30   # market overpaying vs realized vol

# VIX proxy environment gate
VIX_CAUTION_LEVEL  = 20.0  # above this: reduce size, be selective
VIX_STANDBY_LEVEL  = 30.0  # above this: stand aside

# HV lookback periods
HV_SHORT_LB  = 20    # 20-day HV — recent regime
HV_LONG_LB   = 252   # 252-day HV — structural baseline


# -------------------------
# Historical Volatility
# -------------------------

def compute_hv(daily_df: pd.DataFrame, lookback: int) -> float:
    """
    Annualized historical volatility from daily log returns.

    Uses closing prices over `lookback` bars.
    Annualized by multiplying by sqrt(252) — the number of trading days per year.

    Returns nan if insufficient data.
    """
    if len(daily_df) < lookback + 1:
        return np.nan

    closes = daily_df["close"].iloc[-(lookback + 1):]
    log_returns = np.log(closes / closes.shift(1)).dropna()

    if len(log_returns) < lookback:
        return np.nan

    return float(log_returns.std() * np.sqrt(252))


# -------------------------
# IV Metrics
# -------------------------

def compute_iv_metrics(
    symbol: str,
    current_iv: float,
    iv_history: pd.DataFrame,
    hv_20d: float,
    hv_252d: float,
) -> dict:
    """
    Compute IVR, IV percentile, and IV/HV ratios for one symbol.

    Parameters
    ----------
    symbol      : ticker
    current_iv  : today's IV reading (averaged 30-delta call + put)
    iv_history  : DataFrame with columns ['date', 'iv'] — historical daily readings
    hv_20d      : 20-day annualized HV from price data
    hv_252d     : 252-day annualized HV from price data

    Returns a dict of all computed metrics plus a signal_quality label
    describing how much history we have and how much to trust the output.
    """
    base = {
        "symbol":          symbol,
        "current_iv":      round(current_iv, 4),
        "hv_20d":          round(hv_20d, 4)  if not np.isnan(hv_20d)  else np.nan,
        "hv_252d":         round(hv_252d, 4) if not np.isnan(hv_252d) else np.nan,
        "iv_rank":         np.nan,
        "iv_percentile":   np.nan,
        "iv_hv_ratio_20d": np.nan,
        "iv_hv_ratio_252d":np.nan,
        "history_days":    0,
        "signal_quality":  "no_history",
        "elevated":        False,
    }

    if iv_history.empty:
        return base

    hist_iv = iv_history["iv"].dropna()
    n = len(hist_iv)
    base["history_days"] = n

    if n < 5:
        base["signal_quality"] = "too_short"
        return base

    # --- IV Rank ---
    # Where does current IV sit between the rolling high and low?
    iv_high = hist_iv.max()
    iv_low  = hist_iv.min()
    if iv_high > iv_low:
        iv_rank = (current_iv - iv_low) / (iv_high - iv_low) * 100
    else:
        iv_rank = np.nan

    # --- IV Percentile ---
    # What % of past days had IV lower than today?
    iv_percentile = float((hist_iv < current_iv).mean() * 100)

    # --- IV/HV Ratios ---
    # Is the market overpaying vs what the stock has actually been doing?
    iv_hv_20d  = current_iv / hv_20d  if not np.isnan(hv_20d)  and hv_20d  > 0 else np.nan
    iv_hv_252d = current_iv / hv_252d if not np.isnan(hv_252d) and hv_252d > 0 else np.nan

    # --- Signal quality label ---
    if n < 20:
        quality = "too_short"
    elif n < 60:
        quality = "early"
    elif n < 120:
        quality = "developing"
    elif n < 252:
        quality = "maturing"
    else:
        quality = "full"

    # --- Elevation flag ---
    # All three primary filters must pass
    elevated = (
        not np.isnan(iv_percentile) and iv_percentile >= MIN_IV_PERCENTILE
        and not np.isnan(iv_rank)   and iv_rank       >= MIN_IV_RANK
        and not np.isnan(iv_hv_20d) and iv_hv_20d     >= MIN_IV_HV_RATIO
    )

    return {
        "symbol":           symbol,
        "current_iv":       round(current_iv, 4),
        "hv_20d":           round(hv_20d, 4)       if not np.isnan(hv_20d)      else np.nan,
        "hv_252d":          round(hv_252d, 4)       if not np.isnan(hv_252d)     else np.nan,
        "iv_rank":          round(iv_rank, 1)        if not np.isnan(iv_rank)     else np.nan,
        "iv_percentile":    round(iv_percentile, 1),
        "iv_hv_ratio_20d":  round(iv_hv_20d, 2)     if not np.isnan(iv_hv_20d)  else np.nan,
        "iv_hv_ratio_252d": round(iv_hv_252d, 2)    if not np.isnan(iv_hv_252d) else np.nan,
        "history_days":     n,
        "signal_quality":   quality,
        "elevated":         elevated,
    }


# -------------------------
# VIX proxy
# -------------------------

def assess_vix_environment(spy_iv: float) -> dict:
    """
    Classify the market volatility environment using SPY's IV as a VIX proxy.

    Returns a dict with:
      - spy_iv        : the raw IV reading
      - vix_proxy     : same value, labelled for clarity
      - environment   : 'green' / 'caution' / 'standby'
      - trade_ok      : bool — whether to proceed with stock-level filters
      - note          : plain English description
    """
    if np.isnan(spy_iv):
        return {
            "spy_iv":      np.nan,
            "vix_proxy":   np.nan,
            "environment": "unknown",
            "trade_ok":    False,
            "note":        "Could not compute SPY IV — environment gate unavailable.",
        }

    if spy_iv < VIX_CAUTION_LEVEL:
        env, ok, note = "green",   True,  "Calm environment. Idiosyncratic IV elevated on candidates is meaningful."
    elif spy_iv < VIX_STANDBY_LEVEL:
        env, ok, note = "caution", True,  "Elevated systemic vol. Size smaller. Be selective."
    else:
        env, ok, note = "standby", False, "High systemic vol. Stand aside — IV elevation may be noise, not signal."

    return {
        "spy_iv":      round(spy_iv, 4),
        "vix_proxy":   round(spy_iv * 100, 2),   # express as percentage like VIX
        "environment": env,
        "trade_ok":    ok,
        "note":        note,
    }


# -------------------------
# Full universe IV scan
# -------------------------

def compute_universe_iv_metrics(
    iv_snapshots: dict[str, dict],
    daily_data: dict[str, pd.DataFrame],
    iv_history_loader,           # callable: symbol -> pd.DataFrame
) -> tuple[dict, pd.DataFrame]:
    """
    For every symbol with a valid IV snapshot:
      1. Compute HV from existing daily price data
      2. Load that symbol's IV history from logged CSVs
      3. Compute IVR, IV percentile, IV/HV ratio

    Also extracts the SPY VIX proxy separately.

    Parameters
    ----------
    iv_snapshots      : {symbol: snapshot_dict} from iv_ingestion
    daily_data        : {symbol: DataFrame} from get_daily_batch — already in memory
    iv_history_loader : callable(symbol) -> DataFrame with ['date','iv'] columns

    Returns
    -------
    (vix_environment_dict, metrics_DataFrame)
    """
    # --- VIX proxy first (environment gate) ---
    spy_snap = iv_snapshots.get(BENCHMARK)
    spy_iv   = spy_snap["iv"] if spy_snap else float("nan")
    vix_env  = assess_vix_environment(spy_iv)

    rows = []

    for symbol, snap in iv_snapshots.items():
        if symbol == BENCHMARK:
            continue  # SPY handled separately as VIX proxy

        current_iv = snap["iv"]
        daily_df   = daily_data.get(symbol, pd.DataFrame())

        hv_20d  = compute_hv(daily_df, HV_SHORT_LB)
        hv_252d = compute_hv(daily_df, HV_LONG_LB)

        iv_hist = iv_history_loader(symbol)

        metrics = compute_iv_metrics(symbol, current_iv, iv_hist, hv_20d, hv_252d)

        # Carry through spread quality flag from ingestion
        metrics["spread_ok"] = snap.get("spread_ok", True)

        rows.append(metrics)

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.sort_values("iv_percentile", ascending=False).reset_index(drop=True)

    return vix_env, df
