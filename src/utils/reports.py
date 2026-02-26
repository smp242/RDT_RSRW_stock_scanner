"""
Reporting layer — reads historical scans and produces
trend / tracking reports for the 30-session research phase.
"""

from pathlib import Path
import logging

import pandas as pd

from src.utils.logger import STOCK_DIR, SECTOR_DIR, REPORT_DIR, STRONG_DIR, WEAK_DIR

logger = logging.getLogger(__name__)


def _load_all_scans(scan_dir: Path) -> pd.DataFrame:
    """Load all CSV files in a scan directory into one DataFrame."""
    files = sorted(scan_dir.glob("*.csv"))
    if not files:
        logger.warning(f"No scan files found in {scan_dir}")
        return pd.DataFrame()

    dfs = [pd.read_csv(f) for f in files]
    combined = pd.concat(dfs, ignore_index=True)

    if "scan_date" in combined.columns:
        combined["scan_date"] = pd.to_datetime(combined["scan_date"], format="%Y%m%d")

    return combined


def _latest_per_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    If multiple scans exist for the same day, keep only the latest
    (by scan_timestamp) for each symbol.
    """
    if df.empty:
        return df
    return (
        df.sort_values("scan_timestamp")
          .groupby(["symbol", "scan_date"])
          .last()
          .reset_index()
    )


# -------------------------
# Sector Trend Report
# -------------------------

def sector_trend_report() -> pd.DataFrame:
    """
    Pivot: rows = scan_date, columns = sector ETF, values = composite_score.
    Shows how sector scores evolve over the 30-session window.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    raw = _load_all_scans(SECTOR_DIR)
    if raw.empty:
        logger.warning("No sector scans to report on.")
        return pd.DataFrame()

    daily = _latest_per_day(raw)

    pivot = daily.pivot_table(
        index="scan_date",
        columns="symbol",
        values="composite_score",
        aggfunc="last",
    )
    pivot = pivot.sort_index()

    # Add rank columns (1 = strongest that day)
    rank = pivot.rank(axis=1, ascending=False).astype(int)
    rank.columns = [f"{c}_rank" for c in rank.columns]

    report = pd.concat([pivot, rank], axis=1)

    out_path = REPORT_DIR / "sector_trend.csv"
    report.to_csv(out_path)
    logger.info(f"Sector trend report → {out_path}")

    return report


# -------------------------
# Sector Score Change Report
# -------------------------

def sector_change_report() -> pd.DataFrame:
    """
    For each sector: latest score, score N days ago, delta, direction.
    Quick view of what's accelerating or decelerating.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    raw = _load_all_scans(SECTOR_DIR)
    if raw.empty:
        return pd.DataFrame()

    daily = _latest_per_day(raw)

    dates = sorted(daily["scan_date"].unique())
    if len(dates) < 2:
        logger.warning("Need at least 2 scan days for change report.")
        return pd.DataFrame()

    latest_date = dates[-1]
    results = []

    for symbol in daily["symbol"].unique():
        sym_data = daily[daily["symbol"] == symbol].sort_values("scan_date")

        latest = sym_data[sym_data["scan_date"] == latest_date]
        if latest.empty:
            continue

        latest_score = latest["composite_score"].iloc[0]

        # 1-day change
        if len(dates) >= 2:
            prev = sym_data[sym_data["scan_date"] == dates[-2]]
            delta_1d = latest_score - prev["composite_score"].iloc[0] if not prev.empty else None
        else:
            delta_1d = None

        # 5-day change (if available)
        if len(dates) >= 5:
            prev5 = sym_data[sym_data["scan_date"] == dates[-5]]
            delta_5d = latest_score - prev5["composite_score"].iloc[0] if not prev5.empty else None
        else:
            delta_5d = None

        results.append({
            "symbol": symbol,
            "latest_score": latest_score,
            "delta_1d": delta_1d,
            "delta_5d": delta_5d,
            "direction": "▲" if delta_1d and delta_1d > 0 else ("▼" if delta_1d and delta_1d < 0 else "—"),
        })

    df = pd.DataFrame(results).sort_values("latest_score", ascending=False).reset_index(drop=True)

    out_path = REPORT_DIR / "sector_changes.csv"
    df.to_csv(out_path, index=False)
    logger.info(f"Sector change report → {out_path}")

    return df


# -------------------------
# Stock Tracker Report
# -------------------------

def stock_tracker_report(symbol: str) -> pd.DataFrame:
    """
    Full history for a single stock across all scans.
    Shows score evolution, bias changes, alignment shifts.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    raw = _load_all_scans(STOCK_DIR)
    if raw.empty:
        return pd.DataFrame()

    daily = _latest_per_day(raw)
    sym_data = daily[daily["symbol"] == symbol.upper()].sort_values("scan_date")

    if sym_data.empty:
        logger.warning(f"No scan history for {symbol}")
        return pd.DataFrame()

    cols = [
        "scan_date", "symbol", "sector",
        "weekly_score", "weekly_bias",
        "daily_score", "daily_bias",
    ]
    # Include hourly if present
    if "hourly_score" in sym_data.columns:
        cols += ["hourly_score", "hourly_bias"]
    cols += ["composite_score", "aligned"]

    report = sym_data[cols].copy()

    # Add day-over-day change
    report["score_delta"] = report["composite_score"].diff()

    out_path = REPORT_DIR / f"stock_{symbol.upper()}.csv"
    report.to_csv(out_path, index=False)
    logger.info(f"Stock tracker for {symbol} → {out_path}")

    return report


# -------------------------
# Universe Ranking History
# -------------------------

def stock_ranking_report() -> pd.DataFrame:
    """
    Pivot: rows = scan_date, columns = symbol, values = composite_score.
    Wide-format view of the entire universe over time.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    raw = _load_all_scans(STOCK_DIR)
    if raw.empty:
        return pd.DataFrame()

    daily = _latest_per_day(raw)

    pivot = daily.pivot_table(
        index="scan_date",
        columns="symbol",
        values="composite_score",
        aggfunc="last",
    )
    pivot = pivot.sort_index()

    out_path = REPORT_DIR / "stock_ranking_history.csv"
    pivot.to_csv(out_path)
    logger.info(f"Stock ranking history → {out_path}")

    return pivot


# -------------------------
# Watchlist Frequency Reports
# -------------------------

def watchlist_frequency_report(n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analyze which stocks appear most/least often on the strong and weak lists.

    Returns
    -------
    (strong_freq, weak_freq) — DataFrames sorted by appearance count.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    strong_freq = _build_frequency("strong", STRONG_DIR, n)
    weak_freq   = _build_frequency("weak", WEAK_DIR, n)

    return strong_freq, weak_freq


def _build_frequency(list_type: str, scan_dir: Path, n: int) -> pd.DataFrame:
    """Build frequency + consistency stats for a watchlist type."""

    raw = _load_all_scans(scan_dir)
    if raw.empty:
        logger.warning(f"No {list_type} watchlist files found.")
        return pd.DataFrame()

    # Keep only latest scan per day
    daily = _latest_per_day(raw)

    total_days = daily["scan_date"].nunique()
    if total_days == 0:
        return pd.DataFrame()

    # --- Appearance count ---
    appearances = (
        daily.groupby("symbol")["scan_date"]
        .nunique()
        .reset_index()
        .rename(columns={"scan_date": "days_on_list"})
    )
    appearances["total_scan_days"] = total_days
    appearances["pct_days"] = (
        (appearances["days_on_list"] / total_days * 100).round(1)
    )

    # --- Average rank when on list ---
    avg_rank = (
        daily.groupby("symbol")["rank"]
        .mean()
        .reset_index()
        .rename(columns={"rank": "avg_rank"})
    )
    avg_rank["avg_rank"] = avg_rank["avg_rank"].round(1)

    # --- Average composite score ---
    avg_score = (
        daily.groupby("symbol")["composite_score"]
        .mean()
        .reset_index()
        .rename(columns={"composite_score": "avg_score"})
    )
    avg_score["avg_score"] = avg_score["avg_score"].round(4)

    # --- Best / worst rank ---
    best_rank = (
        daily.groupby("symbol")["rank"]
        .min()
        .reset_index()
        .rename(columns={"rank": "best_rank"})
    )

    # --- Streak: consecutive most recent days on list ---
    streaks = _compute_streaks(daily)

    # --- Sector ---
    sectors = (
        daily.groupby("symbol")["sector"]
        .first()
        .reset_index()
    )

    # --- Merge ---
    freq = (
        appearances
        .merge(avg_rank, on="symbol")
        .merge(avg_score, on="symbol")
        .merge(best_rank, on="symbol")
        .merge(streaks, on="symbol", how="left")
        .merge(sectors, on="symbol", how="left")
        .sort_values("days_on_list", ascending=False)
        .reset_index(drop=True)
    )

    # Reorder columns
    col_order = [
        "symbol", "sector", "days_on_list", "total_scan_days",
        "pct_days", "current_streak", "avg_rank", "best_rank",
        "avg_score",
    ]
    freq = freq[[c for c in col_order if c in freq.columns]]

    out_path = REPORT_DIR / f"watchlist_{list_type}_frequency.csv"
    freq.to_csv(out_path, index=False)
    logger.info(f"{list_type.capitalize()} frequency report → {out_path}")

    return freq


def _compute_streaks(daily: pd.DataFrame) -> pd.DataFrame:
    """
    For each symbol, compute how many consecutive recent scan days
    it has appeared on the list (current streak).
    """
    dates = sorted(daily["scan_date"].unique(), reverse=True)
    streak_data = []

    for symbol in daily["symbol"].unique():
        sym_dates = set(daily[daily["symbol"] == symbol]["scan_date"])
        streak = 0
        for d in dates:
            if d in sym_dates:
                streak += 1
            else:
                break
        streak_data.append({"symbol": symbol, "current_streak": streak})

    return pd.DataFrame(streak_data)