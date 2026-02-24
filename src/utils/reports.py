"""
Reporting layer — reads historical scans and produces
trend / tracking reports for the 30-session research phase.
"""

from pathlib import Path
import logging

import pandas as pd

from src.utils.logger import STOCK_DIR, SECTOR_DIR, REPORT_DIR

logger = logging.getLogger(__name__)


def _load_all_scans(scan_dir: Path) -> pd.DataFrame:
    """Load all CSV files in a scan directory into one DataFrame."""
    files = sorted(scan_dir.glob("*.csv"))
    if not files:
        logger.warning(f"No scan files found in {scan_dir}")
        return pd.DataFrame()

    dfs = [pd.read_csv(f) for f in files]
    combined = pd.concat(dfs, ignore_index=True)

    # Ensure scan_date is a proper date for sorting/grouping
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