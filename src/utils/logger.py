from pathlib import Path
from datetime import datetime, timezone
import logging
import uuid

import pandas as pd

logger = logging.getLogger(__name__)

# Directory structure:
#   data/
#   ├── scans/
#   │   ├── stocks/
#   │   └── sectors/
#   ├── spot/
#   │   ├── universe/
#   │   └── singles/
#   ├── iv/
#   │   ├── history/         ← per-symbol daily IV log (append mode)
#   │   │   ├── AAPL.csv
#   │   │   └── ...
#   │   └── scans/           ← point-in-time IV metrics snapshots
#   │       └── iv_scan_YYYYMMDD_uuid.csv
#   ├── watchlists/
#   │   ├── strong/
#   │   └── weak/
#   ├── reports/
#   └── logs/

BASE_DIR     = Path(__file__).parent.parent.parent / "data"
STOCK_DIR    = BASE_DIR / "scans" / "stocks"
SECTOR_DIR   = BASE_DIR / "scans" / "sectors"
REPORT_DIR   = BASE_DIR / "reports"
STRONG_DIR   = BASE_DIR / "watchlists" / "strong"
WEAK_DIR     = BASE_DIR / "watchlists" / "weak"
SPOT_UNI_DIR = BASE_DIR / "spot" / "universe"
SPOT_SYM_DIR = BASE_DIR / "spot" / "singles"
IV_HIST_DIR  = BASE_DIR / "iv" / "history"
IV_SCAN_DIR  = BASE_DIR / "iv" / "scans"

MODEL_VERSION = "v1.3_zscore_3tf"

SCAN_DIRS = {
    "stock": STOCK_DIR,
    "sector": SECTOR_DIR,
}


def log_scan(
    df: pd.DataFrame,
    scan_type: str = "stock",
    metadata: dict | None = None,
) -> Path:
    """
    Write scan results to CSV in the appropriate subdirectory.
    """
    df = df.copy()

    scan_dir = SCAN_DIRS.get(scan_type)
    if scan_dir is None:
        raise ValueError(f"Unknown scan_type: {scan_type}. Use 'stock' or 'sector'.")

    scan_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y%m%d")
    scan_timestamp = now.strftime("%Y%m%d_%H%M%S")
    scan_uuid = uuid.uuid4().hex[:8]

    df["scan_uuid"] = scan_uuid
    df["scan_date"] = scan_date
    df["scan_timestamp"] = scan_timestamp
    df["scan_type"] = scan_type
    df["model_version"] = MODEL_VERSION

    if metadata:
        for key, value in metadata.items():
            df[f"meta_{key}"] = value

    df = df.sort_values("composite_score", ascending=False)

    filename = scan_dir / f"{scan_type}_{scan_timestamp}_{scan_uuid}.csv"
    df.to_csv(filename, index=False)

    logger.info(f"Logged {scan_type} scan → {filename}")
    return filename


def log_watchlists(
    df: pd.DataFrame,
    n: int = 10,
    metadata: dict | None = None,
) -> tuple[Path, Path]:
    """
    Log the top N strongest and weakest stocks to separate daily files.
    """
    STRONG_DIR.mkdir(parents=True, exist_ok=True)
    WEAK_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y%m%d")
    scan_timestamp = now.strftime("%Y%m%d_%H%M%S")
    scan_uuid = uuid.uuid4().hex[:8]

    sorted_df = df.sort_values("composite_score", ascending=False)

    watchlist_cols = [
        "symbol", "sector",
        "weekly_bias", "daily_bias",
    ]
    if "hourly_bias" in df.columns:
        watchlist_cols.append("hourly_bias")
    watchlist_cols += [
        "composite_score", "aligned",
        "weekly_score", "daily_score",
    ]
    if "hourly_score" in df.columns:
        watchlist_cols.append("hourly_score")

    watchlist_cols = [c for c in watchlist_cols if c in sorted_df.columns]

    strong = sorted_df.head(n)[watchlist_cols].copy()
    strong["rank"] = range(1, len(strong) + 1)
    strong["list"] = "strong"
    strong["scan_date"] = scan_date
    strong["scan_timestamp"] = scan_timestamp
    strong["scan_uuid"] = scan_uuid
    strong["model_version"] = MODEL_VERSION
    if metadata:
        for key, value in metadata.items():
            strong[f"meta_{key}"] = value

    weak = sorted_df.tail(n)[watchlist_cols].copy()
    weak["rank"] = range(1, len(weak) + 1)
    weak["list"] = "weak"
    weak["scan_date"] = scan_date
    weak["scan_timestamp"] = scan_timestamp
    weak["scan_uuid"] = scan_uuid
    weak["model_version"] = MODEL_VERSION
    if metadata:
        for key, value in metadata.items():
            weak[f"meta_{key}"] = value

    strong_path = STRONG_DIR / f"strong_{scan_date}.csv"
    weak_path = WEAK_DIR / f"weak_{scan_date}.csv"

    strong.to_csv(
        strong_path,
        mode="a",
        header=not strong_path.exists(),
        index=False,
    )
    weak.to_csv(
        weak_path,
        mode="a",
        header=not weak_path.exists(),
        index=False,
    )

    logger.info(f"Logged top {n} strong → {strong_path}")
    logger.info(f"Logged top {n} weak   → {weak_path}")

    return strong_path, weak_path


def log_spot_universe(
    df: pd.DataFrame,
    metadata: dict | None = None,
) -> Path:
    """
    Log a full universe spot scan.
    One file per run, timestamped.
    """
    SPOT_UNI_DIR.mkdir(parents=True, exist_ok=True)

    df = df.copy()

    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y%m%d")
    scan_timestamp = now.strftime("%Y%m%d_%H%M%S")
    scan_uuid = uuid.uuid4().hex[:8]

    df["scan_date"] = scan_date
    df["scan_timestamp"] = scan_timestamp
    df["scan_uuid"] = scan_uuid
    df["model_version"] = MODEL_VERSION

    if metadata:
        for key, value in metadata.items():
            df[f"meta_{key}"] = value

    filename = SPOT_UNI_DIR / f"spot_universe_{scan_timestamp}_{scan_uuid}.csv"
    df.to_csv(filename, index=False)

    logger.info(f"Logged spot universe scan → {filename}")
    return filename


def log_spot_single(
    result: dict,
    metadata: dict | None = None,
) -> Path:
    """
    Log a single-symbol spot scan.
    Appends to a per-symbol daily file so you can track intraday changes.
    """
    SPOT_SYM_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y%m%d")
    scan_timestamp = now.strftime("%Y%m%d_%H%M%S")
    scan_uuid = uuid.uuid4().hex[:8]

    row = {**result}
    row["scan_date"] = scan_date
    row["scan_timestamp"] = scan_timestamp
    row["scan_uuid"] = scan_uuid
    row["model_version"] = MODEL_VERSION

    if metadata:
        for key, value in metadata.items():
            row[f"meta_{key}"] = value

    df = pd.DataFrame([row])

    symbol = result["symbol"]
    filename = SPOT_SYM_DIR / f"{symbol}_{scan_date}.csv"

    df.to_csv(
        filename,
        mode="a",
        header=not filename.exists(),
        index=False,
    )

    logger.info(f"Logged spot scan for {symbol} → {filename}")
    return filename


def log_iv_daily(
    iv_snapshots: dict,
    iv_metrics: pd.DataFrame | None = None,
) -> tuple[Path, Path | None]:
    """
    Append today's IV reading to each symbol's history file, and
    optionally write a full metrics snapshot for point-in-time analysis.

    Parameters
    ----------
    iv_snapshots : {symbol: snapshot_dict} from iv_ingestion
    iv_metrics   : DataFrame from iv_engine.compute_universe_iv_metrics (optional)

    Returns
    -------
    (history_dir, scan_path_or_None)
    """
    IV_HIST_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    scan_date = now.strftime("%Y%m%d")
    scan_timestamp = now.strftime("%Y%m%d_%H%M%S")
    scan_uuid = uuid.uuid4().hex[:8]

    # --- Append daily IV reading to per-symbol history files ---
    for symbol, snap in iv_snapshots.items():
        hist_file = IV_HIST_DIR / f"{symbol}.csv"
        row = pd.DataFrame([{
            "date": scan_date,
            "iv":   snap["iv"],
        }])
        row.to_csv(
            hist_file,
            mode="a",
            header=not hist_file.exists(),
            index=False,
        )

    logger.info(f"Appended IV history for {len(iv_snapshots)} symbols → {IV_HIST_DIR}")

    # --- Optional: write full metrics snapshot ---
    scan_path = None
    if iv_metrics is not None and not iv_metrics.empty:
        IV_SCAN_DIR.mkdir(parents=True, exist_ok=True)
        df = iv_metrics.copy()
        df["scan_date"] = scan_date
        df["scan_timestamp"] = scan_timestamp
        df["scan_uuid"] = scan_uuid
        df["model_version"] = MODEL_VERSION
        scan_path = IV_SCAN_DIR / f"iv_scan_{scan_timestamp}_{scan_uuid}.csv"
        df.to_csv(scan_path, index=False)
        logger.info(f"Logged IV scan snapshot → {scan_path}")

    return IV_HIST_DIR, scan_path


def load_iv_history(symbol: str) -> pd.DataFrame:
    """
    Load a symbol's daily IV history from its CSV file.

    Returns a DataFrame with columns ['date', 'iv'], sorted oldest-first.
    Returns an empty DataFrame if no history exists yet.

    This is the callable passed as iv_history_loader to
    iv_engine.compute_universe_iv_metrics().
    """
    hist_file = IV_HIST_DIR / f"{symbol}.csv"
    if not hist_file.exists():
        return pd.DataFrame(columns=["date", "iv"])

    try:
        df = pd.read_csv(hist_file, parse_dates=["date"])
        df = df.dropna(subset=["iv"])
        df = df.sort_values("date").reset_index(drop=True)
        return df[["date", "iv"]]
    except Exception as e:
        logger.warning(f"Could not load IV history for {symbol}: {e}")
        return pd.DataFrame(columns=["date", "iv"])