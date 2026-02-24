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
#   │   │   ├── stock_20260223_143022_a1b2c3d4.csv
#   │   │   └── ...
#   │   └── sectors/
#   │       ├── sector_20260223_143022_e5f6g7h8.csv
#   │       └── ...
#   └── reports/
#       ├── sector_trend.csv
#       ├── stock_NVDA.csv
#       └── ...

BASE_DIR    = Path("data")
STOCK_DIR   = BASE_DIR / "scans" / "stocks"
SECTOR_DIR  = BASE_DIR / "scans" / "sectors"
REPORT_DIR  = BASE_DIR / "reports"

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

    Parameters
    ----------
    df : DataFrame from compute_stock_rs()
    scan_type : 'stock' or 'sector'
    metadata : optional dict of params (lookbacks, weights, etc.)

    Returns
    -------
    Path to the written file.
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