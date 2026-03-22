# src/utils/data_ingestion.py

"""
Alpaca data ingestion for daily and weekly bar data.
Fetched data is cached to disk (data/cache/) with per-timeframe TTLs so that
repeated runs within the same session skip redundant API calls.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, cast

import hashlib
import logging
import os
import pickle
import time

import pandas as pd
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed, Adjustment

from src.models.universe import BENCHMARK

load_dotenv()

logger = logging.getLogger(__name__)

MAX_SYMBOLS_PER_REQUEST = 200
REQUIRED_COLUMNS = {"open", "high", "low", "close", "volume"}

# -------------------------
# Cache
# -------------------------

_CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"

# How long (seconds) each timeframe's cache is considered fresh
_CACHE_TTL: dict[str, int] = {
    "daily":  4 * 3600,   # 4 hours — daily bars don't change intraday
    "weekly": 4 * 3600,   # 4 hours
    "1h":    30 * 60,     # 30 minutes
    "15m":   15 * 60,     # 15 minutes
    "5m":     5 * 60,     # 5 minutes
}


def _cache_path(label: str, symbols: List[str]) -> Path:
    sym_hash = hashlib.md5(",".join(sorted(symbols)).encode()).hexdigest()[:10]
    return _CACHE_DIR / f"{label}_{sym_hash}.pkl"


def _load_cache(label: str, symbols: List[str]) -> Optional[Dict[str, pd.DataFrame]]:
    path = _cache_path(label, symbols)
    if not path.exists():
        return None
    ttl = _CACHE_TTL.get(label, 300)
    if time.time() - path.stat().st_mtime > ttl:
        logger.debug(f"Cache expired for {label}")
        return None
    logger.info(f"Cache hit for {label} ({path.name})")
    with open(path, "rb") as f:
        return pickle.load(f)


def _save_cache(label: str, symbols: List[str], data: Dict[str, pd.DataFrame]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(label, symbols)
    with open(path, "wb") as f:
        pickle.dump(data, f)
    logger.debug(f"Cached {label} → {path.name}")


# -------------------------
# Client (singleton)
# -------------------------

_client: Optional[StockHistoricalDataClient] = None


def get_client() -> StockHistoricalDataClient:
    global _client
    if _client is None:
        try:
            _client = StockHistoricalDataClient(
                os.environ["ALPACA_API_KEY"],
                os.environ["ALPACA_SECRET_KEY"],
            )
        except KeyError as e:
            raise RuntimeError(
                f"Missing Alpaca API credential: {e}. "
                "Check your .env file for ALPACA_API_KEY and ALPACA_SECRET_KEY."
            ) from e
    return _client


# -------------------------
# Internal helpers
# -------------------------

def _ensure_benchmark(symbols: List[str]) -> List[str]:
    """Guarantee the benchmark is in every request."""
    if BENCHMARK not in symbols:
        symbols = [BENCHMARK] + symbols
    return symbols


def _chunked(lst: List[str], size: int):
    """Yield successive chunks of `size` from list."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def _fetch_bars(
    symbols: List[str],
    timeframe: TimeFrame,
    start: datetime,
    end: datetime,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch bars for a list of symbols, chunking if needed.
    Returns {symbol: DataFrame} with a DatetimeIndex on 'timestamp'.
    """
    client = get_client()
    symbols = _ensure_benchmark(symbols)

    all_frames: List[pd.DataFrame] = []

    for chunk in _chunked(symbols, MAX_SYMBOLS_PER_REQUEST):
        request = StockBarsRequest(
            symbol_or_symbols=chunk,
            timeframe=timeframe,  # pyright: ignore[reportArgumentType]
            start=start,
            end=end,
            adjustment=Adjustment.SPLIT,
            feed=DataFeed.IEX,
        )
        try:
            bars = client.get_stock_bars(request)
            df = cast(pd.DataFrame, bars.df).reset_index()  # pyright: ignore[reportAttributeAccessIssue]
            all_frames.append(df)
        except Exception as e:
            logger.warning(f"API request failed for chunk starting with {chunk[:3]}: {e}")

    if not all_frames:
        return {}

    df = pd.concat(all_frames, ignore_index=True)

    data: Dict[str, pd.DataFrame] = {}

    for symbol in symbols:
        sym_df = df[df["symbol"] == symbol].copy()

        if sym_df.empty:
            logger.warning(f"{symbol}: no bar data returned — skipping")
            continue

        missing = REQUIRED_COLUMNS - set(sym_df.columns)
        if missing:
            logger.warning(f"{symbol}: missing columns {missing} — skipping")
            continue

        sym_df = sym_df.sort_values("timestamp")
        sym_df.set_index("timestamp", inplace=True)
        data[symbol] = sym_df

    return data


# -------------------------
# Public API
# -------------------------

def get_daily_batch(
    symbols: List[str],
    trading_days: int = 60,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch daily bars.  `trading_days` is approximate — we pad with
    calendar days to make sure we have enough after weekends/holidays.
    Cached for 4 hours.
    """
    cached = _load_cache("daily", symbols)
    if cached is not None:
        return cached

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))
    result = _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Day),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )
    _save_cache("daily", symbols, result)
    return result


def get_weekly_batch(
    symbols: List[str],
    weeks: int = 26,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch weekly bars.  Alpaca supports TimeFrameUnit.Week natively.
    Cached for 4 hours.
    """
    cached = _load_cache("weekly", symbols)
    if cached is not None:
        return cached

    end = datetime.now(timezone.utc)
    start = end - timedelta(weeks=weeks)
    result = _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Week),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )
    _save_cache("weekly", symbols, result)
    return result


def get_hourly_batch(
    symbols: List[str],
    trading_days: int = 30,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch 1-hour bars.
    ~6.5 bars per trading day, so 30 days ≈ 195 bars.
    Cached for 30 minutes.
    """
    cached = _load_cache("1h", symbols)
    if cached is not None:
        return cached

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))
    result = _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Hour),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )
    _save_cache("1h", symbols, result)
    return result


def get_15m_batch(
    symbols: List[str],
    trading_days: int = 5,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch 15-minute bars.
    ~26 bars per trading day, so 5 days ≈ 130 bars.
    Cached for 15 minutes.
    """
    cached = _load_cache("15m", symbols)
    if cached is not None:
        return cached

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))
    result = _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=15, unit=TimeFrameUnit.Minute),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )
    _save_cache("15m", symbols, result)
    return result


def get_5m_batch(
    symbols: List[str],
    trading_days: int = 2,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch 5-minute bars.
    ~78 bars per trading day, so 2 days ≈ 156 bars.
    Cached for 5 minutes.
    """
    cached = _load_cache("5m", symbols)
    if cached is not None:
        return cached

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))
    result = _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=5, unit=TimeFrameUnit.Minute),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )
    _save_cache("5m", symbols, result)
    return result
