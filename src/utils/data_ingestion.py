# src/utils/data_ingestion.py

"""
Alpaca data ingestion for daily and weekly bar data.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, cast

import os
import logging

import pandas as pd
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed, Adjustment

load_dotenv()

logger = logging.getLogger(__name__)

BENCHMARK = "SPY"
MAX_SYMBOLS_PER_REQUEST = 200
REQUIRED_COLUMNS = {"open", "high", "low", "close", "volume"}


# -------------------------
# Client (singleton)
# -------------------------

_client: Optional[StockHistoricalDataClient] = None


def get_client() -> StockHistoricalDataClient:
    global _client
    if _client is None:
        _client = StockHistoricalDataClient(
            os.environ["ALPACA_API_KEY"],
            os.environ["ALPACA_SECRET_KEY"],
        )
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
            adjustment=Adjustment.RAW,
            feed=DataFeed.IEX,
        )
        bars = client.get_stock_bars(request)
        df = cast(pd.DataFrame, bars.df).reset_index()  # pyright: ignore[reportAttributeAccessIssue]
        all_frames.append(df)

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
    """
    end = datetime.now(timezone.utc)
    # ~1.5x calendar days to cover weekends + holidays
    start = end - timedelta(days=int(trading_days * 1.5))

    return _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Day), # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )


def get_weekly_batch(
    symbols: List[str],
    weeks: int = 26,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch weekly bars.  Alpaca supports TimeFrameUnit.Week natively.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(weeks=weeks)

    return _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Week), # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )


def get_hourly_batch(
    symbols: List[str],
    trading_days: int = 30,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch 1-hour bars.
    ~6.5 bars per trading day, so 30 days ≈ 195 bars.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))

    return _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Hour),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )


def get_15m_batch(
    symbols: List[str],
    trading_days: int = 5,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch 15-minute bars.
    ~26 bars per trading day, so 5 days ≈ 130 bars.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))

    return _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=15, unit=TimeFrameUnit.Minute),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )


def get_5m_batch(
    symbols: List[str],
    trading_days: int = 2,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch 5-minute bars.
    ~78 bars per trading day, so 2 days ≈ 156 bars.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=int(trading_days * 1.5))

    return _fetch_bars(
        symbols=symbols,
        timeframe=TimeFrame(amount=5, unit=TimeFrameUnit.Minute),  # pyright: ignore[reportArgumentType]
        start=start,
        end=end,
    )

