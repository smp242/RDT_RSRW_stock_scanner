"""
Options chain data ingestion via Alpaca.

Fetches the 30-delta call and put for each symbol at the nearest expiration
between 30 and 60 DTE, extracts implied volatility, and checks bid/ask
spread quality.

Feed: starts on 'indicative' (free tier, approximate).
      Flip IV_FEED to OptionsFeed.OPRA when ready to trade from this data.
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional

import numpy as np

from dotenv import load_dotenv

from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest
from alpaca.data.enums import OptionsFeed
from alpaca.data.models import OptionsSnapshot
from alpaca.trading.enums import ContractType

from src.models.universe import BENCHMARK

load_dotenv()

logger = logging.getLogger(__name__)

# -------------------------
# Feed selection
# -------------------------
# Change to OptionsFeed.OPRA when your OPRA agreement is active and
# you are ready to make trading decisions from this data.
IV_FEED = OptionsFeed.INDICATIVE

# Target delta for contract selection
TARGET_DELTA = 0.30

# Expiration window (DTE)
MIN_DTE = 30
MAX_DTE = 60

# Bid/ask spread quality threshold (as fraction of option mid-price)
MAX_SPREAD_PCT = 0.15


# -------------------------
# Client (singleton)
# -------------------------

_client: Optional[OptionHistoricalDataClient] = None


def get_option_client() -> OptionHistoricalDataClient:
    global _client
    if _client is None:
        try:
            _client = OptionHistoricalDataClient(
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

def _expiration_window() -> tuple[date, date]:
    """Return today+MIN_DTE and today+MAX_DTE as the expiration filter."""
    today = date.today()
    return today + timedelta(days=MIN_DTE), today + timedelta(days=MAX_DTE)


def _spread_quality(snap: OptionsSnapshot) -> tuple[bool, float]:
    """
    Check whether the bid/ask spread is tight enough for a clean IV reading.
    Returns (passes, spread_pct).
    A wide spread means the IV we back out is noisy — the bid and ask imply
    meaningfully different volatilities, and the midpoint is unreliable.
    """
    q = snap.latest_quote
    if q is None or q.bid_price is None or q.ask_price is None:
        return False, float("nan")

    mid = (q.bid_price + q.ask_price) / 2
    if mid <= 0:
        return False, float("nan")

    spread_pct = (q.ask_price - q.bid_price) / mid
    return spread_pct <= MAX_SPREAD_PCT, round(spread_pct, 4)


def _find_target_contract(
    snapshots: dict[str, OptionsSnapshot],
    contract_type: ContractType,
) -> Optional[OptionsSnapshot]:
    """
    From a dict of option snapshots, find the contract of the given type
    whose delta is closest to ±TARGET_DELTA.
    Skips contracts with missing greeks, missing IV, or missing quotes.
    """
    target = TARGET_DELTA if contract_type == ContractType.CALL else -TARGET_DELTA

    candidates = [
        snap for snap in snapshots.values()
        if snap.greeks is not None
        and snap.greeks.delta is not None
        and snap.implied_volatility is not None
        and snap.latest_quote is not None
        # Filter to the correct contract type using the symbol convention:
        # OCC symbol format: AAPL250321C00180000 — 'C' or 'P' at position 15
        and (
            ("C" in snap.symbol[6:] if contract_type == ContractType.CALL
             else "P" in snap.symbol[6:])
        )
    ]

    if not candidates:
        return None

    return min(candidates, key=lambda s: abs(s.greeks.delta - target))  # type: ignore[union-attr]


# -------------------------
# Public API
# -------------------------

def get_iv_snapshot(symbol: str) -> Optional[dict]:
    """
    Fetch the option chain for one symbol and return a dict with:
      - symbol
      - iv              : averaged IV from 30-delta call and put
      - call_iv         : call-side IV
      - put_iv          : put-side IV
      - call_delta      : actual delta of selected call contract
      - put_delta       : actual delta of selected put contract
      - call_spread_pct : bid/ask spread quality on call
      - put_spread_pct  : bid/ask spread quality on put
      - spread_ok       : True if both contracts pass quality check
      - call_symbol     : OCC symbol of selected call
      - put_symbol      : OCC symbol of selected put

    Returns None if the chain is unavailable or no valid contracts found.
    """
    client = get_option_client()
    exp_min, exp_max = _expiration_window()

    try:
        request = OptionChainRequest(
            underlying_symbol=symbol,
            expiration_date_gte=exp_min,
            expiration_date_lte=exp_max,
            feed=IV_FEED,
        )
        chain = client.get_option_chain(request)
    except Exception as e:
        logger.warning(f"{symbol}: option chain fetch failed — {e}")
        return None

    if not chain:
        logger.warning(f"{symbol}: empty option chain returned")
        return None

    call = _find_target_contract(chain, ContractType.CALL)
    put  = _find_target_contract(chain, ContractType.PUT)

    if call is None or put is None:
        logger.warning(f"{symbol}: could not find 30-delta call or put in {MIN_DTE}–{MAX_DTE} DTE window")
        return None

    call_ok, call_spread = _spread_quality(call)
    put_ok,  put_spread  = _spread_quality(put)

    call_iv = float(call.implied_volatility)  # type: ignore[arg-type]
    put_iv  = float(put.implied_volatility)   # type: ignore[arg-type]
    avg_iv  = (call_iv + put_iv) / 2

    if not call_ok or not put_ok:
        logger.warning(
            f"{symbol}: wide bid/ask spread — call {call_spread:.1%}, put {put_spread:.1%}. "
            "IV reading flagged as noisy."
        )

    return {
        "symbol":          symbol,
        "iv":              round(avg_iv, 4),
        "call_iv":         round(call_iv, 4),
        "put_iv":          round(put_iv, 4),
        "call_delta":      round(float(call.greeks.delta), 3),  # type: ignore[union-attr]
        "put_delta":       round(float(put.greeks.delta), 3),   # type: ignore[union-attr]
        "call_spread_pct": call_spread,
        "put_spread_pct":  put_spread,
        "spread_ok":       call_ok and put_ok,
        "call_symbol":     call.symbol,
        "put_symbol":      put.symbol,
    }


def get_iv_universe(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch IV snapshots for a list of symbols.
    Returns {symbol: snapshot_dict} for successful fetches only.
    Logs a warning for each failure — does not raise.
    """
    results: dict[str, dict] = {}
    total = len(symbols)

    for i, symbol in enumerate(symbols, 1):
        logger.info(f"IV fetch {i}/{total}: {symbol}")
        snap = get_iv_snapshot(symbol)
        if snap is not None:
            results[symbol] = snap
        else:
            logger.warning(f"{symbol}: skipped — no valid IV snapshot")

    logger.info(f"IV fetch complete: {len(results)}/{total} symbols")
    return results
