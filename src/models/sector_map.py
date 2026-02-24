"""
Hard-coded stock → sector ETF mapping.
Must stay in sync with universe.py STOCK_SYMBOLS.
"""

SECTOR_MAP: dict[str, str] = {
    # Technology (XLK)
    "AAPL": "XLK",
    "MSFT": "XLK",
    "NVDA": "XLK",
    "AVGO": "XLK",
    "ORCL": "XLK",
    "AMD": "XLK",
    "CSCO": "XLK",
    "CRM": "XLK",
    "ACN": "XLK",
    "ADBE": "XLK",
    "TXN": "XLK",
    "QCOM": "XLK",
    "INTC": "XLK",
    "IBM": "XLK",
    "INTU": "XLK",
    "AMAT": "XLK",
    "NOW": "XLK",
    "SHOP": "XLK",
    "PLTR": "XLK",

    # Communication Services (XLC)
    "GOOGL": "XLC",
    "META": "XLC",
    "NFLX": "XLC",
    "TMUS": "XLC",
    "DIS": "XLC",
    "CMCSA": "XLC",
    "VZ": "XLC",
    "T": "XLC",

    # Consumer Discretionary (XLY)
    "AMZN": "XLY",
    "TSLA": "XLY",
    "HD": "XLY",
    "MCD": "XLY",
    "TJX": "XLY",
    "LOW": "XLY",
    "BKNG": "XLY",
    "NKE": "XLY",
    "SBUX": "XLY",

    # Financials (XLF)
    "JPM": "XLF",
    "BAC": "XLF",
    "WFC": "XLF",
    "GS": "XLF",
    "MS": "XLF",
    "BLK": "XLF",
    "SPGI": "XLF",
    "AXP": "XLF",
    "C": "XLF",
    "BX": "XLF",
    "ICE": "XLF",
    "CB": "XLF",
    "MCO": "XLF",
    "CME": "XLF",
    "AON": "XLF",
    "MMC": "XLF",

    # Healthcare (XLV)
    "LLY": "XLV",
    "UNH": "XLV",
    "JNJ": "XLV",
    "MRK": "XLV",
    "ABBV": "XLV",
    "ABT": "XLV",
    "TMO": "XLV",
    "PFE": "XLV",
    "AMGN": "XLV",
    "DHR": "XLV",
    "ISRG": "XLV",
    "MDT": "XLV",
    "BMY": "XLV",
    "GILD": "XLV",
    "SYK": "XLV",
    "VRTX": "XLV",
    "BSX": "XLV",
    "ELV": "XLV",
    "REGN": "XLV",
    "ZTS": "XLV",

    # Energy (XLE)
    "XOM": "XLE",
    "CVX": "XLE",
    "COP": "XLE",
    "EOG": "XLE",
    "SLB": "XLE",
    "PXD": "XLE",
    "MPC": "XLE",

    # Industrials (XLI)
    "CAT": "XLI",
    "GE": "XLI",
    "RTX": "XLI",
    "HON": "XLI",
    "UNP": "XLI",
    "BA": "XLI",
    "DE": "XLI",
    "LMT": "XLI",
    "UPS": "XLI",
    "ADP": "XLI",
    "GD": "XLI",
    "ITW": "XLI",
    "ETN": "XLI",
    "WM": "XLI",
    "EMR": "XLI",

    # Consumer Staples (XLP)
    "PG": "XLP",
    "COST": "XLP",
    "KO": "XLP",
    "PEP": "XLP",
    "WMT": "XLP",
    "PM": "XLP",
    "MO": "XLP",
    "MDLZ": "XLP",
    "CL": "XLP",
    "TGT": "XLP",
    "STZ": "XLP",

    # Utilities (XLU)
    "NEE": "XLU",
    "SO": "XLU",
    "DUK": "XLU",
    "CEG": "XLU",

    # Real Estate (XLRE)
    "PLD": "XLRE",
    "AMT": "XLRE",
    "EQIX": "XLRE",
    "SPG": "XLRE",

    # Materials (XLB)
    "LIN": "XLB",
    "APD": "XLB",
    "SHW": "XLB",
    "FCX": "XLB",
    "NEM": "XLB",
}


def get_sector(symbol: str) -> str | None:
    """Return sector ETF for a stock, or None if unmapped."""
    return SECTOR_MAP.get(symbol)


def get_stocks_in_sector(sector_etf: str) -> list[str]:
    """Return all stocks mapped to a given sector ETF."""
    return [sym for sym, sec in SECTOR_MAP.items() if sec == sector_etf]


def validate_universe(stock_symbols: list[str]) -> list[str]:
    """Return any symbols in the universe that are missing from SECTOR_MAP."""
    return [s for s in stock_symbols if s not in SECTOR_MAP]