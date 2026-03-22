"""
Stock universe and benchmark definitions.
Single source of truth for all symbol lists.
Primary universe: US-listed large caps with high options open interest and activity.
Market cap generally >$40B, with emphasis on options liquidity over cap size.
"""

BENCHMARK = "SPY"

STOCK_SYMBOLS = [
    # Technology
    "AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "AMD", "CSCO", "CRM",
    "ACN", "ADBE", "TXN", "QCOM", "INTC", "IBM", "INTU", "AMAT",
    "NOW", "SHOP", "PLTR",
    "MU", "PANW", "MSTR",

    # Communication Services
    "GOOGL", "META", "NFLX", "TMUS", "DIS", "CMCSA", "VZ", "T",

    # Consumer Discretionary
    "AMZN", "TSLA", "HD", "MCD", "TJX", "LOW", "BKNG", "NKE", "SBUX",
    "UBER", "ABNB", "RCL", "GM", "F",

    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "BLK", "SPGI", "AXP",
    "C", "BX", "ICE", "CB", "MCO", "CME", "AON", "MMC",
    "V", "MA", "SCHW", "PYPL", "COIN",

    # Healthcare
    "LLY", "UNH", "JNJ", "MRK", "ABBV", "ABT", "TMO", "PFE",
    "AMGN", "DHR", "ISRG", "MDT", "BMY", "GILD", "SYK", "VRTX",
    "BSX", "ELV", "REGN", "ZTS",

    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC",

    # Industrials
    "CAT", "GE", "RTX", "HON", "UNP", "BA", "DE", "LMT",
    "UPS", "ADP", "GD", "ITW", "ETN", "WM", "EMR",
    "DAL",

    # Consumer Staples
    "PG", "COST", "KO", "PEP", "WMT", "PM", "MO", "MDLZ",
    "CL", "TGT", "STZ",

    # Utilities
    "NEE", "SO", "DUK", "CEG",

    # Real Estate
    "PLD", "AMT", "EQIX", "SPG",

    # Materials
    "LIN", "APD", "SHW", "FCX", "NEM",
]

SECTOR_ETFS = [
    "XLK",   # Technology
    "XLF",   # Financials
    "XLV",   # Healthcare
    "XLE",   # Energy
    "XLY",   # Consumer Discretionary
    "XLI",   # Industrials
    "XLC",   # Communication Services
    "XLP",   # Consumer Staples
    "XLU",   # Utilities
    "XLB",   # Materials
    "XLRE",  # Real Estate
]

# Deduplicated master list
ALL_SYMBOLS = sorted(set(STOCK_SYMBOLS + SECTOR_ETFS + [BENCHMARK]))

