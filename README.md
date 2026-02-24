# Stock Scanner

Relative strength scanner for US mega-cap equities.
Scores stocks vs SPY across weekly, daily, and hourly timeframes.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Add your Alpaca API keys to .env
```

## Usage

```bash
# Daily scan
python -m src.main scan

# Sector trends (after multiple scans)
python -m src.main report-sectors

# Track a stock
python -m src.main report-stock NVDA
```

## Disclaimer

This is a research tool, not financial advice.

Data Layer: WORKING
RS Engine: PARTIALLY BUILT (in scratch)
Sector Layer: NOT BUILT
Logger: NOT BUILT
CLI Runner: NOT BUILT
