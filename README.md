# Stock Scanner

A relative strength scanner for US mega-cap equities. Scores ~110 stocks across all 11 GICS sectors against SPY using multi-timeframe momentum analysis.

Built for systematic daily tracking and intraday momentum spot checks.

---

## Features

### Daily Scanner (`scan`)
- Multi-timeframe relative strength scoring (weekly, daily, hourly)
- Z-score normalized composite scoring across the universe
- Sector ETF ranking
- Automatic watchlist logging (top 10 strong / weak)
- Bias alignment detection across timeframes

### Spot Scanner (`spot`)
- Intraday momentum analysis across 1H, 15m, and 5m timeframes
- Weekly, daily, and hourly ATR
- Range consumed (daily + weekly) — how much expected move is left
- Relative volume (intraday + daily)
- Distance from key levels (daily high/low, 20-day high/low)
- Single symbol deep-dive or full universe scan

### Reports
- **Sector trend** — score evolution over time with daily rankings
- **Sector changes** — 1-day and 5-day score deltas
- **Stock tracker** — full score history for any symbol
- **Universe rankings** — wide-format pivot of all scores over time
- **Watchlist frequency** — which stocks appear most often on strong/weak lists, streak tracking

---

## Universe

~110 US-listed stocks with market cap > $200B across all 11 GICS sectors, plus 11 sector ETFs and SPY as benchmark.

| Sector | ETF | Example Holdings |
|--------|-----|-----------------|
| Technology | XLK | AAPL, MSFT, NVDA, AVGO, AMD |
| Financials | XLF | JPM, GS, BLK, MS, BAC |
| Healthcare | XLV | LLY, UNH, JNJ, ABBV, MRK |
| Energy | XLE | XOM, CVX, COP, SLB |
| Consumer Discretionary | XLY | AMZN, TSLA, HD, MCD |
| Industrials | XLI | CAT, GE, RTX, HON, DE |
| Communication Services | XLC | GOOGL, META, NFLX, DIS |
| Consumer Staples | XLP | PG, COST, KO, WMT, PEP |
| Utilities | XLU | NEE, SO, DUK, CEG |
| Materials | XLB | LIN, APD, SHW, FCX |
| Real Estate | XLRE | PLD, AMT, EQIX, SPG |

---

## Setup

```bash
# Clone
git clone https://github.com/RRS/RW_stock_scanner.git
cd RW_stock_scanner

# Virtual environment
python -m venv .venv
source .venv/bin/activate

# Dependencies
pip install -r requirements.txt

# Environment variables
cp .env.example .env
# Add your Alpaca API keys to .env
```

### Requirements

- Python 3.11+
- [Alpaca Markets](https://alpaca.markets/) API key (free tier works)

---

## Usage

### Daily Scan

```bash
# Full scan — weekly, daily, hourly
python -m src.main scan

# Skip hourly bars (faster)
python -m src.main scan --no-hourly

# Show top/bottom 20 instead of 10
python -m src.main scan --top-n 20

# Filter to a single sector
python -m src.main scan --sector XLK
```

### Spot Scan — Intraday Momentum

```bash
# Single symbol deep-dive
python -m src.main spot NVDA

# Full universe intraday scan
python -m src.main spot

# Filter to a sector
python -m src.main spot --sector XLF
```

### Reports

```bash
# Sector rotation trends + score changes
python -m src.main report-sectors

# Single stock score history
python -m src.main report-stock NVDA

# Full universe ranking history
python -m src.main report-rankings

# Watchlist frequency analysis
python -m src.main report-watchlists
```

---

## Scoring Model

### Daily Scanner (v1.3)

Each stock is scored across three timeframes using four components:

| Component | Weight | Description |
|-----------|--------|-------------|
| Slope | 0.35 | Log-linear price slope — trend direction |
| Relative Strength | 0.35 | Log-return vs SPY — outperformance |
| Relative Volume | 0.15 | Current vs average volume — participation |
| Volatility Adjustment | 0.15 | Inverted vol ratio vs SPY — lower vol preferred |

All components are **z-score normalized** across the universe before blending, so no single metric dominates.

Timeframe weights for composite score:

| Timeframe | Weight |
|-----------|--------|
| Weekly | 0.40 |
| Daily | 0.35 |
| Hourly | 0.25 |

### Spot Scanner

Intraday momentum composite from RS vs SPY:

| Timeframe | Weight |
|-----------|--------|
| 1 Hour | 0.50 |
| 15 Minute | 0.30 |
| 5 Minute | 0.20 |

---

## Data Structure

```
data/
├── scans/
│   ├── stocks/          ← daily stock scan CSVs
│   └── sectors/         ← daily sector scan CSVs
├── spot/
│   ├── universe/        ← full universe spot scans
│   └── singles/         ← per-symbol intraday logs (append daily)
├── watchlists/
│   ├── strong/          ← top N strongest per day
│   └── weak/            ← top N weakest per day
├── reports/
│   ├── sector_trend.csv
│   ├── sector_changes.csv
│   ├── stock_ranking_history.csv
│   ├── stock_NVDA.csv
│   ├── watchlist_strong_frequency.csv
│   └── watchlist_weak_frequency.csv
└── logs/
```

---

## Project Structure

```
src/
├── main.py                  ← CLI entry point (Typer)
├── models/
│   ├── universe.py          ← stock + sector symbol lists
│   └── sector_map.py        ← stock → sector ETF mapping
├── utils/
│   ├── data_ingestion.py    ← Alpaca API data fetching
│   ├── rs_engine.py         ← relative strength scoring engine
│   ├── spot_engine.py       ← intraday momentum engine
│   ├── logger.py            ← scan + watchlist + spot logging
│   └── reports.py           ← historical analysis reports
└── scheduler.py             ← optional: automated daily runs
```

---

## Scheduling (Optional)

For automated daily scans, run the built-in scheduler:

```bash
# Foreground
python -m src.scheduler

# Background
nohup python -m src.scheduler > data/logs/scheduler_nohup.log 2>&1 &
```

Runs at 4:30 PM local time, Monday–Friday. Skips weekends.

Or use a crontab:

```bash
crontab -e
# Add:
30 16 * * 1-5 cd /path/to/RW_stock_scanner && /path/to/python -m src.main scan >> data/logs/cron.log 2>&1
```

---

## Disclaimer

This software is for **educational and research purposes only**. It does not constitute financial advice, investment recommendations, or a solicitation to buy or sell securities. Use at your own risk. The authors assume no liability for financial losses incurred through the use of this software.

---

## License

[MIT](LICENSE)
