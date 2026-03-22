import logging
from pathlib import Path

# Configure logging before any submodule imports run.
# WARNING+ goes to stderr (visible in the terminal); DEBUG+ goes to the log file.
_log_dir = Path(__file__).parent.parent / "data" / "logs"
_log_dir.mkdir(parents=True, exist_ok=True)
_file_handler = logging.FileHandler(_log_dir / "scanner.log")
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s"))
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.WARNING)
_console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logging.basicConfig(level=logging.DEBUG, handlers=[_file_handler, _console_handler])

from typing import Optional

import typer
import pandas as pd
from src.models.universe import STOCK_SYMBOLS, SECTOR_ETFS
from src.models.sector_map import validate_universe
from src.utils.data_ingestion import (
    get_daily_batch, get_weekly_batch, get_hourly_batch,
    get_15m_batch, get_5m_batch,
)
from src.utils.rs_engine import compute_stock_rs
from src.utils.spot_engine import spot_scan_symbol, spot_scan_universe
from src.utils.logger import (
    log_scan, log_watchlists,
    log_spot_universe, log_spot_single,
    log_iv_daily, load_iv_history,
)
from src.utils.iv_ingestion import get_iv_universe
from src.utils.iv_engine import compute_universe_iv_metrics
from src.utils.reports import (
    sector_trend_report,
    sector_change_report,
    stock_tracker_report,
    stock_ranking_report,
    watchlist_frequency_report,
)
from src.utils.charts import generate_all_charts

app = typer.Typer()


def _inject_spy(
    target_daily: dict,
    target_weekly: dict,
    target_hourly: Optional[dict],
    data_daily: dict,
    data_weekly: dict,
    data_hourly: Optional[dict],
) -> None:
    """Inject SPY into a sub-dict so the scoring engine has the benchmark."""
    if "SPY" in data_daily:
        target_daily["SPY"] = data_daily["SPY"]
        target_weekly["SPY"] = data_weekly["SPY"]
        if data_hourly is not None and target_hourly is not None and "SPY" in data_hourly:
            target_hourly["SPY"] = data_hourly["SPY"]


@app.command()
def scan(
    top_n: int = typer.Option(10, help="Number of top/bottom stocks to display"),
    trading_days: int = typer.Option(60, help="Lookback for daily bars"),
    weeks: int = typer.Option(26, help="Lookback for weekly bars"),
    hourly_days: int = typer.Option(30, help="Lookback trading days for hourly bars"),
    no_hourly: bool = typer.Option(False, help="Skip hourly data fetch"),
    sector: Optional[str] = typer.Option(None, help="Filter to a sector ETF (e.g. XLK)"),
):
    """Pull fresh data and score the universe."""

    unmapped = validate_universe(STOCK_SYMBOLS)
    if unmapped:
        typer.echo(f"WARNING: no sector mapping for {unmapped}")

    all_symbols = list(set(STOCK_SYMBOLS + SECTOR_ETFS))

    typer.echo("Fetching daily bars...")
    data_daily = get_daily_batch(all_symbols, trading_days=trading_days)

    typer.echo("Fetching weekly bars...")
    data_weekly = get_weekly_batch(all_symbols, weeks=weeks)

    data_hourly = None
    if not no_hourly:
        typer.echo("Fetching hourly bars...")
        data_hourly = get_hourly_batch(all_symbols, trading_days=hourly_days)

    if not data_daily or not data_weekly:
        typer.echo("ERROR: No data returned. Check API keys / network.")
        raise typer.Exit(code=1)

    typer.echo("Computing relative strength...")

    sector_daily  = {s: data_daily[s]  for s in SECTOR_ETFS if s in data_daily}
    sector_weekly = {s: data_weekly[s] for s in SECTOR_ETFS if s in data_weekly}
    sector_hourly = {s: data_hourly[s] for s in SECTOR_ETFS if s in data_hourly} if data_hourly else None
    _inject_spy(sector_daily, sector_weekly, sector_hourly, data_daily, data_weekly, data_hourly)

    stock_daily  = {s: data_daily[s]  for s in STOCK_SYMBOLS if s in data_daily}
    stock_weekly = {s: data_weekly[s] for s in STOCK_SYMBOLS if s in data_weekly}
    stock_hourly = {s: data_hourly[s] for s in STOCK_SYMBOLS if s in data_hourly} if data_hourly else None
    _inject_spy(stock_daily, stock_weekly, stock_hourly, data_daily, data_weekly, data_hourly)

    sector_df = compute_stock_rs(sector_daily, sector_weekly, sector_hourly)
    stock_df  = compute_stock_rs(stock_daily, stock_weekly, stock_hourly)

    if sector:
        stock_df = stock_df[stock_df["sector"] == sector.upper()]
        if stock_df.empty:
            typer.echo(f"No stocks found for sector {sector.upper()}")
            raise typer.Exit()

    typer.echo("Logging results...")
    scan_meta = {
        "trading_days": trading_days,
        "weeks": weeks,
        "hourly_days": hourly_days,
        "no_hourly": no_hourly,
    }
    sector_path = log_scan(sector_df, scan_type="sector", metadata=scan_meta)
    stock_path  = log_scan(stock_df,  scan_type="stock",  metadata=scan_meta)
    strong_path, weak_path = log_watchlists(stock_df, n=top_n, metadata=scan_meta)

    typer.echo(f"  → {sector_path}")
    typer.echo(f"  → {stock_path}")
    typer.echo(f"  → {strong_path}")
    typer.echo(f"  → {weak_path}")

    display_cols = ["symbol", "sector", "weekly_bias", "daily_bias"]
    if not no_hourly and "hourly_bias" in stock_df.columns:
        display_cols.append("hourly_bias")
    display_cols += ["composite_score", "aligned"]

    typer.echo("\n=== Sector Ranking ===")
    sector_display = [c for c in display_cols if c in sector_df.columns]
    typer.echo(sector_df[sector_display].to_string(index=False))

    typer.echo(f"\n=== Top {top_n} Stocks ===")
    typer.echo(stock_df[display_cols].head(top_n).to_string(index=False))

    typer.echo(f"\n=== Weakest {top_n} Stocks ===")
    typer.echo(stock_df[display_cols].tail(top_n).to_string(index=False))


@app.command()
def spot(
    symbol: Optional[str] = typer.Argument(None, help="Single ticker (e.g. NVDA). Omit for full universe."),
    sector: Optional[str] = typer.Option(None, help="Filter universe to a sector ETF"),
):
    """Intraday momentum spot check — single symbol or full universe."""

    if symbol:
        symbols = list(set([symbol.upper(), "SPY"]))
    else:
        symbols = list(set(STOCK_SYMBOLS + ["SPY"]))

    typer.echo("Fetching daily bars...")
    data_daily = get_daily_batch(symbols, trading_days=60)

    typer.echo("Fetching weekly bars...")
    data_weekly = get_weekly_batch(symbols, weeks=26)

    typer.echo("Fetching 1H bars...")
    data_1h = get_hourly_batch(symbols, trading_days=5)

    typer.echo("Fetching 15m bars...")
    data_15m = get_15m_batch(symbols, trading_days=5)

    typer.echo("Fetching 5m bars...")
    data_5m = get_5m_batch(symbols, trading_days=2)

    if not data_daily:
        typer.echo("ERROR: No data returned.")
        raise typer.Exit(code=1)

    spot_meta = {"mode": "single" if symbol else "universe"}

    if symbol:
        # --- Single symbol ---
        sym = symbol.upper()
        typer.echo(f"\nComputing spot scan for {sym}...")

        result = spot_scan_symbol(
            sym, data_daily, data_weekly,
            data_1h, data_15m, data_5m,
        )

        if result is None:
            typer.echo(f"No data available for {sym}")
            raise typer.Exit(code=1)

        spot_path = log_spot_single(result, metadata=spot_meta)
        typer.echo(f"  → {spot_path}")

        # --- Pretty print ---
        typer.echo(f"\n{'='*50}")
        typer.echo(f"  SPOT SCAN: {sym}")
        typer.echo(f"{'='*50}")

        typer.echo(f"\n--- Context ---")
        typer.echo(f"  Sector:          {result['sector']}")
        typer.echo(f"  Price:           ${result['price']:.2f}")

        typer.echo(f"\n--- ATR ---")
        typer.echo(f"  Weekly ATR:      ${result['weekly_atr']:.2f}" if not pd.isna(result['weekly_atr']) else "  Weekly ATR:      N/A")
        typer.echo(f"  Daily ATR:       ${result['daily_atr']:.2f}" if not pd.isna(result['daily_atr']) else "  Daily ATR:       N/A")
        typer.echo(f"  Hourly ATR:      ${result['hourly_atr']:.2f}" if not pd.isna(result['hourly_atr']) else "  Hourly ATR:      N/A")

        typer.echo(f"\n--- Range Consumed ---")
        typer.echo(f"  Daily:           {result['daily_range_consumed']*100:.1f}%" if not pd.isna(result['daily_range_consumed']) else "  Daily:           N/A")
        typer.echo(f"  Weekly:          {result['weekly_range_consumed']*100:.1f}%" if not pd.isna(result['weekly_range_consumed']) else "  Weekly:           N/A")

        typer.echo(f"\n--- Intraday RS vs SPY ---")
        typer.echo(f"  1H RS:           {result['1h_rs']:+.4f}" if not pd.isna(result['1h_rs']) else "  1H RS:           N/A")
        typer.echo(f"  15m RS:          {result['15m_rs']:+.4f}" if not pd.isna(result['15m_rs']) else "  15m RS:          N/A")
        typer.echo(f"  5m RS:           {result['5m_rs']:+.4f}" if not pd.isna(result['5m_rs']) else "  5m RS:           N/A")

        typer.echo(f"\n--- Intraday Momentum ---")
        typer.echo(f"  Composite:       {result['intraday_composite']:+.4f}" if not pd.isna(result['intraday_composite']) else "  Composite:       N/A")
        typer.echo(f"  Bias:            {result['intraday_bias']}")
        typer.echo(f"  Aligned:         {result['intraday_aligned']}")

        typer.echo(f"\n--- Volume ---")
        typer.echo(f"  RVOL (daily):    {result['rvol_daily']:.2f}x" if not pd.isna(result['rvol_daily']) else "  RVOL (daily):    N/A")
        typer.echo(f"  1H RVOL:         {result['1h_rvol']:.2f}x" if not pd.isna(result['1h_rvol']) else "  1H RVOL:         N/A")
        typer.echo(f"  15m RVOL:        {result['15m_rvol']:.2f}x" if not pd.isna(result['15m_rvol']) else "  15m RVOL:        N/A")
        typer.echo(f"  5m RVOL:         {result['5m_rvol']:.2f}x" if not pd.isna(result['5m_rvol']) else "  5m RVOL:         N/A")

        typer.echo(f"\n--- Distance from Levels ---")
        typer.echo(f"  From daily high: {result['pct_from_daily_high']:+.2f}%" if not pd.isna(result['pct_from_daily_high']) else "  From daily high: N/A")
        typer.echo(f"  From daily low:  {result['pct_from_daily_low']:+.2f}%" if not pd.isna(result['pct_from_daily_low']) else "  From daily low:  N/A")
        typer.echo(f"  From 20d high:   {result['pct_from_20d_high']:+.2f}%" if not pd.isna(result['pct_from_20d_high']) else "  From 20d high:   N/A")
        typer.echo(f"  From 20d low:    {result['pct_from_20d_low']:+.2f}%" if not pd.isna(result['pct_from_20d_low']) else "  From 20d low:    N/A")

    else:
        # --- Full universe ---
        typer.echo("\nComputing spot scan for full universe...")

        spot_df = spot_scan_universe(
            data_daily, data_weekly,
            data_1h, data_15m, data_5m,
        )

        if spot_df.empty:
            typer.echo("No results.")
            raise typer.Exit(code=1)

        if sector:
            spot_df = spot_df[spot_df["sector"] == sector.upper()]
            if spot_df.empty:
                typer.echo(f"No stocks found for sector {sector.upper()}")
                raise typer.Exit()

        spot_path = log_spot_universe(spot_df, metadata=spot_meta)
        typer.echo(f"  → {spot_path}")

        display_cols = [
            "symbol", "sector", "price",
            "daily_atr",
            "daily_range_consumed",
            "1h_rs", "15m_rs", "5m_rs",
            "intraday_composite", "intraday_aligned",
            "rvol_daily",
            "pct_from_20d_high",
        ]
        display_cols = [c for c in display_cols if c in spot_df.columns]

        # Format for display
        display_df = spot_df[display_cols].copy()
        if "daily_range_consumed" in display_df.columns:
            display_df["daily_range_consumed"] = (display_df["daily_range_consumed"] * 100).round(1)
        for col in ["1h_rs", "15m_rs", "5m_rs", "intraday_composite"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].round(4)
        if "pct_from_20d_high" in display_df.columns:
            display_df["pct_from_20d_high"] = display_df["pct_from_20d_high"].round(2)

        typer.echo(f"\n=== Top 10 Intraday Momentum ===")
        typer.echo(display_df.head(10).to_string(index=False))

        typer.echo(f"\n=== Bottom 10 Intraday Momentum ===")
        typer.echo(display_df.tail(10).to_string(index=False))


@app.command()
def report_sectors():
    """Show sector score trends over time."""
    typer.echo("Generating sector trend report...")
    trend = sector_trend_report()
    if not trend.empty:
        typer.echo(trend.to_string())

    typer.echo("\nGenerating sector change report...")
    changes = sector_change_report()
    if not changes.empty:
        typer.echo(changes.to_string(index=False))


@app.command()
def report_stock(
    symbol: str = typer.Argument(..., help="Ticker to track (e.g. NVDA)"),
):
    """Show score history for a single stock."""
    typer.echo(f"Generating report for {symbol.upper()}...")
    report = stock_tracker_report(symbol)
    if not report.empty:
        typer.echo(report.to_string(index=False))
    else:
        typer.echo(f"No scan history found for {symbol.upper()}")


@app.command()
def report_rankings():
    """Show full universe ranking history over time."""
    typer.echo("Generating stock ranking history...")
    rankings = stock_ranking_report()
    if not rankings.empty:
        typer.echo(rankings.to_string())


@app.command()
def report_watchlists(
    top_n: int = typer.Option(10, help="Show top N most frequent symbols"),
):
    """Show which stocks appear most often on strong/weak lists."""
    typer.echo("Analyzing watchlist frequency...\n")

    strong_freq, weak_freq = watchlist_frequency_report()

    if not strong_freq.empty:
        typer.echo("=== Most Frequently Strong ===")
        typer.echo(strong_freq.head(top_n).to_string(index=False))
        typer.echo("")
    else:
        typer.echo("No strong watchlist data yet.\n")

    if not weak_freq.empty:
        typer.echo("=== Most Frequently Weak ===")
        typer.echo(weak_freq.head(top_n).to_string(index=False))
    else:
        typer.echo("No weak watchlist data yet.")


@app.command()
def iv_scan(
    top_n: int = typer.Option(10, help="Number of elevated-IV candidates to display"),
    no_log: bool = typer.Option(False, help="Skip logging IV history (dry run)"),
):
    """
    Fetch IV snapshots, compute IVR/IV percentile/IV-HV ratio, assess VIX environment.

    Run this daily to build IV history. Signal quality improves with 120+ days logged.
    """
    symbols = list(set(STOCK_SYMBOLS + ["SPY"]))

    typer.echo(f"Fetching daily bars for HV computation ({len(symbols)} symbols)...")
    data_daily = get_daily_batch(symbols, trading_days=300)  # need 252+ for HV-252d

    if not data_daily:
        typer.echo("ERROR: No price data returned. Check API keys / network.")
        raise typer.Exit(code=1)

    typer.echo(f"Fetching IV snapshots ({len(STOCK_SYMBOLS)} stocks + SPY)...")
    iv_snapshots = get_iv_universe(symbols)

    if not iv_snapshots:
        typer.echo("ERROR: No IV snapshots returned. Check Alpaca options data access.")
        raise typer.Exit(code=1)

    typer.echo("Computing IV metrics...")
    vix_env, iv_df = compute_universe_iv_metrics(iv_snapshots, data_daily, load_iv_history)

    # --- VIX environment ---
    typer.echo(f"\n=== Market Environment (VIX Proxy) ===")
    typer.echo(f"  SPY IV:       {vix_env['spy_iv']:.4f}  ({vix_env['vix_proxy']:.1f} VIX-equivalent)")
    typer.echo(f"  Environment:  {vix_env['environment'].upper()}")
    typer.echo(f"  Trade OK:     {vix_env['trade_ok']}")
    typer.echo(f"  Note:         {vix_env['note']}")

    if not no_log:
        typer.echo("\nLogging IV history...")
        hist_dir, scan_path = log_iv_daily(iv_snapshots, iv_df if not iv_df.empty else None)
        typer.echo(f"  → history: {hist_dir}")
        if scan_path:
            typer.echo(f"  → snapshot: {scan_path}")

    if iv_df.empty:
        typer.echo("\nNo IV metrics computed.")
        raise typer.Exit()

    # --- Elevated candidates ---
    elevated = iv_df[iv_df["elevated"] == True]

    display_cols = [
        "symbol", "current_iv", "iv_percentile", "iv_rank",
        "iv_hv_ratio_20d", "hv_20d", "signal_quality", "spread_ok",
    ]
    display_cols = [c for c in display_cols if c in iv_df.columns]

    typer.echo(f"\n=== Elevated IV Candidates ({len(elevated)} / {len(iv_df)}) ===")
    if elevated.empty:
        typer.echo("  None — no stocks pass all three filters right now.")
        if not vix_env["trade_ok"]:
            typer.echo("  (Environment gate is STANDBY — systemic vol is masking signal)")
    else:
        typer.echo(elevated[display_cols].head(top_n).to_string(index=False))

    typer.echo(f"\n=== Top {top_n} by IV Percentile (all stocks) ===")
    typer.echo(iv_df[display_cols].head(top_n).to_string(index=False))


@app.command()
def charts():
    """Generate sector RRG, sector heatmap, and stock snapshot charts."""
    typer.echo("Generating charts from scan history...")
    paths = generate_all_charts()
    if not paths:
        typer.echo("No charts generated — run 'scan' first to build history.")
        raise typer.Exit(code=1)
    typer.echo(f"\nGenerated {len(paths)} chart(s):")
    for p in paths:
        typer.echo(f"  → {p}")


@app.command()
def daily(
    top_n: int = typer.Option(10, help="Top/bottom N for watchlists and IV candidates"),
    no_hourly: bool = typer.Option(False, help="Skip hourly data fetch"),
    no_iv: bool = typer.Option(False, help="Skip IV scan"),
    no_charts: bool = typer.Option(False, help="Skip chart generation"),
):
    """
    Full daily pipeline: scan → iv-scan → charts in one command.

    This is what the LaunchAgent calls. Can also be run manually after close.
    """
    errors: list[str] = []

    # --- RS scan ---
    typer.echo("\n" + "="*60)
    typer.echo("STEP 1/3: Relative Strength Scan")
    typer.echo("="*60)
    # Call the underlying logic directly to avoid subprocess overhead
    unmapped = validate_universe(STOCK_SYMBOLS)
    if unmapped:
        typer.echo(f"WARNING: no sector mapping for {unmapped}")

    all_symbols = list(set(STOCK_SYMBOLS + SECTOR_ETFS))

    typer.echo("Fetching daily bars...")
    data_daily = get_daily_batch(all_symbols, trading_days=300)

    typer.echo("Fetching weekly bars...")
    data_weekly = get_weekly_batch(all_symbols, weeks=26)

    data_hourly = None
    if not no_hourly:
        typer.echo("Fetching hourly bars...")
        data_hourly = get_hourly_batch(all_symbols, trading_days=30)

    if not data_daily or not data_weekly:
        typer.echo("ERROR: No price data. Aborting.")
        raise typer.Exit(code=1)

    sector_daily  = {s: data_daily[s]  for s in SECTOR_ETFS if s in data_daily}
    sector_weekly = {s: data_weekly[s] for s in SECTOR_ETFS if s in data_weekly}
    sector_hourly = {s: data_hourly[s] for s in SECTOR_ETFS if s in data_hourly} if data_hourly else None
    _inject_spy(sector_daily, sector_weekly, sector_hourly, data_daily, data_weekly, data_hourly)

    stock_daily  = {s: data_daily[s]  for s in STOCK_SYMBOLS if s in data_daily}
    stock_weekly = {s: data_weekly[s] for s in STOCK_SYMBOLS if s in data_weekly}
    stock_hourly = {s: data_hourly[s] for s in STOCK_SYMBOLS if s in data_hourly} if data_hourly else None
    _inject_spy(stock_daily, stock_weekly, stock_hourly, data_daily, data_weekly, data_hourly)

    sector_df = compute_stock_rs(sector_daily, sector_weekly, sector_hourly)
    stock_df  = compute_stock_rs(stock_daily, stock_weekly, stock_hourly)

    scan_meta = {"trading_days": 300, "weeks": 26, "no_hourly": no_hourly}
    log_scan(sector_df, scan_type="sector", metadata=scan_meta)
    log_scan(stock_df,  scan_type="stock",  metadata=scan_meta)
    log_watchlists(stock_df, n=top_n, metadata=scan_meta)
    typer.echo("RS scan logged.")

    # --- IV scan ---
    if not no_iv:
        typer.echo("\n" + "="*60)
        typer.echo("STEP 2/3: IV Scan")
        typer.echo("="*60)
        try:
            iv_symbols = list(set(STOCK_SYMBOLS + ["SPY"]))
            iv_snapshots = get_iv_universe(iv_symbols)
            if iv_snapshots:
                _, iv_df = compute_universe_iv_metrics(iv_snapshots, data_daily, load_iv_history)
                log_iv_daily(iv_snapshots, iv_df if not iv_df.empty else None)
                elevated_count = int(iv_df["elevated"].sum()) if not iv_df.empty else 0
                typer.echo(f"IV scan logged. Elevated candidates: {elevated_count}")
            else:
                typer.echo("WARNING: No IV snapshots returned — skipping IV log.")
                errors.append("iv-scan: no snapshots")
        except Exception as e:
            typer.echo(f"WARNING: IV scan failed — {e}")
            errors.append(f"iv-scan: {e}")
    else:
        typer.echo("\nSTEP 2/3: IV Scan — skipped (--no-iv)")

    # --- Charts ---
    if not no_charts:
        typer.echo("\n" + "="*60)
        typer.echo("STEP 3/3: Charts")
        typer.echo("="*60)
        try:
            paths = generate_all_charts()
            typer.echo(f"Generated {len(paths)} chart(s).")
            for p in paths:
                typer.echo(f"  → {p}")
        except Exception as e:
            typer.echo(f"WARNING: Charts failed — {e}")
            errors.append(f"charts: {e}")
    else:
        typer.echo("\nSTEP 3/3: Charts — skipped (--no-charts)")

    # --- Summary ---
    typer.echo("\n" + "="*60)
    if errors:
        typer.echo(f"Daily pipeline complete with {len(errors)} warning(s):")
        for err in errors:
            typer.echo(f"  ! {err}")
    else:
        typer.echo("Daily pipeline complete. All steps OK.")
    typer.echo("="*60)


if __name__ == "__main__":
    app()
