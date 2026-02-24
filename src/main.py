import typer
from src.models.universe import STOCK_SYMBOLS, SECTOR_ETFS
from src.models.sector_map import validate_universe
from src.utils.data_ingestion import get_daily_batch, get_weekly_batch, get_hourly_batch
from src.utils.rs_engine import compute_stock_rs
from src.utils.logger import log_scan
from src.utils.reports import (
    sector_trend_report,
    sector_change_report,
    stock_tracker_report,
    stock_ranking_report,
)

app = typer.Typer()


@app.command()
def scan(
    top_n: int = typer.Option(10, help="Number of top/bottom stocks to display"),
    trading_days: int = typer.Option(60, help="Lookback for daily bars"),
    weeks: int = typer.Option(26, help="Lookback for weekly bars"),
    hourly_days: int = typer.Option(30, help="Lookback trading days for hourly bars"),
    no_hourly: bool = typer.Option(False, help="Skip hourly data fetch"),
    sector: str = typer.Option(None, help="Filter to a sector ETF (e.g. XLK)"),
):
    """Pull fresh data and score the universe."""

    unmapped = validate_universe(STOCK_SYMBOLS)
    if unmapped:
        typer.echo(f"WARNING: no sector mapping for {unmapped}")

    # -- Data Ingestion --
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

    # -- Compute RS --
    typer.echo("Computing relative strength...")

    sector_daily  = {s: data_daily[s]  for s in SECTOR_ETFS if s in data_daily}
    sector_weekly = {s: data_weekly[s] for s in SECTOR_ETFS if s in data_weekly}
    sector_hourly = None
    if data_hourly:
        sector_hourly = {s: data_hourly[s] for s in SECTOR_ETFS if s in data_hourly}

    if "SPY" in data_daily:
        sector_daily["SPY"]  = data_daily["SPY"]
        sector_weekly["SPY"] = data_weekly["SPY"]
        if data_hourly is not None and sector_hourly is not None and "SPY" in data_hourly:
            sector_hourly["SPY"] = data_hourly["SPY"]

    stock_daily  = {s: data_daily[s]  for s in STOCK_SYMBOLS if s in data_daily}
    stock_weekly = {s: data_weekly[s] for s in STOCK_SYMBOLS if s in data_weekly}
    stock_hourly = None
    if data_hourly:
        stock_hourly = {s: data_hourly[s] for s in STOCK_SYMBOLS if s in data_hourly}

    if "SPY" in data_daily:
        stock_daily["SPY"]  = data_daily["SPY"]
        stock_weekly["SPY"] = data_weekly["SPY"]
        if data_hourly is not None and stock_hourly is not None and "SPY" in data_hourly:
            stock_hourly["SPY"] = data_hourly["SPY"]

    sector_df = compute_stock_rs(sector_daily, sector_weekly, sector_hourly)
    stock_df  = compute_stock_rs(stock_daily, stock_weekly, stock_hourly)

    if sector:
        stock_df = stock_df[stock_df["sector"] == sector.upper()]
        if stock_df.empty:
            typer.echo(f"No stocks found for sector {sector.upper()}")
            raise typer.Exit()

    # -- Log --
    typer.echo("Logging results...")
    scan_meta = {
        "trading_days": trading_days,
        "weeks": weeks,
        "hourly_days": hourly_days,
        "no_hourly": no_hourly,
    }
    sector_path = log_scan(sector_df, scan_type="sector", metadata=scan_meta)
    stock_path  = log_scan(stock_df,  scan_type="stock",  metadata=scan_meta)
    typer.echo(f"  → {sector_path}")
    typer.echo(f"  → {stock_path}")

    # -- Display --
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


if __name__ == "__main__":
    app()
