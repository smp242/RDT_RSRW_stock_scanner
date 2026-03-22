"""
Visualization layer — generates PNG charts from historical scan data.
All charts written to data/reports/charts/.

Three charts:
  1. Sector RRG  — rotation scatter with trailing paths
  2. Sector heatmap — score × date grid with rank annotations
  3. Stock snapshot — full universe RS bar chart, colored by sector
"""

import logging
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # Non-interactive — safe for headless / scheduled use
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src.utils.logger import REPORT_DIR, STOCK_DIR
from src.utils.reports import sector_trend_report, _load_all_scans, _latest_per_day

logger = logging.getLogger(__name__)

CHART_DIR = REPORT_DIR / "charts"

# -------------------------
# Sector style constants
# -------------------------

SECTOR_COLORS: dict[str, str] = {
    "XLK":  "#4C9BE8",  # blue        — Technology
    "XLC":  "#7B68EE",  # slate blue  — Comm Services
    "XLF":  "#2ECC71",  # green       — Financials
    "XLV":  "#E74C3C",  # red         — Healthcare
    "XLY":  "#F39C12",  # orange      — Cons Discretionary
    "XLI":  "#95A5A6",  # gray        — Industrials
    "XLP":  "#1ABC9C",  # teal        — Cons Staples
    "XLE":  "#E67E22",  # dark orange — Energy
    "XLU":  "#9B59B6",  # purple      — Utilities
    "XLB":  "#D4AC0D",  # gold        — Materials
    "XLRE": "#CB4335",  # dark red    — Real Estate
}

SECTOR_LABELS: dict[str, str] = {
    "XLK":  "Technology",
    "XLC":  "Comm Services",
    "XLF":  "Financials",
    "XLV":  "Healthcare",
    "XLY":  "Cons Discr",
    "XLI":  "Industrials",
    "XLP":  "Cons Staples",
    "XLE":  "Energy",
    "XLU":  "Utilities",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
}


# -------------------------
# Chart 1: Sector RRG
# -------------------------

def plot_sector_rrg(trend: pd.DataFrame) -> Optional[Path]:
    """
    Relative Rotation Graph for sector ETFs.

    X axis  — latest composite RS score (relative to universe mean)
    Y axis  — RS momentum (change over the last N sessions)
    Quadrants:
        Leading   (top-right):    strong and strengthening
        Weakening (bottom-right): strong but fading
        Lagging   (bottom-left):  weak and weakening
        Improving (top-left):     weak but turning up
    Each sector shows a trailing path for the last TRAIL_LEN sessions.
    """
    if trend.empty or len(trend) < 2:
        logger.warning("RRG: not enough scan history (need ≥ 2 days).")
        return None

    # Score columns only (drop *_rank columns emitted by sector_trend_report)
    score_cols = [c for c in trend.columns if not str(c).endswith("_rank")]
    scores = trend[score_cols].copy()
    scores.index = pd.to_datetime(scores.index)
    scores = scores.sort_index()

    MOMENTUM_LB = min(5, len(scores) - 1)   # lookback for momentum calc
    TRAIL_LEN   = min(10, len(scores))       # how many past positions to draw

    current = scores.iloc[-1]
    momentum = current - scores.iloc[-(MOMENTUM_LB + 1)]

    # Axis limits with 30% padding
    x_bound = max(abs(current.max()), abs(current.min()), 0.1) * 1.4
    y_bound = max(abs(momentum.max()), abs(momentum.min()), 0.1) * 1.4

    fig, ax = plt.subplots(figsize=(11, 9))
    sns.set_style("whitegrid")

    # Quadrant shading
    ax.axhspan(0, y_bound,  xmin=0.5, xmax=1.0, alpha=0.04, color="#27AE60")  # Leading
    ax.axhspan(0, y_bound,  xmin=0.0, xmax=0.5, alpha=0.04, color="#2980B9")  # Improving
    ax.axhspan(-y_bound, 0, xmin=0.5, xmax=1.0, alpha=0.04, color="#F1C40F")  # Weakening
    ax.axhspan(-y_bound, 0, xmin=0.0, xmax=0.5, alpha=0.04, color="#E74C3C")  # Lagging

    # Quadrant labels (inside corners)
    label_kw = dict(fontsize=10, fontweight="bold", alpha=0.55, ha="center")
    ax.text( x_bound * 0.72,  y_bound * 0.88, "Leading",   color="#27AE60", **label_kw)
    ax.text(-x_bound * 0.72,  y_bound * 0.88, "Improving", color="#2980B9", **label_kw)
    ax.text( x_bound * 0.72, -y_bound * 0.88, "Weakening", color="#B7950B", **label_kw)
    ax.text(-x_bound * 0.72, -y_bound * 0.88, "Lagging",   color="#E74C3C", **label_kw)

    # Centre axes
    ax.axhline(0, color="#555555", linewidth=0.9, linestyle="--", zorder=2)
    ax.axvline(0, color="#555555", linewidth=0.9, linestyle="--", zorder=2)

    # Precompute trailing momentum for each historical point
    def _momentum_at(idx: int) -> float:
        prev = max(0, idx - MOMENTUM_LB)
        return float(scores.iloc[idx].values[0])  # placeholder; computed per-sector below

    # Plot each sector
    for sector in score_cols:
        if sector not in SECTOR_COLORS:
            continue
        color = SECTOR_COLORS[sector]

        # Build trail: (score, momentum) at each of the last TRAIL_LEN bars
        trail_x, trail_y = [], []
        n = len(scores)
        for i in range(n - TRAIL_LEN, n):
            if i < 0:
                continue
            prev_i = max(0, i - MOMENTUM_LB)
            sx = float(scores[sector].iloc[i])
            sy = float(scores[sector].iloc[i] - scores[sector].iloc[prev_i])
            trail_x.append(sx)
            trail_y.append(sy)

        if not trail_x:
            continue

        # Draw fading trail line + dots
        n_trail = len(trail_x)
        for j in range(n_trail - 1):
            alpha = 0.12 + 0.55 * (j / n_trail)
            ax.plot([trail_x[j], trail_x[j + 1]], [trail_y[j], trail_y[j + 1]],
                    color=color, linewidth=1.4, alpha=alpha, zorder=3)
            ax.scatter(trail_x[j], trail_y[j], color=color, s=18,
                       alpha=alpha, zorder=4)

        # Current position — large dot
        cx, cy = trail_x[-1], trail_y[-1]
        ax.scatter(cx, cy, color=color, s=150, zorder=6,
                   edgecolors="white", linewidths=0.8)

        # Arrow from previous to current
        if n_trail >= 2:
            ax.annotate(
                "", xy=(cx, cy),
                xytext=(trail_x[-2], trail_y[-2]),
                arrowprops=dict(arrowstyle="-|>", color=color,
                                lw=1.5, mutation_scale=12),
                zorder=5,
            )

        # Ticker label
        ax.annotate(sector, (cx, cy),
                    textcoords="offset points", xytext=(9, 4),
                    fontsize=9, fontweight="bold", color=color, zorder=7)

    latest_date = scores.index[-1]
    ax.set_xlim(-x_bound, x_bound)
    ax.set_ylim(-y_bound, y_bound)
    ax.set_xlabel("RS Score (composite, z-normalized)", fontsize=11, labelpad=8)
    ax.set_ylabel(f"RS Momentum ({MOMENTUM_LB}-session change)", fontsize=11, labelpad=8)
    ax.set_title(f"Sector Rotation (RRG)  —  {latest_date.strftime('%B %d, %Y')}",
                 fontsize=14, fontweight="bold", pad=14)

    plt.tight_layout()
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CHART_DIR / f"sector_rrg_{latest_date.strftime('%Y%m%d')}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Sector RRG → {out_path}")
    return out_path


# -------------------------
# Chart 2: Sector Heatmap
# -------------------------

def plot_sector_heatmap(trend: pd.DataFrame) -> Optional[Path]:
    """
    Score heatmap: rows = scan dates (most recent at top),
    columns = sector ETFs, colour = composite score.
    Cell annotations show daily rank (1 = strongest).
    """
    if trend.empty:
        logger.warning("Heatmap: no sector scan history.")
        return None

    score_cols = [c for c in trend.columns if not str(c).endswith("_rank")]
    rank_cols  = [c for c in trend.columns if str(c).endswith("_rank")]

    scores = trend[score_cols].copy()
    scores.index = pd.to_datetime(scores.index)
    scores = scores.sort_index(ascending=False)   # most recent at top

    # Align rank frame to same date order
    if rank_cols:
        ranks = trend[rank_cols].copy()
        ranks.columns = score_cols
        ranks.index = pd.to_datetime(ranks.index)
        ranks = ranks.sort_index(ascending=False)
        annot = ranks.astype(int).astype(str)
    else:
        annot = True  # fallback: show raw scores

    n_rows = len(scores)
    fig_height = max(4, n_rows * 0.38 + 2)
    fig, ax = plt.subplots(figsize=(13, fig_height))

    sns.heatmap(
        scores,
        ax=ax,
        cmap="RdYlGn",
        center=0,
        annot=annot,
        fmt="",
        linewidths=0.6,
        linecolor="white",
        cbar_kws={"label": "Composite RS Score", "shrink": 0.75, "pad": 0.02},
    )

    # Format y-axis (dates)
    ax.set_yticklabels(
        [pd.Timestamp(t.get_text()).strftime("%b %d") if hasattr(t, "get_text") else t
         for t in ax.get_yticklabels()],
        fontsize=9, rotation=0,
    )
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=9, rotation=35, ha="right")
    ax.set_ylabel("", labelpad=0)
    ax.set_xlabel("", labelpad=0)
    ax.set_title(
        f"Sector Score Heatmap  —  Last {n_rows} Sessions  (cell = rank)",
        fontsize=13, fontweight="bold", pad=12,
    )

    plt.tight_layout()
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    latest_date = scores.index[0]
    out_path = CHART_DIR / f"sector_heatmap_{latest_date.strftime('%Y%m%d')}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Sector heatmap → {out_path}")
    return out_path


# -------------------------
# Chart 3: Stock Snapshot
# -------------------------

def plot_stock_snapshot(stock_df: pd.DataFrame) -> Optional[Path]:
    """
    Horizontal bar chart of the full stock universe sorted by composite RS score.
    Bars are coloured by sector. A dashed line marks zero (benchmark parity).
    """
    if stock_df.empty:
        logger.warning("Stock snapshot: no stock scan data.")
        return None

    required = {"symbol", "composite_score", "sector"}
    if not required.issubset(stock_df.columns):
        logger.warning(f"Stock snapshot: missing columns {required - set(stock_df.columns)}")
        return None

    df = stock_df.copy().sort_values("composite_score", ascending=True)
    n = len(df)

    colors = [SECTOR_COLORS.get(str(s), "#AAAAAA") for s in df["sector"]]

    fig_height = max(8, n * 0.21 + 2)
    fig, ax = plt.subplots(figsize=(13, fig_height))

    ax.barh(df["symbol"], df["composite_score"], color=colors, height=0.72, zorder=3)

    # Zero reference line
    ax.axvline(0, color="#333333", linewidth=0.9, linestyle="--", alpha=0.6, zorder=4)

    # Sector legend
    present_sectors = df["sector"].dropna().unique()
    legend_handles = [
        mpatches.Patch(
            color=SECTOR_COLORS.get(s, "#AAAAAA"),
            label=f"{s}  {SECTOR_LABELS.get(s, '')}",
        )
        for s in SECTOR_COLORS if s in present_sectors
    ]
    ax.legend(
        handles=legend_handles,
        loc="lower right",
        fontsize=8,
        framealpha=0.85,
        ncol=2,
    )

    latest_date = stock_df["scan_date"].max() if "scan_date" in stock_df.columns else ""
    date_str = pd.Timestamp(latest_date).strftime("%B %d, %Y") if latest_date != "" else ""

    ax.set_xlabel("Composite RS Score (z-normalized)", fontsize=11, labelpad=8)
    ax.set_title(
        f"Stock RS Rankings  —  {date_str}  ({n} symbols)",
        fontsize=14, fontweight="bold", pad=12,
    )
    ax.tick_params(axis="y", labelsize=7.5)
    ax.grid(axis="x", alpha=0.3, zorder=1)

    plt.tight_layout()
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    date_tag = pd.Timestamp(latest_date).strftime("%Y%m%d") if latest_date != "" else "latest"
    out_path = CHART_DIR / f"stock_snapshot_{date_tag}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Stock snapshot → {out_path}")
    return out_path


# -------------------------
# Generate all charts
# -------------------------

def generate_all_charts() -> list[Path]:
    """
    Load data from saved scan history and produce all three charts.
    Returns a list of paths to the generated PNG files.
    """
    paths: list[Path] = []

    # Sector charts — both use the same trend pivot
    trend = sector_trend_report()
    for fn in (plot_sector_rrg, plot_sector_heatmap):
        result = fn(trend)
        if result:
            paths.append(result)

    # Stock snapshot — latest day from saved stock scans
    raw = _load_all_scans(STOCK_DIR)
    if not raw.empty:
        daily = _latest_per_day(raw)
        latest_date = daily["scan_date"].max()
        stock_df = daily[daily["scan_date"] == latest_date].copy()
        result = plot_stock_snapshot(stock_df)
        if result:
            paths.append(result)
    else:
        logger.warning("Stock snapshot: no stock scan history found.")

    return paths
