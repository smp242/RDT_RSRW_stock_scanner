#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_daily.sh — daily stock scanner automation
#
# Runs after market close (scheduled via LaunchAgent or cron).
# Chains: scan → iv-scan → charts
# Output is timestamped and appended to data/logs/daily_run.log
#
# Usage (manual):
#   bash scripts/run_daily.sh
# ---------------------------------------------------------------------------

set -euo pipefail

# --- Paths ------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONDA_PYTHON="/Users/smp/miniconda3/envs/ds313_trading_journal/bin/python"
LOG_FILE="$PROJECT_DIR/data/logs/daily_run.log"

# --- Create log dir if needed -----------------------------------------------
mkdir -p "$PROJECT_DIR/data/logs"

# --- Helper: log with timestamp ----------------------------------------------
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*" | tee -a "$LOG_FILE"
}

# --- Skip on weekends -------------------------------------------------------
DOW=$(date +%u)  # 1=Mon … 7=Sun
if [ "$DOW" -ge 6 ]; then
    log "Weekend — skipping scan."
    exit 0
fi

# ---------------------------------------------------------------------------
log "===== Daily scan starting ================================================"
cd "$PROJECT_DIR"

# --- RS scan ----------------------------------------------------------------
log "Running: scan"
if "$CONDA_PYTHON" -m src.main scan >> "$LOG_FILE" 2>&1; then
    log "scan: OK"
else
    log "scan: FAILED (exit $?)"
    # Continue to iv-scan even if RS scan fails — IV history must not be skipped
fi

# --- IV scan ----------------------------------------------------------------
log "Running: iv-scan"
if "$CONDA_PYTHON" -m src.main iv-scan >> "$LOG_FILE" 2>&1; then
    log "iv-scan: OK"
else
    log "iv-scan: FAILED (exit $?)"
fi

# --- Charts -----------------------------------------------------------------
log "Running: charts"
if "$CONDA_PYTHON" -m src.main charts >> "$LOG_FILE" 2>&1; then
    log "charts: OK"
else
    log "charts: FAILED (skipping — scan history may be empty)"
fi

log "===== Daily scan complete ================================================"
