#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# install_launchd.sh — register the daily scanner as a macOS LaunchAgent
#
# Run once:
#   bash scripts/install_launchd.sh
#
# To uninstall:
#   bash scripts/install_launchd.sh --uninstall
# ---------------------------------------------------------------------------

set -euo pipefail

PLIST_NAME="com.stockscanner.daily.plist"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/$PLIST_NAME"
DEST="$HOME/Library/LaunchAgents/$PLIST_NAME"

if [ "${1:-}" = "--uninstall" ]; then
    echo "Unloading $PLIST_NAME ..."
    launchctl unload "$DEST" 2>/dev/null || true
    rm -f "$DEST"
    echo "Uninstalled."
    exit 0
fi

# --- Make the shell script executable ---------------------------------------
chmod +x "$SCRIPT_DIR/run_daily.sh"

# --- Copy plist into LaunchAgents -------------------------------------------
cp "$SRC" "$DEST"

# --- Load it ----------------------------------------------------------------
launchctl unload "$DEST" 2>/dev/null || true   # unload first if already registered
launchctl load -w "$DEST"

echo ""
echo "Installed: $DEST"
echo "Scheduled: weekdays at 21:30 UTC (adjust Hour in plist for your timezone)"
echo ""
echo "Verify with:"
echo "  launchctl list | grep stockscanner"
echo ""
echo "To run immediately (test):"
echo "  bash scripts/run_daily.sh"
echo ""
echo "To uninstall:"
echo "  bash scripts/install_launchd.sh --uninstall"
