#!/usr/bin/env bash
# daily_refresh.sh — AltData daily refresh entry point.
# Called by octa-daily-refresh.service (runs before octa-autopilot.service).
#
# Environment (set by service unit):
#   OCTA_DAILY_REFRESH=1    — enables network fetch in build_altdata_stack
#   OCTA_CONTEXT=refresh    — marks this as a refresh (not training/research)
#
# Training path is UNAFFECTED: config/altdat.yaml offline_only=true is
# enforced independently; this script only updates the local cache.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="$PROJECT_DIR/.venv/bin/python"

cd "$PROJECT_DIR"

exec "$PYTHON" scripts/run_altdata_refresh.py "$@"
