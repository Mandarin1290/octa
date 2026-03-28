#!/bin/bash
# Monday Launch Script — v0.0.0 Validation
# Runs preflight, starts shadow trading (14:20 UTC), then evaluates paper go/no-go (21:30 UTC)
#
# Usage:
#   bash scripts/monday_launch.sh            # full day (shadow 14:20 → paper eval 21:30)
#   bash scripts/monday_launch.sh --dry-run  # preflight only, no shadow start
#
# Requires: TWS running on port 7497 before market open (14:30 UTC)

set -e
OCTA_HOME="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$OCTA_HOME/octa/var/logs"
DRY_RUN=false

# Parse args
for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN=true ;;
    esac
done

mkdir -p "$LOG_DIR"
LAUNCH_LOG="$LOG_DIR/monday_launch_$(date -u +%Y%m%dT%H%M%SZ).log"

log() {
    local ts
    ts="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "[$ts] $*" | tee -a "$LAUNCH_LOG"
}

log "════════════════════════════════════════════════════════════"
log " OCTA Monday Launch — v0.0.0"
log " Log: $LAUNCH_LOG"
log "════════════════════════════════════════════════════════════"

# ── Step 1: Pre-flight check ─────────────────────────────────
log ""
log "STEP 1: Pre-flight check"
cd "$OCTA_HOME"

if python3 scripts/launch_preflight.py --mode paper 2>&1 | tee -a "$LAUNCH_LOG"; then
    log "✅  Pre-flight PASS — system is GO"
else
    log "❌  Pre-flight FAIL — aborting launch"
    log "    Fix blockers listed above, then re-run this script."
    exit 1
fi

if $DRY_RUN; then
    log ""
    log "──────────────────────────────────────────────────────────"
    log " DRY RUN complete — no shadow started"
    log "──────────────────────────────────────────────────────────"
    exit 0
fi

# ── Step 2: Wait for market open (14:30 UTC) if needed ───────
NOW_EPOCH=$(date +%s)
TODAY=$(date -u +%Y-%m-%d)
MARKET_OPEN_EPOCH=$(date -u -d "${TODAY} 14:20:00 UTC" +%s 2>/dev/null || \
                    python3 -c "from datetime import datetime, timezone; \
                    d=datetime.strptime('${TODAY} 14:20:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc); \
                    print(int(d.timestamp()))")

if [ "$NOW_EPOCH" -lt "$MARKET_OPEN_EPOCH" ]; then
    WAIT_SECS=$(( MARKET_OPEN_EPOCH - NOW_EPOCH ))
    log ""
    log "STEP 2: Waiting $(( WAIT_SECS / 60 ))m until 14:20 UTC (market pre-open)..."
    sleep "$WAIT_SECS"
fi

# ── Step 3: Start shadow trading loop ────────────────────────
log ""
log "STEP 3: Starting shadow trading loop"
log "  ADC/1H: PAPER (registry-backed shadow)"
log "  Circuit breakers: HWM 15%, daily loss 5%, kill switch active"
log ""

# Full trading day: 14:30-21:00 UTC = ~6.5h → 13 iterations at 30 min
SHADOW_LOG="$LOG_DIR/shadow_monday_$(date -u +%Y%m%dT%H%M%SZ).log"
INTERVAL_MINUTES=30
MAX_ITERATIONS=13  # 6.5h

ITER=0
START_TIME=$(date +%s)
SHADOW_SIGNALS=0
SHADOW_ERRORS=0

while [ $ITER -lt $MAX_ITERATIONS ]; do
    ITER=$((ITER + 1))
    ELAPSED_M=$(( ($(date +%s) - START_TIME) / 60 ))
    log "  Cycle $ITER/$MAX_ITERATIONS (+${ELAPSED_M}m elapsed)"

    RUN_TS=$(date +%Y%m%dT%H%M%SZ)
    CYCLE_LOG="$LOG_DIR/shadow_cycle_${RUN_TS}.log"

    # Re-run preflight each cycle (fast, < 1s)
    if ! python3 "$OCTA_HOME/scripts/launch_preflight.py" --mode paper >> "$CYCLE_LOG" 2>&1; then
        log "  ❌  Per-cycle preflight FAIL — stopping shadow loop"
        cat "$CYCLE_LOG" | tail -5 | tee -a "$LAUNCH_LOG"
        break
    fi

    # Run shadow cycle
    timeout 300 \
        env OCTA_BROKER_MODE=sandbox \
        env OCTA_MAX_STALE_SECONDS=86400 \
        python3 "$OCTA_HOME/scripts/run_shadow_execution.py" \
        >> "$CYCLE_LOG" 2>&1
    EXIT_CODE=$?

    if [ "$EXIT_CODE" -eq 0 ]; then
        # Check for signal
        SIG=$(grep -oE "SIGNAL|no_signal|shadow signals generated: [0-9]+" "$CYCLE_LOG" 2>/dev/null | tail -1 || echo "ok")
        if echo "$SIG" | grep -qiE "signal|generated"; then
            SHADOW_SIGNALS=$((SHADOW_SIGNALS + 1))
            log "  ✅  Cycle $ITER: 🎯 SIGNAL ($SIG)"
        else
            log "  ✅  Cycle $ITER: no signal ($SIG)"
        fi
    elif [ "$EXIT_CODE" -eq 124 ]; then
        log "  ⚠️  Cycle $ITER: timeout"
        SHADOW_ERRORS=$((SHADOW_ERRORS + 1))
    else
        LAST_ERR=$(grep -iE "error|traceback|RuntimeError" "$CYCLE_LOG" 2>/dev/null | tail -2 || echo "see $CYCLE_LOG")
        log "  ❌  Cycle $ITER: exit=$EXIT_CODE — $LAST_ERR"
        SHADOW_ERRORS=$((SHADOW_ERRORS + 1))
        if [ "$SHADOW_ERRORS" -ge 3 ]; then
            log "  ❌  3 consecutive errors — aborting shadow loop"
            break
        fi
    fi

    if [ $ITER -lt $MAX_ITERATIONS ]; then
        log "  ⏳ Next cycle in ${INTERVAL_MINUTES}m ($(date -u '+%H:%M UTC'))"
        sleep $((INTERVAL_MINUTES * 60))
    fi
done

# ── Step 4: Shadow summary + paper go/no-go ──────────────────
log ""
log "════════════════════════════════════════════════════════════"
log " SHADOW SESSION COMPLETE"
log "  Cycles run:      $ITER / $MAX_ITERATIONS"
log "  Signals:         $SHADOW_SIGNALS"
log "  Errors:          $SHADOW_ERRORS"
log "════════════════════════════════════════════════════════════"

log ""
log "STEP 4: Paper go/no-go evaluation (21:30 UTC)"

# Run preflight one final time for paper decision
if python3 "$OCTA_HOME/scripts/launch_preflight.py" --mode paper 2>&1 | tee -a "$LAUNCH_LOG"; then
    log ""
    log "══════════════════════════════════════════════════════════"
    log " 🟢  PAPER GO — all checks pass"
    log "  Start paper trading:"
    log "    python3 scripts/run_octa.py --mode paper --config configs/autonomous_paper.yaml"
    log "══════════════════════════════════════════════════════════"
else
    log ""
    log "══════════════════════════════════════════════════════════"
    log " 🔴  PAPER NO-GO — circuit breaker(s) triggered during shadow"
    log "  Review shadow logs: $LOG_DIR/"
    log "  Retry paper launch Tuesday after investigation."
    log "══════════════════════════════════════════════════════════"
fi

log ""
log "Launch log: $LAUNCH_LOG"
