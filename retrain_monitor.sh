#!/usr/bin/env bash
# retrain_monitor.sh — snapshot progress of a running full-cascade retrain.
#
# Usage:
#   ./retrain_monitor.sh <log_file>           # single snapshot
#   ./retrain_monitor.sh <log_file> --watch   # loop every 300s until done
#
# Log format (authoritative from run_full_cascade_training_from_parquets.py):
#   [train]  symbol=X asset_class=equities run_id=...
#   [stage]  symbol=X asset_class=equities tf=1D status=PASS ...
#   [stage]  symbol=X asset_class=equities tf=1D status=GATE_FAIL reason=... pf=... sharpe=... ...
#   [stage]  symbol=X asset_class=equities tf=1D status=SKIP reason=SCOPE_VIOLATION_ASSET_CLASS ...
#   [stage]  symbol=X asset_class=equities tf=1D status=SKIP reason=cascade_previous_not_pass ...
#   [stage]  symbol=X asset_class=equities tf=1D status=TRAIN_ERROR ...
#   [error]  run_exception:signal_abort:SIGTERM
#
# Crisis OOS format (embedded in GATE_FAIL reason field — lowercase):
#   reason=crisis_oos_failed:['rate_hike_2022']
#   reason=crisis_oos_failed:['gfc']
#   reason=crisis_oos_failed:['covid']
#   (crisis_oos_passed = PASS with no special marker — counted in PASS totals)

set -uo pipefail

LOG="${1:-}"
WATCH="${2:-}"

if [[ -z "$LOG" || ! -f "$LOG" ]]; then
    echo "Usage: $0 <log_file> [--watch]" >&2
    exit 1
fi

snapshot() {
    local log="$1"

    # --- symbol counts ---
    local trained
    trained=$(grep -c '^\[train\]' "$log" 2>/dev/null || echo 0)

    # --- 1D stage outcomes ---
    local pass_1d gate_fail_1d skip_scope skip_cascade train_err
    pass_1d=$(grep 'tf=1D.*status=PASS' "$log" 2>/dev/null | wc -l)
    gate_fail_1d=$(grep 'tf=1D.*status=GATE_FAIL' "$log" 2>/dev/null | wc -l)
    skip_scope=$(grep 'reason=SCOPE_VIOLATION_ASSET_CLASS' "$log" 2>/dev/null | wc -l)
    skip_cascade=$(grep 'reason=cascade_previous_not_pass' "$log" 2>/dev/null | wc -l)
    train_err=$(grep 'status=TRAIN_ERROR' "$log" 2>/dev/null | wc -l)

    # --- 1H stage outcomes (only matters when 1D passes) ---
    local pass_1h gate_fail_1h
    pass_1h=$(grep 'tf=1H.*status=PASS' "$log" 2>/dev/null | wc -l)
    gate_fail_1h=$(grep 'tf=1H.*status=GATE_FAIL' "$log" 2>/dev/null | wc -l)

    # --- crisis OOS (lowercase, embedded in GATE_FAIL reason) ---
    # grep -c exits 1 on zero matches (outputs "0"); || true prevents pipefail abort.
    local crisis_failed crisis_rate_hike crisis_gfc crisis_covid
    crisis_failed=$(grep 'reason=crisis_oos_failed:' "$log" 2>/dev/null | wc -l || true)
    crisis_rate_hike=$(grep 'reason=crisis_oos_failed:' "$log" 2>/dev/null | grep 'rate_hike_2022' | wc -l || true)
    crisis_gfc=$(grep 'reason=crisis_oos_failed:' "$log" 2>/dev/null | grep "'gfc'" | wc -l || true)
    crisis_covid=$(grep 'reason=crisis_oos_failed:' "$log" 2>/dev/null | grep "'covid'" | wc -l || true)
    crisis_failed=${crisis_failed:-0}
    crisis_rate_hike=${crisis_rate_hike:-0}
    crisis_gfc=${crisis_gfc:-0}
    crisis_covid=${crisis_covid:-0}

    # current symbol
    local current
    current=$(grep '^\[train\] symbol=' "$log" 2>/dev/null | tail -1 | sed 's/.*symbol=//;s/ .*//')

    # done?
    local done_flag=""
    if grep -q 'run_exception\|DONE\|\[done\]\|\[complete\]' "$log" 2>/dev/null; then
        done_flag=" [RUN ENDED]"
    fi

    printf "[%s]%s\n" "$(date -u +%H:%M)" "$done_flag"
    printf "  Symbols started : %d\n" "$trained"
    printf "  1D PASS         : %d\n" "$pass_1d"
    printf "  1D GATE_FAIL    : %d\n" "$gate_fail_1d"
    printf "  1D Crisis FAIL  : %d  (hard gate — no paper_ready)  rate_hike=%d  gfc=%d  covid=%d\n" \
        "$crisis_failed" "$crisis_rate_hike" "$crisis_gfc" "$crisis_covid"
    printf "  1H PASS         : %d\n" "$pass_1h"
    printf "  1H GATE_FAIL    : %d\n" "$gate_fail_1h"
    printf "  SKIP (scope)    : %d\n" "$skip_scope"
    printf "  SKIP (cascade)  : %d\n" "$skip_cascade"
    printf "  TRAIN_ERR       : %d\n" "$train_err"
    printf "  Current symbol  : %s\n" "${current:-(none)}"
    # --- invariant checks ---
    local other_gate_fail
    other_gate_fail=$(( gate_fail_1d - crisis_failed ))
    local total_accounted
    total_accounted=$(( pass_1d + gate_fail_1d + skip_scope + train_err ))
    # skip_cascade is not independent (each 1D GATE_FAIL creates one 1H SKIP), so don't add it
    if (( crisis_failed > gate_fail_1d )); then
        printf "  [WARN] crisis_failed(%d) > gate_fail_1d(%d) — count error\n" \
            "$crisis_failed" "$gate_fail_1d"
    fi
}

if [[ "$WATCH" == "--watch" ]]; then
    while true; do
        snapshot "$LOG"
        echo ""
        if grep -q 'run_exception\|DONE\|\[done\]\|\[complete\]' "$LOG" 2>/dev/null; then
            echo "Run has ended — stopping monitor."
            break
        fi
        sleep 300
    done
else
    snapshot "$LOG"
fi
