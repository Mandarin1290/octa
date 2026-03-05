#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# OCTA: SAFE "CHECK → CLEAN → START → VERIFY" Universe Training
# - stops timers/services that can collide
# - detects & kills stale autopilot + stale lock holders
# - starts run_universe_training.sh and captures EVDIR robustly
# - verifies PID, log growth, and run artifact activity
#
# Usage:
#   bash scripts/train_universe_safe.sh
#
# Notes:
# - fail-closed: will not proceed if repo dirty (except whitelisted "2.5")
# - does NOT enable network (keeps OCTA_ALLOW_NET=0)
# ============================================================

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

LOCK="/tmp/octa_universe_training.lock"
WHITELIST_UNTRACKED_REGEX='^(2\.5)$'

export OCTA_DAILY_REFRESH="${OCTA_DAILY_REFRESH:-0}"
export OCTA_ALLOW_NET="${OCTA_ALLOW_NET:-0}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

ts() { date +"[%Y-%m-%d %H:%M:%S %Z]"; }
die(){ echo "$(ts) [FATAL] $*" >&2; exit 1; }

echo "$(ts) [INFO] ROOT=$ROOT"
echo "$(ts) [INFO] env: OCTA_DAILY_REFRESH=$OCTA_DAILY_REFRESH OCTA_ALLOW_NET=$OCTA_ALLOW_NET OMP_NUM_THREADS=$OMP_NUM_THREADS MKL_NUM_THREADS=$MKL_NUM_THREADS PYTHONHASHSEED=$PYTHONHASHSEED"

# ------------------------------------------------------------
# 0) Preconditions
# ------------------------------------------------------------
[ -f "$ROOT/scripts/run_universe_training.sh" ] || die "Missing: scripts/run_universe_training.sh"
[ -f "$ROOT/scripts/octa_autopilot.py" ] || die "Missing: scripts/octa_autopilot.py"
[ -f "$ROOT/configs/autopilot_daily.yaml" ] || die "Missing: configs/autopilot_daily.yaml"

# repo hygiene: allow only "?? 2.5" untracked (your known harmless file)
echo "$(ts) [INFO] Checking git status..."
GIT_STATUS="$(git status --porcelain || true)"
if [ -n "$GIT_STATUS" ]; then
  # reject any staged/modified/deleted
  if echo "$GIT_STATUS" | grep -Eq '^(M|A|D|R|C|U|\?\? )'; then
    # allow untracked-only if all untracked match whitelist
    BAD=0
    while IFS= read -r line; do
      if [[ "$line" =~ ^\?\?\ (.*)$ ]]; then
        f="${BASH_REMATCH[1]}"
        if ! echo "$f" | grep -Eq "$WHITELIST_UNTRACKED_REGEX"; then
          echo "$(ts) [ERROR] Untracked not allowed: $f" >&2
          BAD=1
        fi
      else
        echo "$(ts) [ERROR] Repo not clean (tracked change): $line" >&2
        BAD=1
      fi
    done <<< "$GIT_STATUS"
    [ "$BAD" -eq 0 ] || die "Refusing to run with dirty repo. Commit/stash first."
  fi
fi
echo "$(ts) [INFO] git status OK (clean or whitelisted)."

# ------------------------------------------------------------
# 1) Stop timers/services to prevent overlap
# ------------------------------------------------------------
echo "$(ts) [INFO] Stopping OCTA timers/services (to prevent overlap)..."
systemctl --user stop octa-autopilot.timer 2>/dev/null || true
systemctl --user stop octa-paper-runner.timer 2>/dev/null || true
systemctl --user stop octa-autopilot.service 2>/dev/null || true
systemctl --user stop octa-paper-runner.service 2>/dev/null || true

# ------------------------------------------------------------
# 2) Kill stale autopilot processes (only OCTA autopilot)
# ------------------------------------------------------------
echo "$(ts) [INFO] Searching for stale autopilot processes..."
PIDS="$(ps -eo pid,cmd | grep -E 'python3 .*scripts/octa_autopilot\.py' | grep -v grep | awk '{print $1}' || true)"
if [ -n "${PIDS:-}" ]; then
  echo "$(ts) [WARN] Found autopilot PIDs: $PIDS"
  echo "$(ts) [INFO] Sending SIGTERM..."
  kill $PIDS 2>/dev/null || true
  sleep 3
  STILL="$(ps -p $PIDS -o pid= 2>/dev/null || true)"
  if [ -n "${STILL:-}" ]; then
    echo "$(ts) [WARN] Still running after SIGTERM: $STILL"
    echo "$(ts) [INFO] Sending SIGKILL..."
    kill -9 $STILL 2>/dev/null || true
  fi
else
  echo "$(ts) [INFO] No autopilot processes found."
fi

# ------------------------------------------------------------
# 3) Lock hygiene: if lock is held, terminate holder(s)
# ------------------------------------------------------------
echo "$(ts) [INFO] Checking lock: $LOCK"
if [ -e "$LOCK" ]; then
  # holder PIDs via fuser if available; fallback to lsof
  HOLDERS=""
  if command -v fuser >/dev/null 2>&1; then
    HOLDERS="$(fuser "$LOCK" 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+$' || true)"
  fi
  if [ -z "${HOLDERS:-}" ] && command -v lsof >/dev/null 2>&1; then
    HOLDERS="$(lsof -t "$LOCK" 2>/dev/null || true)"
  fi

  if [ -n "${HOLDERS:-}" ]; then
    echo "$(ts) [WARN] Lock is held by PID(s): $HOLDERS"
    echo "$(ts) [INFO] Terminating lock holders..."
    kill $HOLDERS 2>/dev/null || true
    sleep 2
    STILL2="$(ps -p $HOLDERS -o pid= 2>/dev/null || true)"
    if [ -n "${STILL2:-}" ]; then
      echo "$(ts) [WARN] Lock holders still alive, SIGKILL: $STILL2"
      kill -9 $STILL2 2>/dev/null || true
    fi
  else
    echo "$(ts) [INFO] Lock file exists but not held (stale)."
  fi
else
  echo "$(ts) [INFO] No lock file present."
fi
# do NOT delete lock file; flock handles it atomically

# ------------------------------------------------------------
# 4) Start universe training (capture EVDIR robustly)
# ------------------------------------------------------------
echo "$(ts) [INFO] Starting universe training..."
EVDIR="$(
  ./scripts/run_universe_training.sh \
    | sed -n 's/^EVIDENCE_DIR=//p' \
    | tail -n 1
)"
[ -n "${EVDIR:-}" ] || die "Failed to capture EVDIR from run_universe_training.sh output."
[ -d "$EVDIR" ] || die "EVDIR not found: $EVDIR"
echo "$(ts) [INFO] EVDIR=$EVDIR"

# ------------------------------------------------------------
# 5) Verify PID + liveness
# ------------------------------------------------------------
[ -f "$EVDIR/pid.txt" ] || die "pid.txt missing in $EVDIR (launcher likely failed). Check $EVDIR/training.log"
PID="$(cat "$EVDIR/pid.txt" || true)"
echo "$(ts) [INFO] Training PID=$PID"
if ! ps -p "$PID" >/dev/null 2>&1; then
  echo "$(ts) [ERROR] PID not running. Showing logs..."
  echo "----- training.log -----"; tail -n 200 "$EVDIR/training.log" || true
  echo "----- autopilot_training.log -----"; tail -n 200 "$EVDIR/autopilot_training.log" || true
  die "Autopilot not running."
fi
ps -p "$PID" -o pid,etimes,%cpu,%mem,cmd

# ------------------------------------------------------------
# 6) Sanity checks: log growth and artifacts activity
# ------------------------------------------------------------
LOG="$EVDIR/autopilot_training.log"
touch "$LOG"

echo "$(ts) [INFO] Checking log growth over 30s..."
SZ1="$(stat -c%s "$LOG" 2>/dev/null || echo 0)"
sleep 15
SZ2="$(stat -c%s "$LOG" 2>/dev/null || echo 0)"
sleep 15
SZ3="$(stat -c%s "$LOG" 2>/dev/null || echo 0)"

echo "$(ts) [INFO] log sizes: $SZ1 -> $SZ2 -> $SZ3 bytes"
if [ "$SZ3" -le "$SZ1" ]; then
  echo "$(ts) [WARN] autopilot_training.log did not grow in 30s."
  echo "$(ts) [INFO] Last 60 lines:"
  tail -n 60 "$LOG" || true
  echo "$(ts) [INFO] If it is training a fold, logs may be quiet; continuing."
fi

echo "$(ts) [INFO] Quick artifact activity check..."
LATEST_RUN="$(ls -td artifacts/runs/* 2>/dev/null | head -n 1 || true)"
if [ -n "${LATEST_RUN:-}" ]; then
  echo "$(ts) [INFO] Latest artifacts run: $LATEST_RUN"
  ls -lah "$LATEST_RUN" | head -n 30 || true
  if [ -f "$LATEST_RUN/stage_progress.jsonl" ]; then
    echo "$(ts) [INFO] stage_progress tail:"
    tail -n 30 "$LATEST_RUN/stage_progress.jsonl" || true
  fi
else
  echo "$(ts) [WARN] No artifacts/runs found yet."
fi

# ------------------------------------------------------------
# 7) Re-enable timers (optional, safe)
# ------------------------------------------------------------
echo "$(ts) [INFO] Re-enabling timers (safe defaults)..."
systemctl --user start octa-autopilot.timer 2>/dev/null || true
systemctl --user start octa-paper-runner.timer 2>/dev/null || true

echo "$(ts) [INFO] DONE. To follow live logs:"
echo "  tail -F $EVDIR/autopilot_training.log"
echo "  tail -F $EVDIR/training.log"
