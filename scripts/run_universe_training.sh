#!/usr/bin/env bash
set -euo pipefail

export OCTA_DAILY_REFRESH="${OCTA_DAILY_REFRESH:-0}"
export OCTA_ALLOW_NET="${OCTA_ALLOW_NET:-0}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVDIR="octa/var/evidence/universe_training_${STAMP}"
LOCK="/tmp/octa_universe_training.lock"

mkdir -p "$EVDIR"
echo "EVIDENCE_DIR=$EVDIR"

banner() {
  cat <<'EOF'
=============================
        O C T Λ
  Institutional Risk-First
   Quant Architecture
=============================
EOF
}

{
  echo "EVIDENCE_DIR=$EVDIR"
  echo "----- SYSTEM SNAPSHOT -----"
  git rev-parse HEAD || true
  git status --short || true
  date
  echo "OCTA_DAILY_REFRESH=$OCTA_DAILY_REFRESH"
  echo "OCTA_ALLOW_NET=$OCTA_ALLOW_NET"
  echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"
  echo "MKL_NUM_THREADS=$MKL_NUM_THREADS"
  echo "PYTHONHASHSEED=$PYTHONHASHSEED"
  echo "PWD=$(pwd -P)"
} > "$EVDIR/snapshot.txt"

banner > "$EVDIR/training.log"

exec 9>"$LOCK"
if ! command -v flock >/dev/null 2>&1; then
  echo "[FATAL] flock not found. Install util-linux." | tee -a "$EVDIR/training.log"
  exit 43
fi
if ! flock -n 9; then
  echo "[FATAL] Another universe training process is already running (lock: $LOCK)" | tee -a "$EVDIR/training.log"
  exit 42
fi

CFG_ABS="$ROOT/configs/autopilot_daily.yaml"
PY_ABS="$ROOT/scripts/octa_autopilot.py"

if [[ ! -f "$CFG_ABS" ]]; then
  echo "[FATAL] Missing config: $CFG_ABS" | tee -a "$EVDIR/training.log"
  exit 2
fi
if [[ ! -f "$PY_ABS" ]]; then
  echo "[FATAL] Missing autopilot script: $PY_ABS" | tee -a "$EVDIR/training.log"
  exit 2
fi

echo "----- START TRAINING -----" | tee -a "$EVDIR/training.log"

# Evidence: exact command line (no tee dependency on this file)
printf 'python3 %s --config %s\n' "$PY_ABS" "$CFG_ABS" > "$EVDIR/command.txt"

# Launch python3 directly into log file — no pipe, so:
#   1. $! captures the subshell (proxy for python3), not a tee process
#   2. wait returns python3's exit code via subshell
#   3. no SIGPIPE vulnerability from a broken reader
: > "$EVDIR/autopilot_training.log"
set +e
( stdbuf -oL -eL python3 "$PY_ABS" --config "$CFG_ABS" >> "$EVDIR/autopilot_training.log" 2>&1 ) &
PID=$!
set -e

echo "$PID" > "$EVDIR/pid.txt"
echo "[INFO] autopilot pid=$PID" | tee -a "$EVDIR/training.log"

sleep 2
if ! kill -0 "$PID" 2>/dev/null; then
  echo "[FATAL] autopilot exited immediately." | tee -a "$EVDIR/training.log"
  echo "Last 120 lines of autopilot_training.log:" | tee -a "$EVDIR/training.log"
  tail -n 120 "$EVDIR/autopilot_training.log" 2>/dev/null | tee -a "$EVDIR/training.log" || true
  exit 3
fi

# Evidence: process snapshot (confirms real python3 PID inside subshell)
ps -p "$PID" -o pid,ppid,pgid,stat,cmd --no-headers > "$EVDIR/ps_snapshot.txt" 2>/dev/null || \
  pgrep -a -P "$PID" >> "$EVDIR/ps_snapshot.txt" 2>/dev/null || true

echo "[INFO] autopilot running. Tail with:" | tee -a "$EVDIR/training.log"
echo "  tail -F $EVDIR/autopilot_training.log" | tee -a "$EVDIR/training.log"

wait "$PID"
RC=$?
echo "[INFO] autopilot exited rc=$RC" | tee -a "$EVDIR/training.log"
exit "$RC"
