#!/usr/bin/env bash
set -euo pipefail

# OCTA Universe Training Supervisor
# Usage:
#   bash scripts/supervise_universe_training.sh
#
# Optional:
#   SUPERVISOR_DRY_RUN=1 bash scripts/supervise_universe_training.sh
#     Preflight/snapshots only; no launch, kill, or restart actions.

export OCTA_DAILY_REFRESH="${OCTA_DAILY_REFRESH:-0}"
export OCTA_ALLOW_NET="${OCTA_ALLOW_NET:-0}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

echo "non_canonical_supervisor_entrypoint:scripts/supervise_universe_training.sh:use_scripts/run_octa.py_for_canonical_foundation_orchestration" >&2
exit 2

LOCK_FILE="/tmp/octa_universe_training.lock"
UTCSTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUP_EVDIR="octa/var/evidence/universe_supervisor_${UTCSTAMP}"
SUP_LOG="$SUP_EVDIR/supervisor.log"
SUPERVISOR_DRY_RUN="${SUPERVISOR_DRY_RUN:-0}"
MAX_RESTARTS=2
PROGRESS_HANG_SECONDS=300
SUCCESS_WINDOW_SECONDS=600
FAST_EXIT_SECONDS=60
POLL_SECONDS=5

LAST_LAUNCHER_PID=""
LAST_EVDIR=""
CURRENT_PID=""
declare -A UNIT_WAS_ACTIVE
declare -A UNIT_WAS_ENABLED
UNITS=(
  "octa-autopilot.timer"
  "octa-autopilot.service"
  "octa-paper-runner.timer"
  "octa-paper-runner.service"
)

mkdir -p "$SUP_EVDIR"
touch "$SUP_LOG"

ts_utc() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
now_epoch() { date -u +%s; }
log() {
  echo "[$(ts_utc)] $*" | tee -a "$SUP_LOG" >&2
}
die() {
  log "[FATAL] $*"
  exit 1
}

unit_is_active_raw() {
  local unit="$1"
  systemctl --user is-active "$unit" 2>&1 || true
}
unit_is_enabled_raw() {
  local unit="$1"
  systemctl --user is-enabled "$unit" 2>&1 || true
}
unit_is_stopped_state() {
  local st="$1"
  [[ "$st" == "inactive" || "$st" == "failed" ]]
}

capture_lock_holders() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -t "$LOCK_FILE" 2>/dev/null | awk 'NF' | sort -u || true
    return
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -v "$LOCK_FILE" 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/) print $i}' | sort -u || true
    return
  fi

  local pid fd target
  for pidpath in /proc/[0-9]*; do
    pid="${pidpath#/proc/}"
    for fd in "$pidpath"/fd/*; do
      [ -e "$fd" ] || continue
      target="$(readlink "$fd" 2>/dev/null || true)"
      if [[ "$target" == "$LOCK_FILE" || "$target" == "$LOCK_FILE (deleted)" ]]; then
        echo "$pid"
        break
      fi
    done
  done | sort -u || true
}

cmdline_for_pid() {
  local pid="$1"
  tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true
}

is_same_user_pid() {
  local pid="$1"
  local owner
  owner="$(ps -o user= -p "$pid" 2>/dev/null | awk '{print $1}' || true)"
  [[ -n "$owner" && "$owner" == "$USER" ]]
}

is_octa_autopilot_pid() {
  local pid="$1"
  local cmd
  [ -d "/proc/$pid" ] || return 1
  cmd="$(cmdline_for_pid "$pid")"
  [[ "$cmd" == *"python3"* && "$cmd" == *"scripts/octa_autopilot.py"* ]]
}

write_redacted_env_snapshot() {
  env | sort | while IFS= read -r line; do
    if [[ "$line" != *=* ]]; then
      printf '%s\n' "$line"
      continue
    fi
    local key="${line%%=*}"
    local upper="${key^^}"
    if [[ "$upper" == *PASS* || "$upper" == *SECRET* || "$upper" == *TOKEN* || "$upper" == *KEY* || "$upper" == *USER* || "$upper" == *ACCOUNT* || "$upper" == *AUTH* ]]; then
      printf '%s=<redacted>\n' "$key"
      continue
    fi
    printf '%s\n' "$line"
  done
}

snapshot_preflight() {
  {
    echo "ROOT=$ROOT"
    echo "SUP_EVDIR=$SUP_EVDIR"
    echo "UTCSTAMP=$UTCSTAMP"
    echo "SUPERVISOR_DRY_RUN=$SUPERVISOR_DRY_RUN"
    echo "OCTA_DAILY_REFRESH=$OCTA_DAILY_REFRESH"
    echo "OCTA_ALLOW_NET=$OCTA_ALLOW_NET"
    echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"
    echo "MKL_NUM_THREADS=$MKL_NUM_THREADS"
    echo "PYTHONHASHSEED=$PYTHONHASHSEED"
    echo "PYTHONUNBUFFERED=$PYTHONUNBUFFERED"
    write_redacted_env_snapshot
  } >"$SUP_EVDIR/env_snapshot.txt"

  {
    echo "HEAD:"
    git rev-parse HEAD 2>&1 || true
    echo
    echo "STATUS:"
    git status --short 2>&1 || true
  } >"$SUP_EVDIR/git_snapshot.txt"

  {
    ps -eo pid,ppid,user,stat,etimes,%cpu,%mem,cmd
  } >"$SUP_EVDIR/process_snapshot.txt" || true

  {
    for unit in "${UNITS[@]}"; do
      echo "$unit active=$(unit_is_active_raw "$unit") enabled=$(unit_is_enabled_raw "$unit")"
    done
  } >"$SUP_EVDIR/systemd_snapshot.txt"
}

check_repo_hygiene() {
  local status line bad=0 file
  local -a allowlist=(
    "2.5"
    "scripts/run_universe_training.sh"
    "scripts/train_universe_safe.sh"
    "scripts/supervise_universe_training.sh"
  )
  status="$(git status --porcelain || true)"
  [ -n "$status" ] || return 0

  while IFS= read -r line; do
    [ -n "$line" ] || continue
    if [[ "$line" =~ ^\?\?\ (.+)$ ]]; then
      file="${BASH_REMATCH[1]}"
      local allowed=0 entry
      for entry in "${allowlist[@]}"; do
        if [[ "$file" == "$entry" ]]; then
          allowed=1
          break
        fi
      done
      if [[ "$allowed" -ne 1 ]]; then
        log "[ERROR] Untracked path not allowlisted: $file"
        bad=1
      fi
    else
      log "[ERROR] Tracked repo change present: $line"
      bad=1
    fi
  done <<<"$status"

  [[ "$bad" -eq 0 ]] || die "Repo hygiene failed (allowlisted untracked only)."
}

capture_timers_state() {
  for unit in "${UNITS[@]}"; do
    UNIT_WAS_ACTIVE["$unit"]="$(unit_is_active_raw "$unit")"
    UNIT_WAS_ENABLED["$unit"]="$(unit_is_enabled_raw "$unit")"
  done
  {
    for unit in "${UNITS[@]}"; do
      echo "$unit pre_active=${UNIT_WAS_ACTIVE[$unit]} pre_enabled=${UNIT_WAS_ENABLED[$unit]}"
    done
  } >>"$SUP_EVDIR/systemd_snapshot.txt"
}

stop_orchestration_units() {
  local unit active
  for unit in "${UNITS[@]}"; do
    systemctl --user stop "$unit" 2>/dev/null || true
  done

  {
    echo "post_stop_states:"
    for unit in "${UNITS[@]}"; do
      active="$(unit_is_active_raw "$unit")"
      echo "$unit active=$active"
      if ! unit_is_stopped_state "$active"; then
        die "Failed to stop $unit (state=$active)"
      fi
    done
  } >>"$SUP_EVDIR/systemd_snapshot.txt"
}

restore_orchestration_units() {
  local unit want_active
  for unit in "${UNITS[@]}"; do
    want_active="${UNIT_WAS_ACTIVE[$unit]:-unknown}"
    if [[ "$want_active" == "active" ]]; then
      systemctl --user start "$unit" 2>/dev/null || true
    else
      systemctl --user stop "$unit" 2>/dev/null || true
    fi
  done
}

final_systemd_snapshot() {
  {
    echo "final_states:"
    for unit in "${UNITS[@]}"; do
      echo "$unit active=$(unit_is_active_raw "$unit") enabled=$(unit_is_enabled_raw "$unit")"
    done
  } >>"$SUP_EVDIR/systemd_snapshot.txt"
}

ensure_lock_state() {
  local holders
  holders="$(capture_lock_holders || true)"
  if [[ -n "$holders" ]]; then
    log "[WARN] lock holders detected for $LOCK_FILE: $holders"
    while IFS= read -r pid; do
      [ -n "$pid" ] || continue
      if ! is_same_user_pid "$pid"; then
        die "Lock held by another user pid=$pid"
      fi
      if is_octa_autopilot_pid "$pid"; then
        log "[INFO] lock holder pid=$pid is OCTA autopilot (treated as running)."
      else
        die "Lock held by non-OCTA process pid=$pid cmd='$(cmdline_for_pid "$pid")'"
      fi
    done <<<"$holders"
    die "Existing OCTA universe training appears to be running; refusing overlap."
  fi

  if [[ -e "$LOCK_FILE" ]]; then
    log "[WARN] stale lock file detected (exists but unheld): $LOCK_FILE"
  else
    log "[INFO] no lock present."
  fi
}

await_evidence_dir_from_launcher() {
  local launcher_out="$1"
  local wait_deadline="$2"
  local ev=""
  while (( "$(now_epoch)" < wait_deadline )); do
    ev="$(sed -n 's/^EVIDENCE_DIR=//p' "$launcher_out" 2>/dev/null | tail -n 1 || true)"
    if [[ -n "$ev" ]]; then
      echo "$ev"
      return 0
    fi
    if ! kill -0 "$LAST_LAUNCHER_PID" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  return 1
}

newest_run_dir() {
  if [[ ! -d "artifacts/runs" ]]; then
    return 0
  fi
  find "artifacts/runs" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr \
    | head -n 1 \
    | awk '{print $2}' || true
}

recent_artifact_file_count() {
  local run_dir="$1"
  local recent_s="$2"
  [[ -n "$run_dir" && -d "$run_dir" ]] || { echo 0; return; }
  find "$run_dir" -type f -printf '%T@\n' 2>/dev/null \
    | awk -v now="$(now_epoch)" -v sec="$recent_s" '$1 >= (now-sec) {n++} END {print n+0}'
}

capture_intervention_evidence() {
  local attempt_dir="$1"
  local pid="$2"
  local evdir="$3"
  local reason="$4"
  local run_dir stage_file
  local snap="$attempt_dir/intervention_$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$snap"

  {
    echo "reason=$reason"
    echo "pid=$pid"
    echo "evdir=$evdir"
    echo "time=$(ts_utc)"
  } >"$snap/context.txt"

  ps -eo pid,ppid,user,stat,etimes,%cpu,%mem,cmd \
    | grep -E 'python3 .*scripts/octa_autopilot\.py|java|tws|ibgateway|octa_autopilot' \
    | grep -v grep >"$snap/ps_relevant.txt" || true

  if [[ -d "/proc/$pid" ]]; then
    cat "/proc/$pid/status" >"$snap/proc_status.txt" 2>/dev/null || true
    tr '\0' ' ' <"/proc/$pid/cmdline" >"$snap/proc_cmdline.txt" 2>/dev/null || true
    readlink "/proc/$pid/cwd" >"$snap/proc_cwd.txt" 2>/dev/null || true
    ls -l "/proc/$pid/fd" 2>/dev/null | head -n 200 >"$snap/proc_fd_head.txt" || true
  fi

  tail -n 200 "$evdir/autopilot_training.log" >"$snap/autopilot_training_tail_200.txt" 2>/dev/null || true

  run_dir="$(newest_run_dir)"
  if [[ -n "$run_dir" && -d "$run_dir" ]]; then
    stage_file="$run_dir/stage_progress.jsonl"
    if [[ -f "$stage_file" ]]; then
      tail -n 200 "$stage_file" >"$snap/stage_progress_tail_200.txt" 2>/dev/null || true
    fi
  fi

  find "artifacts/runs" -type f -printf '%TY-%Tm-%TdT%TH:%TM:%TSZ %p\n' 2>/dev/null \
    | sort \
    | tail -n 200 >"$snap/newest_artifacts_200.txt" || true
}

kill_training_safely() {
  local pid="$1"
  local t=0
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && [[ "$t" -lt 10 ]]; do
      sleep 1
      t=$((t + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi

  local candidates pid2
  candidates="$(ps -eo pid,user,args \
    | awk -v u="$USER" '$2==u && $0 ~ /python3 .*scripts\/octa_autopilot\.py/ {print $1}' || true)"
  while IFS= read -r pid2; do
    [ -n "$pid2" ] || continue
    if is_octa_autopilot_pid "$pid2"; then
      kill "$pid2" 2>/dev/null || true
      sleep 1
      kill -9 "$pid2" 2>/dev/null || true
    fi
  done <<<"$candidates"

  local holders remain
  holders="$(capture_lock_holders || true)"
  if [[ -n "$holders" ]]; then
    die "Lock still held after kill sequence: $holders"
  fi
  remain="$(ps -eo pid,user,args \
    | awk -v u="$USER" '$2==u && $0 ~ /python3 .*scripts\/octa_autopilot\.py/ {print $1}' || true)"
  if [[ -n "$remain" ]]; then
    die "Autopilot processes remain after kill sequence: $remain"
  fi
}

extract_failure_reason() {
  local evdir="$1"
  local reason
  reason="$(grep -Eo 'budget_exhausted_no_candidates|no_candidates|insufficient_[a-z_]+|[A-Za-z0-9_]*fail[a-zA-Z0-9_]*' \
    "$evdir/autopilot_training.log" 2>/dev/null | tail -n 1 || true)"
  if [[ -z "$reason" ]]; then
    reason="$(tail -n 20 "$evdir/autopilot_training.log" 2>/dev/null | sed '/^[[:space:]]*$/d' | tail -n 1 || true)"
  fi
  echo "${reason:-unknown_failure}"
}

monitor_attempt() {
  local attempt="$1"
  local attempt_dir="$SUP_EVDIR/attempt_${attempt}_$(date -u +%Y%m%dT%H%M%SZ)"
  local launcher_out="$attempt_dir/launcher_stdout.log"
  local launcher_wait_deadline
  local start_t now_t end_success_window
  local evdir pid logf run_dir stage_file
  local initial_log_size=0 current_log_size=0
  local initial_stage_size=0 current_stage_size=0
  local last_progress_t
  local recent_files=0

  mkdir -p "$attempt_dir"
  log "[INFO] Attempt $attempt: launching canonical training runner."

  ( bash "$ROOT/scripts/run_universe_training.sh" >"$launcher_out" 2>&1 ) &
  LAST_LAUNCHER_PID=$!
  echo "$LAST_LAUNCHER_PID" >"$attempt_dir/launcher_pid.txt"

  launcher_wait_deadline=$(( "$(now_epoch)" + 90 ))
  evdir="$(await_evidence_dir_from_launcher "$launcher_out" "$launcher_wait_deadline" || true)"
  [[ -n "$evdir" ]] || {
    tail -n 200 "$launcher_out" >"$attempt_dir/launcher_tail_200.txt" 2>/dev/null || true
    die "Attempt $attempt: failed to parse EVIDENCE_DIR from launcher output."
  }
  [[ -d "$evdir" ]] || die "Attempt $attempt: parsed EVDIR missing: $evdir"
  LAST_EVDIR="$evdir"
  echo "$evdir" >"$attempt_dir/evdir.txt"

  local wait_files_deadline=$(( "$(now_epoch)" + 30 ))
  while (( "$(now_epoch)" < wait_files_deadline )); do
    if [[ -f "$evdir/pid.txt" && -f "$evdir/command.txt" && -f "$evdir/autopilot_training.log" ]]; then
      break
    fi
    sleep 1
  done
  [[ -f "$evdir/pid.txt" ]] || die "Attempt $attempt: missing $evdir/pid.txt"
  [[ -f "$evdir/command.txt" ]] || die "Attempt $attempt: missing $evdir/command.txt"
  [[ -f "$evdir/autopilot_training.log" ]] || die "Attempt $attempt: missing $evdir/autopilot_training.log"

  pid="$(cat "$evdir/pid.txt" | tr -d '[:space:]')"
  [[ "$pid" =~ ^[0-9]+$ ]] || die "Attempt $attempt: invalid pid in $evdir/pid.txt: $pid"
  CURRENT_PID="$pid"
  kill -0 "$pid" 2>/dev/null || {
    tail -n 200 "$evdir/autopilot_training.log" >"$attempt_dir/autopilot_tail_200_dead_pid.txt" 2>/dev/null || true
    die "Attempt $attempt: PID $pid not alive after launch."
  }

  logf="$evdir/autopilot_training.log"
  start_t="$(now_epoch)"
  end_success_window=$((start_t + SUCCESS_WINDOW_SECONDS))
  last_progress_t="$start_t"
  initial_log_size="$(stat -c %s "$logf" 2>/dev/null || echo 0)"
  run_dir="$(newest_run_dir)"
  stage_file=""
  if [[ -n "$run_dir" && -f "$run_dir/stage_progress.jsonl" ]]; then
    stage_file="$run_dir/stage_progress.jsonl"
    initial_stage_size="$(stat -c %s "$stage_file" 2>/dev/null || echo 0)"
  fi

  while true; do
    now_t="$(now_epoch)"
    if ! kill -0 "$pid" 2>/dev/null; then
      if (( now_t - start_t <= FAST_EXIT_SECONDS )); then
        echo "fast_exit"
        return 10
      fi
      local reason
      reason="$(extract_failure_reason "$evdir")"
      log "[ERROR] Attempt $attempt exited before progress tick. reason=$reason"
      echo "clean_failure:$reason"
      return 20
    fi

    local tick=0
    current_log_size="$(stat -c %s "$logf" 2>/dev/null || echo 0)"
    if (( current_log_size > initial_log_size )); then
      tick=1
      initial_log_size="$current_log_size"
    fi

    run_dir="$(newest_run_dir)"
    recent_files=0
    if [[ -n "$run_dir" ]]; then
      recent_files="$(recent_artifact_file_count "$run_dir" "$POLL_SECONDS" || echo 0)"
      if [[ "$recent_files" -gt 0 ]]; then
        tick=1
      fi
      if [[ -f "$run_dir/stage_progress.jsonl" ]]; then
        stage_file="$run_dir/stage_progress.jsonl"
        current_stage_size="$(stat -c %s "$stage_file" 2>/dev/null || echo 0)"
        if (( current_stage_size > initial_stage_size )); then
          tick=1
          initial_stage_size="$current_stage_size"
        fi
      fi
    fi

    if (( tick == 1 )); then
      last_progress_t="$now_t"
      log "[INFO] Attempt $attempt progress tick observed. pid=$pid evdir=$evdir"
      echo "progress"
      return 0
    fi

    if (( now_t - last_progress_t >= PROGRESS_HANG_SECONDS )); then
      echo "hang"
      return 11
    fi
    if (( now_t >= end_success_window )); then
      echo "no_progress_within_10m"
      return 12
    fi
    sleep "$POLL_SECONDS"
  done
}

cleanup() {
  local rc=$?
  set +e
  log "[INFO] supervisor cleanup rc=$rc"
  restore_orchestration_units
  final_systemd_snapshot
  if [[ -n "$LAST_EVDIR" ]]; then
    log "[INFO] tail logs: tail -F $LAST_EVDIR/autopilot_training.log"
  fi
  log "[INFO] supervisor evidence: $SUP_EVDIR"
  exit "$rc"
}
trap cleanup EXIT

main() {
  command -v systemctl >/dev/null 2>&1 || die "systemctl is required."
  [[ -f "$ROOT/scripts/run_universe_training.sh" ]] || die "Missing scripts/run_universe_training.sh"
  [[ -f "$ROOT/scripts/octa_autopilot.py" ]] || die "Missing scripts/octa_autopilot.py"
  [[ -f "$ROOT/configs/autopilot_daily.yaml" ]] || die "Missing configs/autopilot_daily.yaml"

  snapshot_preflight
  check_repo_hygiene
  capture_timers_state

  if [[ "$SUPERVISOR_DRY_RUN" == "1" ]]; then
    log "[INFO] SUPERVISOR_DRY_RUN=1 preflight complete; no launch/kill performed."
    return 0
  fi

  stop_orchestration_units
  ensure_lock_state

  local attempt=1 restarts=0 outcome rc
  while :; do
    set +e
    outcome="$(monitor_attempt "$attempt")"
    rc=$?
    set -e

    log "[INFO] Attempt $attempt outcome=$outcome rc=$rc"
    case "$rc" in
      0)
        log "[INFO] Success: training running with at least one progress tick."
        return 0
        ;;
      10|11|12)
        if [[ -n "$CURRENT_PID" && -n "$LAST_EVDIR" ]]; then
          capture_intervention_evidence "$SUP_EVDIR" "$CURRENT_PID" "$LAST_EVDIR" "$outcome"
          kill_training_safely "$CURRENT_PID"
        fi
        if [[ "$restarts" -lt "$MAX_RESTARTS" ]]; then
          restarts=$((restarts + 1))
          attempt=$((attempt + 1))
          CURRENT_PID=""
          LAST_EVDIR=""
          LAST_LAUNCHER_PID=""
          log "[WARN] Restarting after outcome=$outcome (restart $restarts/$MAX_RESTARTS)."
          continue
        fi
        die "Exceeded restart budget ($MAX_RESTARTS) after outcome=$outcome"
        ;;
      20)
        die "Clean failure detected before progress tick: $outcome"
        ;;
      *)
        die "Unexpected attempt failure outcome=$outcome rc=$rc"
        ;;
    esac
  done
}

main "$@"
