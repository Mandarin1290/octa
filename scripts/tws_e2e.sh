#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${HOME}/.config/octa/ibkr.env"
CFG_FILE="${REPO_DIR}/configs/execution_ibkr.yaml"
CHAIN_PY="${REPO_DIR}/scripts/tws_x11_autologin_chain.py"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing dependency: $1" >&2; exit 2; }; }
need wmctrl
need xdotool
need python
need pgrep
need awk
need sed
need tr
need grep
need date
need tee
need mktemp

# ----------------------------
# Timings (tune here)
# ----------------------------
POLL_SEC=0.25

# Chain should normally return quickly; keep a cap but not 240s.
CHAIN_TIMEOUT_SEC="${CHAIN_TIMEOUT_SEC:-120}"

# Start draining early, before main window appears (helps when popups come first)
PRE_MAIN_DRAIN_SEC="${PRE_MAIN_DRAIN_SEC:-40}"

# Wait for main TWS window to show up
WAIT_MAIN_MAX_SEC="${WAIT_MAIN_MAX_SEC:-180}"

# During spawn phase, new popups appear over ~80s in your setup
BOOTSTRAP_OBSERVE_SEC="${BOOTSTRAP_OBSERVE_SEC:-90}"

# Fast-settle after bootstrap
FAST_TOTAL_SEC="${FAST_TOTAL_SEC:-25}"
FAST_CLEAN_STREAK="${FAST_CLEAN_STREAK:-4}"

# Guard: require quiet for N seconds
GUARD_MAX_SEC="${GUARD_MAX_SEC:-300}"
GUARD_QUIET_SEC="${GUARD_QUIET_SEC:-30}"

# Stop/close timings
STOP_MAX_SEC="${STOP_MAX_SEC:-45}"
STOP_TERM_WAIT_SEC="${STOP_TERM_WAIT_SEC:-10}"
STOP_KILL_WAIT_SEC="${STOP_KILL_WAIT_SEC:-2}"

# ----------------------------
# Popup patterns
#   - keep strict, do NOT include "Interactive Brokers|Trader Workstation" here
# ----------------------------
POPUP_PAT='Warnhinweis|Risikohinweis|Disclaimer|Login Messages|IBKR Login Messenger|Message Center|Dow Jones|Top 10|Börsenspiegel|Boersenspiegel'

# "Program is closing" dialogs are not something we should click aggressively during startup.
# But during shutdown we allow closing them.
CLOSING_PAT='Programm wird geschlossen|Program is closing'

if [[ -z "${DISPLAY:-}" ]]; then
  echo "ERROR: DISPLAY is empty -> not in X11 GUI session." >&2
  exit 2
fi

# Logging
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${REPO_DIR}/octa/var/logs"
mkdir -p "$LOG_DIR"
RUN_LOG="${LOG_DIR}/tws_e2e_${TS}.log"
echo "LOG: $RUN_LOG"

log() { echo "$*" | tee -a "$RUN_LOG"; }

# ----------------------------
# Credentials
# ----------------------------
load_env_file() {
  if [[ -f "$ENV_FILE" ]]; then
    local tmp
    tmp="$(mktemp)"
    tr -d '\r' < "$ENV_FILE" \
      | sed -E 's/^\s*export\s+//g' \
      | sed -E 's/^\s+//; s/\s+$//' \
      | grep -Ev '^\s*#|^\s*$' \
      > "$tmp"
    set -a
    # shellcheck disable=SC1090
    source "$tmp"
    set +a
    rm -f "$tmp"
  fi

  local u p
  u="${OCTA_IBKR_USERNAME:-${IBKR_USERNAME:-${TWS_USERNAME:-${IBKR_USER:-}}}}"
  p="${OCTA_IBKR_PASSWORD:-${IBKR_PASSWORD:-${TWS_PASSWORD:-${IBKR_PASS:-${IBKR_PW:-}}}}}"

  u="$(printf "%s" "${u:-}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  p="$(printf "%s" "${p:-}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"

  if [[ -z "$u" || -z "$p" ]]; then
    echo "ERROR: credentials missing (check $ENV_FILE)." >&2
    exit 2
  fi

  export OCTA_IBKR_USERNAME="$u"
  export OCTA_IBKR_PASSWORD="$p"
  export IBKR_USERNAME="$u"
  export IBKR_PASSWORD="$p"
  export TWS_USERNAME="$u"
  export TWS_PASSWORD="$p"
  export IBKR_USER="$u"
  export IBKR_PASS="$p"
  export IBKR_PW="$p"
  export OCTA_IBKR_ENV_FILE="${OCTA_IBKR_ENV_FILE:-$ENV_FILE}"

  log "Credentials: loaded (USER_LEN=${#u} PASS_LEN=${#p})."
}

# ----------------------------
# Window helpers
# ----------------------------
main_window_id() {
  wmctrl -l 2>/dev/null | awk '
    BEGIN{IGNORECASE=1}
    {
      id=$1; title="";
      for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
      if (title ~ /(Interactive Brokers|Trader Workstation|IBKR|DUH[0-9]+)/) { print id; exit }
    }' || true
}

wait_for_main() {
  log "Waiting for TWS main window (max ${WAIT_MAIN_MAX_SEC}s)..."
  local end=$(( $(date +%s) + WAIT_MAIN_MAX_SEC ))
  while [[ $(date +%s) -lt $end ]]; do
    local id
    id="$(main_window_id)"
    if [[ -n "$id" ]]; then
      log "Main window detected: $id"
      return 0
    fi
    sleep 1
  done
  log "FAIL: main window not detected."
  return 1
}

# ----------------------------
# Popup drain
# ----------------------------
popups_present() { wmctrl -l 2>/dev/null | grep -Eiq "$POPUP_PAT"; }

list_popups() {
  wmctrl -l 2>/dev/null | grep -Ei "$POPUP_PAT" | awk '{
    id=$1; title="";
    for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
    print id "|" title;
  }' || true
}

present() { local id="$1"; wmctrl -l 2>/dev/null | awk '{print $1}' | grep -qi "^${id}$"; }

get_geom() {
  local id="$1"
  xdotool getwindowgeometry --shell "$id" 2>/dev/null | awk -F= '
    $1=="X"{x=$2}
    $1=="Y"{y=$2}
    $1=="WIDTH"{w=$2}
    $1=="HEIGHT"{h=$2}
    END{ if(x==""||y==""||w==""||h=="") exit 1; print x, y, w, h }'
}

click_pct() {
  local id="$1" rx="$2" ry="$3"
  local x y w h cx cy
  read -r x y w h <<<"$(get_geom "$id")" || return 1
  cx=$(( x + (w*rx)/100 ))
  cy=$(( y + (h*ry)/100 ))
  xdotool windowactivate --sync "$id" 2>/dev/null || true
  xdotool mousemove --sync "$cx" "$cy" click 1 2>/dev/null || true
}

key_to_win() {
  local id="$1"; shift
  xdotool windowactivate --sync "$id" 2>/dev/null || wmctrl -ia "$id" 2>/dev/null || true
  xdotool key --window "$id" --clearmodifiers "$@" 2>/dev/null || true
}

accept_warnhinweis() {
  local id="$1"
  for k in Return KP_Enter space; do
    key_to_win "$id" "$k"
    sleep 0.10
    present "$id" || return 0
  done
  click_pct "$id" 12 90 || true
  sleep 0.10
  present "$id" || return 0
  for rx in 55 62 70 78 85 92; do
    click_pct "$id" "$rx" 92 || true
    sleep 0.12
    present "$id" || return 0
  done
  return 1
}

close_generic() {
  local id="$1"
  key_to_win "$id" Escape;   sleep 0.10; present "$id" || return 0
  key_to_win "$id" Return;   sleep 0.10; present "$id" || return 0
  key_to_win "$id" KP_Enter; sleep 0.10; present "$id" || return 0
  key_to_win "$id" alt+F4;   sleep 0.12; present "$id" || return 0
  wmctrl -ic "$id" >/dev/null 2>&1 || true
  sleep 0.12
  present "$id" || return 0
  return 1
}

drain_once() {
  if ! popups_present; then return 0; fi
  local lines
  lines="$(list_popups)"
  log "Drain: popups detected:"
  if [[ -n "${lines:-}" ]]; then
    echo "$lines" | sed 's/^/  - /' | tee -a "$RUN_LOG" >/dev/null
  else
    log "  - (matched, but not listable)"
    return 1
  fi

  while IFS='|' read -r id title; do
    [[ -z "${id:-}" ]] && continue
    if echo "$title" | grep -Eiq "Warnhinweis|Risikohinweis|Disclaimer"; then
      accept_warnhinweis "$id" || true
    else
      close_generic "$id" || true
    fi
  done <<< "$lines"
  return 1
}

# Only used during shutdown (safe to close "Program is closing" dialogs)
closing_present() { wmctrl -l 2>/dev/null | grep -Eiq "$CLOSING_PAT"; }
drain_closing_once() {
  wmctrl -l 2>/dev/null | grep -Ei "$CLOSING_PAT" | awk '{
    id=$1; title="";
    for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
    print id "|" title;
  }' | while IFS='|' read -r id title; do
    [[ -z "${id:-}" ]] && continue
    close_generic "$id" || true
  done || true
}

# ----------------------------
# STRICT PID handling
# ----------------------------
pid_cmdline() {
  local pid="$1"
  if [[ -r "/proc/$pid/cmdline" ]]; then
    tr '\0' ' ' < "/proc/$pid/cmdline" | sed 's/[[:space:]]\+/ /g'
  else
    echo ""
  fi
}

list_tws_pids_strict() {
  local self="$$" parent="$PPID"
  pgrep -f -i 'java' 2>/dev/null | while read -r pid; do
    [[ -z "${pid:-}" ]] && continue
    [[ "$pid" == "$self" || "$pid" == "$parent" ]] && continue
    local cmd
    cmd="$(pid_cmdline "$pid")"
    if echo "$cmd" | grep -Eiq 'java' \
      && echo "$cmd" | grep -Eiq 'install4j|jclient|ibgateway|tws'; then
      echo "$pid"
    fi
  done
}

tws_running_strict() {
  local pids
  pids="$(list_tws_pids_strict | tr -d ' \n')"
  [[ -n "${pids:-}" ]]
}

# ----------------------------
# Stop existing TWS
#   - MUST NEVER abort the script
# ----------------------------
stop_existing_tws() {
  set +e

  local start end
  start="$(date +%s)"
  end=$(( start + STOP_MAX_SEC ))

  local main_id
  main_id="$(main_window_id)"
  if [[ -n "$main_id" ]]; then
    log "Existing TWS window detected. Closing via Alt+F4..."
    key_to_win "$main_id" alt+F4
  fi

  # wait loop (best effort)
  while [[ $(date +%s) -lt $end ]]; do
    # during shutdown: close closing dialogs too
    closing_present && drain_closing_once || true
    popups_present && drain_once || true

    if [[ -z "$(main_window_id)" ]] && ! tws_running_strict; then
      log "TWS fully stopped."
      sleep "$STOP_KILL_WAIT_SEC"
      set -e
      return 0
    fi
    sleep "$POLL_SEC"
  done

  # TERM remaining strict PIDs
  local pids pid
  pids="$(list_tws_pids_strict | tr '\n' ' ')"
  if [[ -n "${pids// /}" ]]; then
    log "Shutdown timeout; TERM PIDs: $pids"
    for pid in $pids; do
      log "  TERM pid=$pid"
      kill -TERM "$pid" 2>/dev/null || true
    done
    sleep "$STOP_TERM_WAIT_SEC"
  fi

  # KILL remaining
  pids="$(list_tws_pids_strict | tr '\n' ' ')"
  if [[ -n "${pids// /}" ]]; then
    log "KILL remaining PIDs: $pids"
    for pid in $pids; do
      log "  KILL pid=$pid"
      kill -KILL "$pid" 2>/dev/null || true
    done
    sleep "$STOP_KILL_WAIT_SEC"
  fi

  if [[ -n "$(main_window_id)" ]] || tws_running_strict; then
    log "WARN: could not fully stop TWS (continuing anyway)."
    set -e
    return 0
  fi

  log "TWS fully stopped."
  set -e
  return 0
}

# ----------------------------
# MAIN
# ----------------------------
load_env_file
stop_existing_tws

log "[CHAIN] python $CHAIN_PY --config $CFG_FILE --timeout-sec $CHAIN_TIMEOUT_SEC"
set +e
python "$CHAIN_PY" --config "$CFG_FILE" --timeout-sec "$CHAIN_TIMEOUT_SEC" 2>&1 | tee -a "$RUN_LOG"
chain_rc=${PIPESTATUS[0]}
set -e
log "[CHAIN] exit_code=$chain_rc"

if [[ $chain_rc -eq 22 ]]; then
  log "FAIL: chain returned MISSING_CREDENTIALS (22)."
  exit 2
fi

# If chain said OK but nothing is running -> hard fail
if ! tws_running_strict; then
  log "FAIL: chain finished but no TWS/ibgateway java process is running."
  exit 1
fi

# Start draining immediately, even before main appears (your popups often show first)
log "Pre-main drain: ${PRE_MAIN_DRAIN_SEC}s (start detection early)..."
pre_end=$(( $(date +%s) + PRE_MAIN_DRAIN_SEC ))
while [[ $(date +%s) -lt $pre_end ]]; do
  popups_present && drain_once || true
  sleep "$POLL_SEC"
done
log "Pre-main drain: done."

if ! wait_for_main; then
  log "FAIL: process exists but main window not found."
  exit 1
fi

# During spawn, more windows appear later -> observe & drain
log "Bootstrap: observing+draining for ${BOOTSTRAP_OBSERVE_SEC}s (spawn phase)..."
boot_end=$(( $(date +%s) + BOOTSTRAP_OBSERVE_SEC ))
while [[ $(date +%s) -lt $boot_end ]]; do
  popups_present && drain_once || true
  sleep "$POLL_SEC"
done
log "Bootstrap: done."

log "Fast-settle: ${FAST_TOTAL_SEC}s, clean streak=${FAST_CLEAN_STREAK}"
fast_end=$(( $(date +%s) + FAST_TOTAL_SEC ))
streak=0
while [[ $(date +%s) -lt $fast_end ]]; do
  if drain_once; then
    streak=$((streak + 1))
    [[ $streak -ge $FAST_CLEAN_STREAK ]] && break
  else
    streak=0
  fi
  sleep "$POLL_SEC"
done

log "Guard: max ${GUARD_MAX_SEC}s, require quiet=${GUARD_QUIET_SEC}s (poll=${POLL_SEC}s)"
guard_end=$(( $(date +%s) + GUARD_MAX_SEC ))
last_dirty_ts=$(date +%s)

while [[ $(date +%s) -lt $guard_end ]]; do
  if popups_present; then
    drain_once || true
    last_dirty_ts=$(date +%s)
  else
    now=$(date +%s)
    quiet=$(( now - last_dirty_ts ))
    if [[ $quiet -ge $GUARD_QUIET_SEC ]]; then
      log "SUCCESS: stable quiet window reached (${quiet}s)."
      exit 0
    fi
  fi
  sleep "$POLL_SEC"
done

log "FAIL: guard expired; popups still appear."
wmctrl -l 2>/dev/null | grep -Ei "$POPUP_PAT" | tee -a "$RUN_LOG" || true
exit 1
