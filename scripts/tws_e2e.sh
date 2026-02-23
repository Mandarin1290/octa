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

if [[ -z "${DISPLAY:-}" ]]; then
  echo "ERROR: DISPLAY is empty -> not in X11 GUI session." >&2
  exit 2
fi

# ----------------------------
# Timings
# ----------------------------
POLL_SEC=0.25
WAIT_MAIN_MAX_SEC=180
PRE_MAIN_DRAIN_SEC=40
BOOTSTRAP_SEC=90
FAST_TOTAL_SEC=25
FAST_CLEAN_STREAK=4
GUARD_MAX_SEC=300
GUARD_QUIET_SEC=30

ACT_SLEEP=0.03
CHAIN_TIMEOUT_SEC=240

# Popups we watch for
POPUP_PAT='Warnhinweis|Risikohinweis|Disclaimer|Login Messages|IBKR Login Messenger|Message Center|Dow Jones|Top 10|Börsenspiegel|Boersenspiegel|Programm wird geschlossen|Program is closing'

# MAIN window pattern (NEVER send Alt+F4 to this)
MAIN_PAT='Interactive Brokers|Trader Workstation|IBKR|DUH[0-9]+'

# Logging
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${REPO_DIR}/octa/var/logs"
mkdir -p "$LOG_DIR"
RUN_LOG="${LOG_DIR}/tws_e2e_${TS}.log"
echo "LOG: $RUN_LOG"

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

  echo "Credentials: loaded (USER_LEN=${#u} PASS_LEN=${#p})."
}

# ----------------------------
# Window helpers
# ----------------------------
main_window_id() {
  wmctrl -l | awk '
    BEGIN{IGNORECASE=1}
    {
      id=$1; title="";
      for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
      if (title ~ /(Interactive Brokers|Trader Workstation|IBKR|DUH[0-9]+)/) { print id; exit }
    }' || true
}

is_main_title() {
  local title="$1"
  echo "$title" | grep -Eiq "$MAIN_PAT"
}

# ----------------------------
# Popup drain
# ----------------------------
popups_present() { wmctrl -l | grep -Eiq "$POPUP_PAT"; }

list_popups() {
  wmctrl -l | grep -Ei "$POPUP_PAT" | awk '{
    id=$1; title="";
    for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
    print id "|" title;
  }' || true
}

present() { local id="$1"; wmctrl -l | awk '{print $1}' | grep -qi "^${id}$"; }

activate_fast() {
  local id="$1"
  xdotool windowactivate "$id" 2>/dev/null || wmctrl -ia "$id" >/dev/null 2>&1 || true
  sleep "$ACT_SLEEP"
}

key_to_win() {
  local id="$1"; shift
  activate_fast "$id"
  xdotool key --window "$id" --clearmodifiers "$@" 2>/dev/null || true
  sleep "$ACT_SLEEP"
}

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
  activate_fast "$id"
  xdotool mousemove "$cx" "$cy" click 1 2>/dev/null || true
  sleep "$ACT_SLEEP"
}

# IMPORTANT: NEVER confirm/close "Program is closing" dialogs (could accelerate shutdown)
is_closing_dialog() {
  local title="$1"
  echo "$title" | grep -Eiq 'Programm wird geschlossen|Program is closing'
}

accept_warnlike() {
  local id="$1"
  # Try keys that usually mean "Accept/OK" for warn/disclaimer.
  for k in Return KP_Enter space; do
    key_to_win "$id" "$k"
    sleep 0.10
    present "$id" || return 0
  done
  # checkbox-ish then accept cluster
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

# Safer generic close: prefer wmctrl -ic, avoid Return, avoid Alt+F4 unless necessary.
close_window_safely() {
  local id="$1" title="$2"

  # Never attempt to close MAIN window from popup drain.
  if is_main_title "$title"; then
    return 0
  fi

  # Never interact with "closing" dialog (avoid confirming shutdown).
  if is_closing_dialog "$title"; then
    return 0
  fi

  # Prefer direct close.
  wmctrl -ic "$id" >/dev/null 2>&1 || true
  sleep 0.12
  present "$id" || return 0

  # Fallback: Escape only (doesn't usually confirm destructive actions)
  key_to_win "$id" Escape
  sleep 0.12
  present "$id" || return 0

  # Last resort: Alt+F4 (still blocked for MAIN by title check above)
  key_to_win "$id" alt+F4
  sleep 0.12
  present "$id" || return 0

  return 1
}

drain_once() {
  if ! popups_present; then return 0; fi

  local lines
  lines="$(list_popups)"
  echo "Drain: popups detected:"
  echo "${lines:-"(matched, but not listable)"}" | sed 's/^/  - /'
  [[ -z "${lines:-}" ]] && return 1

  while IFS='|' read -r id title; do
    [[ -z "${id:-}" ]] && continue

    # Guard: ignore closing dialogs to avoid killing TWS
    if is_closing_dialog "$title"; then
      continue
    fi

    if echo "$title" | grep -Eiq 'Warnhinweis|Risikohinweis|Disclaimer'; then
      accept_warnlike "$id" || true
      continue
    fi

    # For informational popups, close safely (wmctrl -ic first)
    close_window_safely "$id" "$title" || true
  done <<< "$lines"

  popups_present && return 1 || return 0
}

burst_drain() {
  local passes="${1:-10}"
  local i
  for i in $(seq 1 "$passes"); do
    drain_once && return 0
  done
  return 1
}

# ----------------------------
# STRICT PID handling (unchanged)
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

stop_existing_tws() {
  local main_id
  main_id="$(main_window_id)"
  if [[ -n "$main_id" ]]; then
    echo "Existing TWS window detected. Closing via Alt+F4..."
    key_to_win "$main_id" alt+F4
  fi

  local end=$(( $(date +%s) + 45 ))
  while [[ $(date +%s) -lt $end ]]; do
    popups_present && burst_drain 10 || true
    if [[ -z "$(main_window_id)" ]] && ! tws_running_strict; then
      echo "TWS fully stopped."
      sleep 1
      return 0
    fi
    sleep "$POLL_SEC"
  done

  local pids
  pids="$(list_tws_pids_strict | tr '\n' ' ')"
  if [[ -n "${pids// /}" ]]; then
    echo "Shutdown timeout; TERM PIDs: $pids"
    for pid in $pids; do
      echo "  TERM pid=$pid cmd=$(pid_cmdline "$pid")"
      kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 10
  fi

  pids="$(list_tws_pids_strict | tr '\n' ' ')"
  if [[ -n "${pids// /}" ]]; then
    echo "KILL remaining PIDs: $pids"
    for pid in $pids; do
      echo "  KILL pid=$pid cmd=$(pid_cmdline "$pid")"
      kill -KILL "$pid" 2>/dev/null || true
    done
    sleep 2
  fi

  if [[ -n "$(main_window_id)" ]] || tws_running_strict; then
    echo "WARN: could not fully stop TWS (continuing anyway)."
    return 0
  fi
  echo "TWS fully stopped."
}

wait_for_main_with_drain() {
  echo "Waiting for TWS main window (max ${WAIT_MAIN_MAX_SEC}s)..."
  local end=$(( $(date +%s) + WAIT_MAIN_MAX_SEC ))
  while [[ $(date +%s) -lt $end ]]; do
    popups_present && burst_drain 10 || true

    local id
    id="$(main_window_id)"
    if [[ -n "$id" ]]; then
      echo "Main window detected: $id"
      return 0
    fi
    sleep 1
  done
  echo "FAIL: main window not detected."
  return 1
}

# ----------------------------
# MAIN
# ----------------------------
load_env_file
stop_existing_tws

echo "[CHAIN] python $CHAIN_PY --config $CFG_FILE --timeout-sec ${CHAIN_TIMEOUT_SEC}" | tee -a "$RUN_LOG"
set +e
python "$CHAIN_PY" --config "$CFG_FILE" --timeout-sec "${CHAIN_TIMEOUT_SEC}" 2>&1 | tee -a "$RUN_LOG"
chain_rc=${PIPESTATUS[0]}
set -e
echo "[CHAIN] exit_code=$chain_rc" | tee -a "$RUN_LOG"

if [[ $chain_rc -eq 22 ]]; then
  echo "FAIL: chain returned MISSING_CREDENTIALS (22)." | tee -a "$RUN_LOG"
  exit 2
fi

if ! tws_running_strict; then
  echo "FAIL: chain finished but no TWS/ibgateway java process is running." | tee -a "$RUN_LOG"
  exit 1
fi

echo "Pre-main drain: ${PRE_MAIN_DRAIN_SEC}s (start detection early)..." | tee -a "$RUN_LOG"
pre_end=$(( $(date +%s) + PRE_MAIN_DRAIN_SEC ))
while [[ $(date +%s) -lt $pre_end ]]; do
  popups_present && burst_drain 10 || true
  sleep "$POLL_SEC"
done
echo "Pre-main drain: done." | tee -a "$RUN_LOG"

wait_for_main_with_drain | tee -a "$RUN_LOG"

echo "Bootstrap: observing+draining for ${BOOTSTRAP_SEC}s (spawn phase)..." | tee -a "$RUN_LOG"
boot_end=$(( $(date +%s) + BOOTSTRAP_SEC ))
while [[ $(date +%s) -lt $boot_end ]]; do
  popups_present && burst_drain 10 || true
  sleep "$POLL_SEC"
done
echo "Bootstrap: done." | tee -a "$RUN_LOG"

echo "Fast-settle: ${FAST_TOTAL_SEC}s, clean streak=${FAST_CLEAN_STREAK}" | tee -a "$RUN_LOG"
fast_end=$(( $(date +%s) + FAST_TOTAL_SEC ))
streak=0
while [[ $(date +%s) -lt $fast_end ]]; do
  if burst_drain 8; then
    streak=$((streak + 1))
    [[ $streak -ge $FAST_CLEAN_STREAK ]] && break
  else
    streak=0
  fi
  sleep "$POLL_SEC"
done

echo "Guard: max ${GUARD_MAX_SEC}s, require quiet=${GUARD_QUIET_SEC}s (poll=${POLL_SEC}s)" | tee -a "$RUN_LOG"
guard_end=$(( $(date +%s) + GUARD_MAX_SEC ))
last_dirty_ts=$(date +%s)

while [[ $(date +%s) -lt $guard_end ]]; do
  if popups_present; then
    burst_drain 10 || true
    last_dirty_ts=$(date +%s)
  else
    now=$(date +%s)
    quiet=$(( now - last_dirty_ts ))
    if [[ $quiet -ge $GUARD_QUIET_SEC ]]; then
      echo "SUCCESS: stable quiet window reached (${quiet}s)." | tee -a "$RUN_LOG"
      exit 0
    fi
  fi
  sleep "$POLL_SEC"
done

echo "FAIL: guard expired; popups still appear." | tee -a "$RUN_LOG"
wmctrl -l | grep -Ei "$POPUP_PAT" | tee -a "$RUN_LOG" || true
exit 1
