#!/usr/bin/env bash
# scripts/tws_e2e.sh
# Goal: Start TWS exactly once, autologin, aggressively close popups fast,
# and exit SUCCESS as soon as (a) popups are gone AND (b) main window is present
# (optional: API port listening test).
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

# Optional port test dependency:
command -v ss >/dev/null 2>&1 || command -v netstat >/dev/null 2>&1 || true

# ----------------------------
# Timings (FAST defaults)
# ----------------------------
POLL_SEC="${POLL_SEC:-0.20}"

CHAIN_TIMEOUT_SEC="${CHAIN_TIMEOUT_SEC:-120}"

# FAST path after chain_rc=25
FAST_POPUP_BURST_SEC="${FAST_POPUP_BURST_SEC:-15}"
FAST_MAIN_WAIT_SEC="${FAST_MAIN_WAIT_SEC:-25}"

# Normal path (still faster than before)
PRE_MAIN_DRAIN_SEC="${PRE_MAIN_DRAIN_SEC:-10}"
WAIT_MAIN_MAX_SEC="${WAIT_MAIN_MAX_SEC:-60}"
BOOTSTRAP_OBSERVE_SEC="${BOOTSTRAP_OBSERVE_SEC:-30}"

FAST_TOTAL_SEC="${FAST_TOTAL_SEC:-12}"
FAST_CLEAN_STREAK="${FAST_CLEAN_STREAK:-3}"

GUARD_MAX_SEC="${GUARD_MAX_SEC:-120}"
GUARD_QUIET_SEC="${GUARD_QUIET_SEC:-8}"

# Stop/close timings
STOP_MAX_SEC="${STOP_MAX_SEC:-35}"
STOP_TERM_WAIT_SEC="${STOP_TERM_WAIT_SEC:-8}"
STOP_KILL_WAIT_SEC="${STOP_KILL_WAIT_SEC:-2}"

# ----------------------------
# Popup patterns (strict but complete)
# ----------------------------
POPUP_PAT='Warnung|Warnhinweis|Risikohinweis|Disclaimer|Login Messages|Login Message|IBKR Login|install4j|jclient|Message Center|Dow Jones|Top 10|Börsenspiegel|Boersenspiegel|Hinweis|Agreement|Notice|Information|Haftung|Risk Disclosure'

# Closing dialogs only during shutdown
CLOSING_PAT='Programm wird geschlossen|Program is closing'

# News/info popups that do not block the API and cannot always be closed via keyboard/wmctrl
NEWS_ONLY_PAT='Dow Jones|Heutige Top 10|Top 10 Today|Börsenspiegel|Boersenspiegel'

# True if there are FATAL popups (ones that block API or must be dismissed).
# News-only popups are excluded — they don't prevent API connections.
fatal_popups_present() {
  popups_present || return 1
  wmctrl -l 2>/dev/null | grep -Ei "$POPUP_PAT" | grep -vEi "$NEWS_ONLY_PAT" | grep -q .
}

# ----------------------------
# GUI session detect + auto-fix (Wayland+Xwayland friendly)
# ----------------------------
if [[ -z "${DISPLAY:-}" ]]; then
  export DISPLAY=":0"
fi

if [[ -z "${XAUTHORITY:-}" || ! -f "${XAUTHORITY:-}" ]]; then
  AUTH="$(ps -u "$USER" -o cmd= | sed -n 's/.*Xwayland :0 .* -auth \([^ ]*\).*/\1/p' | head -n1 || true)"
  if [[ -z "${AUTH:-}" || ! -f "$AUTH" ]]; then
    for cand in "${XDG_RUNTIME_DIR:-}/Xauthority" "$HOME/.Xauthority"; do
      [[ -f "$cand" ]] && { AUTH="$cand"; break; }
    done
  fi
  if [[ -n "${AUTH:-}" && -f "$AUTH" ]]; then
    export XAUTHORITY="$AUTH"
  fi
fi

# If still no access, fail clearly
if ! wmctrl -l >/dev/null 2>&1; then
  echo "ERROR: cannot access X session. DISPLAY=$DISPLAY XAUTHORITY=${XAUTHORITY:-<unset>}" >&2
  echo "Hint: run from desktop session or export correct DISPLAY/XAUTHORITY." >&2
  exit 2
fi

# ----------------------------
# Logging
# ----------------------------
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${REPO_DIR}/octa/var/logs"
mkdir -p "$LOG_DIR"
RUN_LOG="${LOG_DIR}/tws_e2e_${TS}.log"
echo "LOG: $RUN_LOG"

log() { echo "[$(date -Is)] $*" | tee -a "$RUN_LOG"; }

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
      if (title ~ /(Interactive Brokers|Trader Workstation|IBKR|TWS)/) { print id; exit }
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
    sleep 0.5
  done
  log "FAIL: main window not detected."
  return 1
}

# ----------------------------
# Popup drain
# ----------------------------
popups_present() { wmctrl -l 2>/dev/null | grep -Eiq "$POPUP_PAT"; }

list_popups() {
  local mid
  mid="$(main_window_id)"
  wmctrl -l 2>/dev/null | grep -Ei "$POPUP_PAT" | awk -v MID="$mid" '{
    id=$1;
    if (MID != "" && tolower(id)==tolower(MID)) next;
    title="";
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
    sleep 0.08
    present "$id" || return 0
  done
  click_pct "$id" 12 90 || true
  sleep 0.08
  present "$id" || return 0
  for rx in 55 62 70 78 85 92; do
    click_pct "$id" "$rx" 92 || true
    sleep 0.10
    present "$id" || return 0
  done
  return 1
}

close_generic() {
  local id="$1"
  key_to_win "$id" Escape;   sleep 0.08; present "$id" || return 0
  key_to_win "$id" Return;   sleep 0.08; present "$id" || return 0
  key_to_win "$id" KP_Enter; sleep 0.08; present "$id" || return 0
  key_to_win "$id" alt+F4;   sleep 0.10; present "$id" || return 0
  wmctrl -ic "$id" >/dev/null 2>&1 || true
  sleep 0.10
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
    log "  - (matched, but not listable; retrying)"
    return 1
  fi

  while IFS='|' read -r id title; do
    [[ -z "${id:-}" ]] && continue
    if echo "$title" | grep -Eiq "Warnung|Warnhinweis|Risikohinweis|Disclaimer|Agreement|Haftung|Risk"; then
      accept_warnhinweis "$id" || true
    else
      close_generic "$id" || true
    fi
  done <<< "$lines"
  return 1
}

# Only used during shutdown
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
# STRICT PID handling (avoid killing wrong java)
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
# Stop existing TWS (never abort)
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

  while [[ $(date +%s) -lt $end ]]; do
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

  local pids pid
  pids="$(list_tws_pids_strict | tr '\n' ' ')"
  if [[ -n "${pids// /}" ]]; then
    log "Shutdown timeout; TERM PIDs: $pids"
    for pid in $pids; do kill -TERM "$pid" 2>/dev/null || true; done
    sleep "$STOP_TERM_WAIT_SEC"
  fi

  pids="$(list_tws_pids_strict | tr '\n' ' ')"
  if [[ -n "${pids// /}" ]]; then
    log "KILL remaining PIDs: $pids"
    for pid in $pids; do kill -KILL "$pid" 2>/dev/null || true; done
    sleep "$STOP_KILL_WAIT_SEC"
  fi

  log "TWS stop: best-effort done."
  set -e
  return 0
}

# ----------------------------
# Optional: API port test
# ----------------------------
API_PORT_TEST="${API_PORT_TEST:-0}"   # set 1 to require a listening port
API_PORT_PRIMARY="${API_PORT_PRIMARY:-7497}"
API_PORT_FALLBACK="${API_PORT_FALLBACK:-7496}"
api_port_listening() {
  local p="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq ":${p}$"
  elif command -v netstat >/dev/null 2>&1; then
    netstat -ltn 2>/dev/null | awk '{print $4}' | grep -Eq ":${p}$"
  else
    return 2
  fi
}
wait_api_port() {
  local max="${1:-40}"
  local end=$(( $(date +%s) + max ))
  while [[ $(date +%s) -lt $end ]]; do
    api_port_listening "$API_PORT_PRIMARY" && { log "API port listening: $API_PORT_PRIMARY"; return 0; }
    api_port_listening "$API_PORT_FALLBACK" && { log "API port listening: $API_PORT_FALLBACK"; return 0; }
    sleep 1
  done
  log "FAIL: API port not listening after ${max}s."
  return 1
}
# ----------------------------
# Handshake (after Bootstrap)
# Return codes:
#   0  = handshake OK
#   41 = blocked by Paper Trading Disclaimer (10141)
#   1  = other failure
# ----------------------------
api_handshake_try() {
  python - <<'PY'
import sys, random
from ib_insync import IB

HOST="127.0.0.1"
PORTS=[7497, 7496]
client_id = random.randint(2000, 65000)

for port in PORTS:
    ib = IB()
    try:
        ib.connect(HOST, port, clientId=client_id, timeout=4)
        if ib.isConnected():
            t = ib.reqCurrentTime()
            acc = ib.managedAccounts()
            ib.disconnect()
            if t and acc:
                print(f"OK port={port} clientId={client_id}")
                sys.exit(0)
        try:
            ib.disconnect()
        except Exception:
            pass
    except Exception:
        try:
            ib.disconnect()
        except Exception:
            pass

sys.exit(1)
PY
}

api_handshake() {
  # quick retries inside READY window
  # Block only on FATAL popups (disclaimers etc.); news popups don't prevent API connections.
  local end=$(( $(date +%s) + 20 ))
  while [[ $(date +%s) -lt $end ]]; do
    # if a fatal popup (disclaimer/warning) is still there, drain and wait
    if fatal_popups_present; then
      drain_once || true
      sleep 0.25
      continue
    fi
    # non-fatal (news) popups may still be present; drain best-effort but proceed
    popups_present && drain_once || true

    # Try handshake, capture output for 10141 detection
    local tmp rc
    tmp="$(mktemp)"
    set +e
    api_handshake_try 2>&1 | tee "$tmp" | tee -a "$RUN_LOG" >/dev/null
    rc=${PIPESTATUS[0]}
    set -e

    if grep -q "Error 10141" "$tmp"; then
      rm -f "$tmp"
      return 41
    fi

    rm -f "$tmp"
    [[ $rc -eq 0 ]] && return 0

    sleep 0.5
  done
  return 1
}

# ----------------------------
# FAST PATH after chain_rc=25
# ----------------------------
fast_path_popup_then_main() {
  log "FAST-PATH: burst drain ${FAST_POPUP_BURST_SEC}s..."
  local burst_end=$(( $(date +%s) + FAST_POPUP_BURST_SEC ))
  while [[ $(date +%s) -lt $burst_end ]]; do
    popups_present && drain_once || true
    sleep "$POLL_SEC"
  done

  log "FAST-PATH: quick main wait ${FAST_MAIN_WAIT_SEC}s..."
  local quick_end=$(( $(date +%s) + FAST_MAIN_WAIT_SEC ))
  while [[ $(date +%s) -lt $quick_end ]]; do
    popups_present && drain_once || true
    if ! fatal_popups_present; then
      local mid
      mid="$(main_window_id)"
      if [[ -n "$mid" ]]; then
        log "FAST-PATH: no fatal popups + main window present ($mid)."
        if [[ "$API_PORT_TEST" == "1" ]]; then
          wait_api_port 40 || return 1
        fi
        log "SUCCESS: TWS ready (FAST-PATH)."
        exit 0
      fi
    fi
    sleep "$POLL_SEC"
  done

  log "FAST-PATH: not ready yet -> continue normal path."
  return 0
}

# ----------------------------
# MAIN
# ----------------------------
load_env_file

# Short-circuit: if TWS is already running and API port is open, skip login entirely.
if tws_running_strict && { api_port_listening "$API_PORT_PRIMARY" || api_port_listening "$API_PORT_FALLBACK"; }; then
  log "TWS already running and API port open. Skipping login — proceeding to handshake."
  if api_handshake; then
    log "SUCCESS: handshake OK (reuse existing session)."
    exit 0
  else
    log "Existing session handshake failed. Proceeding with full restart."
  fi
fi

# Disable AutoRestart BEFORE killing so TWS does not re-launch itself (would race with chain login)
if [[ -f "${HOME}/Jts/jts.ini" ]]; then
  sed -i 's/^AutoRestart=1$/AutoRestart=0/' "${HOME}/Jts/jts.ini" 2>/dev/null || true
  # Restore AutoRestart on any exit (success or failure)
  trap 'sed -i "s/^AutoRestart=0\$/AutoRestart=1/" "${HOME}/Jts/jts.ini" 2>/dev/null || true' EXIT
fi

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

if ! tws_running_strict; then
  log "FAIL: chain finished but no TWS/ibgateway java process is running."
  exit 1
fi

if [[ $chain_rc -eq 25 ]]; then
  fast_path_popup_then_main || true
fi

log "Pre-main drain: ${PRE_MAIN_DRAIN_SEC}s..."
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

log "Bootstrap: observing+draining for ${BOOTSTRAP_OBSERVE_SEC}s..."
boot_end=$(( $(date +%s) + BOOTSTRAP_OBSERVE_SEC ))
while [[ $(date +%s) -lt $boot_end ]]; do
  popups_present && drain_once || true
  sleep "$POLL_SEC"
done
log "Bootstrap: done."

# ---- HANDSHAKE HERE (requested) ----
if [[ "$API_PORT_TEST" == "1" ]]; then
  wait_api_port 40 || exit 1
fi

log "Handshake: trying real IBKR API connection..."
if api_handshake; then
  log "SUCCESS: handshake OK."
else
  rc=$?
  if [[ $rc -eq 41 ]]; then
    log "Handshake blocked by Paper Trading Disclaimer (10141). Draining/accepting and retrying..."
    # try a few short drain rounds, then retry once
    for _ in 1 2 3 4 5; do
      popups_present && drain_once || true
      sleep 0.4
    done
    if api_handshake; then
      log "SUCCESS: handshake OK after disclaimer."
    else
      log "FAIL: handshake still blocked/failed."
      exit 1
    fi
  else
    log "FAIL: handshake failed."
    exit 1
  fi
fi

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

log "Guard: max ${GUARD_MAX_SEC}s, require quiet=${GUARD_QUIET_SEC}s"
guard_end=$(( $(date +%s) + GUARD_MAX_SEC ))
last_dirty_ts=$(date +%s)

while [[ $(date +%s) -lt $guard_end ]]; do
  popups_present && drain_once || true
  if fatal_popups_present; then
    last_dirty_ts=$(date +%s)
  else
    now=$(date +%s)
    quiet=$(( now - last_dirty_ts ))
    if [[ $quiet -ge $GUARD_QUIET_SEC ]]; then
      if [[ "$API_PORT_TEST" == "1" ]]; then
        wait_api_port 40
      fi
      log "SUCCESS: stable quiet window reached (${quiet}s)."
      exit 0
    fi
  fi
  sleep "$POLL_SEC"
done

log "FAIL: guard expired; fatal popups still appear."
wmctrl -l 2>/dev/null | grep -Ei "$POPUP_PAT" | tee -a "$RUN_LOG" || true
exit 1