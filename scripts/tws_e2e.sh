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
need awk
need sed
need tr
need grep

# ---- timing ----
POLL_SEC=0.5
WAIT_MAIN_MAX_SEC=120
GUARD_MAX_SEC=240
GUARD_QUIET_SEC=30

if [[ -z "${DISPLAY:-}" ]]; then
  echo "ERROR: DISPLAY is empty -> not in X11 GUI session." >&2
  exit 2
fi

# ---- load creds robustly (no printing secrets) ----
load_credentials() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: credentials file not found: $ENV_FILE" >&2
    exit 2
  fi

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

  # normalize aliases (some codepaths use variants)
  IBKR_USERNAME="${IBKR_USERNAME:-${IBKR_USER:-${TWS_USERNAME:-}}}"
  IBKR_PASSWORD="${IBKR_PASSWORD:-${IBKR_PASS:-${TWS_PASSWORD:-}}}"

  IBKR_USERNAME="$(printf "%s" "${IBKR_USERNAME:-}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  IBKR_PASSWORD="$(printf "%s" "${IBKR_PASSWORD:-}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"

  if [[ -z "${IBKR_USERNAME}" || -z "${IBKR_PASSWORD}" ]]; then
    echo "ERROR: username/password empty after loading $ENV_FILE" >&2
    exit 2
  fi

  export IBKR_USERNAME IBKR_PASSWORD
  export TWS_USERNAME="$IBKR_USERNAME" TWS_PASSWORD="$IBKR_PASSWORD"
  export IBKR_USER="$IBKR_USERNAME" IBKR_PASS="$IBKR_PASSWORD" IBKR_PW="$IBKR_PASSWORD"

  echo "Credentials: loaded (USER_LEN=${#IBKR_USERNAME} PASS_LEN=${#IBKR_PASSWORD})."
}

# ---- popups ----
POPUP_PAT='Warnhinweis|Risikohinweis|Disclaimer|Login Messages|IBKR Login Messenger|Message Center|Dow Jones|Top 10|Börsenspiegel|Boersenspiegel'

popups_present() { wmctrl -l | grep -Eiq "$POPUP_PAT"; }

list_popups() {
  wmctrl -l | grep -Ei "$POPUP_PAT" | awk '{
    id=$1; title="";
    for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
    print id "|" title;
  }' || true
}

present() { local id="$1"; wmctrl -l | awk '{print $1}' | grep -qi "^${id}$"; }

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
  xdotool mousemove --sync "$cx" "$cy" click 1 || true
}

key_to_win() {
  local id="$1"; shift
  xdotool windowactivate --sync "$id" 2>/dev/null || wmctrl -ia "$id" || true
  xdotool key --window "$id" --clearmodifiers "$@" 2>/dev/null || true
}

accept_warnhinweis() {
  local id="$1"
  for k in Return KP_Enter space; do
    key_to_win "$id" "$k"
    sleep 0.10
    present "$id" || return 0
  done
  # checkbox area + OK cluster
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
  key_to_win "$id" Escape;  sleep 0.10; present "$id" || return 0
  key_to_win "$id" Return;  sleep 0.10; present "$id" || return 0
  key_to_win "$id" KP_Enter; sleep 0.10; present "$id" || return 0
  key_to_win "$id" alt+F4;  sleep 0.12; present "$id" || return 0
  wmctrl -ic "$id" || true; sleep 0.12; present "$id" || return 0
  return 1
}

drain_once() {
  if ! popups_present; then return 0; fi
  local lines; lines="$(list_popups)"
  echo "Drain: popups detected:"
  echo "${lines:-"(matched but not listable)"}" | sed 's/^/  - /'
  [[ -z "${lines:-}" ]] && return 1

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

main_window_id() {
  wmctrl -l | awk '
    BEGIN{IGNORECASE=1}
    {
      id=$1; title="";
      for(i=4;i<=NF;i++){ title=title $i (i<NF?" ":""); }
      if (title ~ /(Interactive Brokers|Trader Workstation|IBKR|DUH[0-9]+)/) { print id; exit }
    }' || true
}

wait_for_main() {
  echo "Waiting for TWS main window (max ${WAIT_MAIN_MAX_SEC}s)..."
  local end=$(( $(date +%s) + WAIT_MAIN_MAX_SEC ))
  while [[ $(date +%s) -lt $end ]]; do
    local id; id="$(main_window_id)"
    if [[ -n "$id" ]]; then
      echo "Main window detected: $id"
      return 0
    fi
    sleep 1
  done
  echo "WARN: main window not detected."
  return 1
}

# ---- main ----
load_credentials

# Run chain with explicit env injection.
# Export both the primary names (OCTA_IBKR_*) that chain.py checks by default
# AND the alias names it falls back to, so the loader finds them immediately.
_chain_rc=0
env -u PYTHONPATH \
  OCTA_IBKR_USERNAME="$IBKR_USERNAME"  OCTA_IBKR_PASSWORD="$IBKR_PASSWORD" \
  IBKR_USERNAME="$IBKR_USERNAME"       IBKR_PASSWORD="$IBKR_PASSWORD" \
  TWS_USERNAME="$IBKR_USERNAME"        TWS_PASSWORD="$IBKR_PASSWORD"  \
  python "$CHAIN_PY" --config "$CFG_FILE" --timeout-sec 240 || _chain_rc=$?

if [[ $_chain_rc -eq 22 ]]; then
  echo "ERROR: autologin chain returned MISSING_CREDENTIALS (exit 22)." >&2
  echo "       Ensure $ENV_FILE contains IBKR_USERNAME and IBKR_PASSWORD" \
       "or set OCTA_IBKR_USERNAME / OCTA_IBKR_PASSWORD before running." >&2
  exit 2
fi
if [[ $_chain_rc -ne 0 ]]; then
  echo "WARN: autologin chain exited with code $_chain_rc (continuing to guard loop)." >&2
fi

wait_for_main || { echo "ERROR: TWS main window not detected within ${WAIT_MAIN_MAX_SEC}s." >&2; exit 1; }

echo "Guard: max ${GUARD_MAX_SEC}s, require quiet=${GUARD_QUIET_SEC}s (poll=${POLL_SEC}s)"
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
      echo "SUCCESS: stable quiet window reached (${quiet}s)."
      exit 0
    fi
  fi
  sleep "$POLL_SEC"
done

echo "FAIL: guard window expired; popups still appear."
wmctrl -l | grep -Ei "$POPUP_PAT" || true
exit 1
