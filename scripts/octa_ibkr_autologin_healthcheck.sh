#!/usr/bin/env bash
set -euo pipefail

TIMEOUT_SEC=180
PROGRESS_WINDOW_SEC=30

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout-sec)
      TIMEOUT_SEC="$2"
      shift 2
      ;;
    --progress-window-sec)
      PROGRESS_WINDOW_SEC="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 4
      ;;
  esac
done

if ! [[ "$TIMEOUT_SEC" =~ ^[0-9]+$ ]] || ! [[ "$PROGRESS_WINDOW_SEC" =~ ^[0-9]+$ ]]; then
  echo "FAIL[4]: timeout/progress values must be integers" >&2
  exit 4
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOT_PTR="$REPO_DIR/octa/var/runtime/systemd_boot_dir"

START_EPOCH="$(date +%s)"
BOOT_DIR=""
EVID=""
START_LINE=1

print_tail_debug() {
  echo "--- journalctl octa-autologin (last 60) ---"
  journalctl --user -u octa-autologin.service -n 60 --no-pager -l || true
  echo "--- evidence events (last 60) ---"
  if [[ -n "$EVID" ]] && [[ -f "$EVID" ]]; then
    tail -n 60 "$EVID" || true
  else
    echo "evidence file missing: ${EVID:-unset}" >&2
  fi
}

stop_services() {
  systemctl --user stop octa-autologin.service >/dev/null 2>&1 || true
  systemctl --user stop octa-ibkr.service >/dev/null 2>&1 || true
}

fail_with() {
  local code="$1"
  local reason="$2"
  echo "FAIL[$code]: $reason"
  stop_services
  print_tail_debug
  exit "$code"
}

scan_since_start() {
  if [[ -n "$EVID" ]] && [[ -f "$EVID" ]]; then
    tail -n "+$START_LINE" "$EVID"
  fi
}

echo "[healthcheck] daemon-reload"
systemctl --user daemon-reload

echo "[healthcheck] set monitor mode"
systemctl --user set-environment OCTA_AUTOLOGIN_MODE=monitor

echo "[healthcheck] restart services"
systemctl --user restart octa-ibkr.service
systemctl --user restart octa-autologin.service

if ! systemctl --user is-active --quiet octa-ibkr.service; then
  fail_with 4 "octa-ibkr.service not active"
fi
if ! systemctl --user is-active --quiet octa-autologin.service; then
  fail_with 4 "octa-autologin.service not active"
fi

EXECSTART_RAW="$(systemctl --user show octa-autologin.service -p ExecStart --value || true)"
ENV_MODE="$(systemctl --user show-environment | rg '^OCTA_AUTOLOGIN_MODE=' || true)"

if [[ "$EXECSTART_RAW" == *"--mode monitor"* ]]; then
  :
elif [[ "$EXECSTART_RAW" == *"--mode \${OCTA_AUTOLOGIN_MODE}"* ]] && [[ "$ENV_MODE" == "OCTA_AUTOLOGIN_MODE=monitor" ]]; then
  :
else
  fail_with 4 "autologin ExecStart/mode misconfigured (ExecStart='$EXECSTART_RAW', env='$ENV_MODE')"
fi

if [[ ! -f "$BOOT_PTR" ]]; then
  fail_with 4 "missing boot dir pointer: $BOOT_PTR"
fi
BOOT_DIR="$(cat "$BOOT_PTR")"
if [[ -z "$BOOT_DIR" ]]; then
  fail_with 4 "empty boot dir pointer"
fi
EVID="$BOOT_DIR/octa_ibkr_autologin_watch/events.jsonl"

if [[ -f "$EVID" ]]; then
  START_LINE="$(( $(wc -l < "$EVID") + 1 ))"
else
  START_LINE=1
fi

echo "[healthcheck] monitoring evidence: $EVID"

PROGRESS_SEEN=0
while true; do
  NOW_EPOCH="$(date +%s)"
  ELAPSED="$(( NOW_EPOCH - START_EPOCH ))"

  if (( ELAPSED > TIMEOUT_SEC )); then
    fail_with 4 "timeout_no_progress_after_${TIMEOUT_SEC}s"
  fi

  if ! systemctl --user is-active --quiet octa-ibkr.service; then
    fail_with 4 "octa-ibkr.service became inactive"
  fi
  if ! systemctl --user is-active --quiet octa-autologin.service; then
    fail_with 4 "octa-autologin.service became inactive"
  fi

  EVENTS="$(scan_since_start || true)"

  if printf '%s\n' "$EVENTS" | rg -q '"event_type":"autologin_error"|"event":"autologin_error"'; then
    fail_with 3 "autologin_error_detected"
  fi

  if printf '%s\n' "$EVENTS" | rg -q '"event_type":"stuck"|"event":"stuck"'; then
    fail_with 2 "stuck_detected"
  fi

  if printf '%s\n' "$EVENTS" | rg -q '"event_type":"state_change"|"event":"state_change"|"event_type":"action_performed"|"event":"action_performed"|"event_type":"login_flow_complete"|"event":"login_flow_complete"'; then
    PROGRESS_SEEN=1
    echo "OK[0]: healthy_progress_observed"
    exit 0
  fi

  if (( ELAPSED >= PROGRESS_WINDOW_SEC )) && (( PROGRESS_SEEN == 0 )); then
    fail_with 4 "no_progress_within_${PROGRESS_WINDOW_SEC}s"
  fi

  sleep 1
done
