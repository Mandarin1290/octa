#!/usr/bin/env bash
set -euo pipefail

: "${OCTA_REPO:?OCTA_REPO is required}"

UTC_NOW="$(date -u +%Y%m%dT%H%M%SZ)"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-}"
if [[ -z "${BOOT_DIR}" ]] && [[ -f "${OCTA_REPO}/octa/var/runtime/systemd_boot_dir" ]]; then
  BOOT_DIR="$(cat "${OCTA_REPO}/octa/var/runtime/systemd_boot_dir" 2>/dev/null || true)"
fi
if [[ -z "${BOOT_DIR}" ]]; then
  BOOT_DIR="${OCTA_REPO}/octa/var/evidence/systemd_x11_state_${UTC_NOW}"
fi
mkdir -p "${BOOT_DIR}"

USE_XVFB="${OCTA_USE_XVFB:-0}"
REQUIRE_X11="${OCTA_REQUIRE_X11:-1}"
DISPLAY_SEL="${OCTA_DISPLAY:-${OCTA_XVFB_DISPLAY:-${DISPLAY:-:99}}}"
export DISPLAY="${DISPLAY_SEL}"
XDG_VAL="${XDG_SESSION_TYPE:-}"
X11_SERVICE_MODE="${OCTA_X11_SERVICE_MODE:-0}"

xvfb_started=false
xvfb_pid=""

if [[ "${USE_XVFB}" == "1" ]]; then
  if pgrep -f "Xvfb ${DISPLAY}" >/dev/null 2>&1; then
    xvfb_pid="$(pgrep -f "Xvfb ${DISPLAY}" | head -n1 || true)"
  elif [[ "${X11_SERVICE_MODE}" != "1" ]]; then
    Xvfb "${DISPLAY}" -screen 0 1920x1080x24 -nolisten tcp -dpi 96 >/dev/null 2>&1 &
    sleep 1
    xvfb_started=true
    xvfb_pid="$(pgrep -f "Xvfb ${DISPLAY}" | head -n1 || true)"
  fi
fi

x11_probe_method="display_only"
x11_probe_ok=false
if [[ "${X11_SERVICE_MODE}" == "1" && "${USE_XVFB}" == "1" ]]; then
  x11_probe_method="deferred_xvfb_service_start"
  x11_probe_ok=true
elif command -v xdpyinfo >/dev/null 2>&1; then
  x11_probe_method="xdpyinfo"
  if xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; then
    x11_probe_ok=true
  fi
elif command -v xset >/dev/null 2>&1; then
  x11_probe_method="xset"
  if xset -display "${DISPLAY}" -q >/dev/null 2>&1; then
    x11_probe_ok=true
  fi
elif [[ -n "${DISPLAY}" ]]; then
  x11_probe_method="display_only"
  x11_probe_ok=true
fi

cat > "${BOOT_DIR}/x11_state.json" <<JSON
{
  "event_type": "x11_state",
  "ts": "${UTC_NOW}",
  "display": "${DISPLAY}",
  "octa_use_xvfb": ${USE_XVFB},
  "xdg_session_type": "${XDG_VAL}",
  "xdpyinfo_ok": ${x11_probe_ok},
  "x11_probe_method": "${x11_probe_method}",
  "xvfb_started": ${xvfb_started},
  "pid": "${xvfb_pid}"
}
JSON

printf '{"event_type":"x11_state","ts":"%s","display":"%s","octa_use_xvfb":%s,"xdg_session_type":"%s","x11_probe_method":"%s","x11_probe_ok":%s,"xvfb_started":%s,"pid":"%s"}\n' \
  "${UTC_NOW}" "${DISPLAY}" "${USE_XVFB}" "${XDG_VAL}" "${x11_probe_method}" "${x11_probe_ok}" "${xvfb_started}" "${xvfb_pid}" >> "${BOOT_DIR}/events.jsonl"

if [[ "${REQUIRE_X11}" == "1" && "${x11_probe_ok}" != "true" ]]; then
  printf '{"event_type":"x11_error","ts":"%s","code":"IBKR_X11_UNAVAILABLE","action":"LOCK_EXECUTION_SHADOW_ONLY","display":"%s","x11_probe_method":"%s"}\n' \
    "${UTC_NOW}" "${DISPLAY}" "${x11_probe_method}" >> "${BOOT_DIR}/events.jsonl"
  exit 2
fi
