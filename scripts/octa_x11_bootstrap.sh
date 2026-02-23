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

REQUIRE_X11="${OCTA_REQUIRE_X11:-1}"
DISPLAY_SEL="${OCTA_XVFB_DISPLAY:-:99}"
if [[ -n "${OCTA_DISPLAY:-}" ]]; then
  DISPLAY_SEL="${OCTA_DISPLAY}"
elif [[ -n "${DISPLAY:-}" ]]; then
  DISPLAY_SEL="${DISPLAY}"
fi
export DISPLAY="${DISPLAY_SEL}"

if [[ "${DISPLAY}" =~ ^:([0-9]+)$ ]]; then
  DNUM="${BASH_REMATCH[1]}"
  SOCKET_PATH="/tmp/.X11-unix/X${DNUM}"
else
  DNUM=""
  SOCKET_PATH=""
fi

missing_binary=""
if ! command -v Xvfb >/dev/null 2>&1; then
  missing_binary="Xvfb"
fi

probe_ok=false
probe_method_used="none"
socket_path_exists=false
waited_ms=0
max_wait_ms=10000
step_ms=200
xdg_session_type="${XDG_SESSION_TYPE:-}"

while [[ ${waited_ms} -le ${max_wait_ms} ]]; do
  if command -v xdpyinfo >/dev/null 2>&1; then
    probe_method_used="xdpyinfo"
    if xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; then
      probe_ok=true
      break
    fi
  elif command -v xset >/dev/null 2>&1; then
    probe_method_used="xset"
    if xset -display "${DISPLAY}" -q >/dev/null 2>&1; then
      probe_ok=true
      break
    fi
  else
    probe_method_used="socket"
    if [[ -n "${SOCKET_PATH}" && -S "${SOCKET_PATH}" ]]; then
      probe_ok=true
      break
    fi
  fi

  if [[ -n "${SOCKET_PATH}" && -S "${SOCKET_PATH}" ]]; then
    socket_path_exists=true
  fi
  sleep 0.2
  waited_ms=$((waited_ms + step_ms))
done

if [[ -n "${SOCKET_PATH}" && -S "${SOCKET_PATH}" ]]; then
  socket_path_exists=true
fi

cat > "${BOOT_DIR}/x11_state.json" <<JSON
{
  "event_type": "x11_state",
  "ts": "${UTC_NOW}",
  "display": "${DISPLAY}",
  "probe_method_used": "${probe_method_used}",
  "probe_ok": ${probe_ok},
  "socket_path": "${SOCKET_PATH}",
  "socket_path_exists": ${socket_path_exists},
  "waited_ms": ${waited_ms},
  "xdg_session_type": "${xdg_session_type}",
  "missing_binary": "${missing_binary}"
}
JSON

printf '{"event_type":"x11_state","ts":"%s","display":"%s","probe_method_used":"%s","probe_ok":%s,"socket_path":"%s","socket_path_exists":%s,"waited_ms":%s,"xdg_session_type":"%s","missing_binary":"%s"}\n' \
  "${UTC_NOW}" "${DISPLAY}" "${probe_method_used}" "${probe_ok}" "${SOCKET_PATH}" "${socket_path_exists}" "${waited_ms}" "${xdg_session_type}" "${missing_binary}" >> "${BOOT_DIR}/events.jsonl"

if [[ "${REQUIRE_X11}" == "1" && "${probe_ok}" != "true" ]]; then
  reason="probe_failed"
  code="IBKR_X11_UNAVAILABLE"
  if [[ -n "${missing_binary}" ]]; then
    reason="missing_binary:${missing_binary}"
    code="IBKR_X11_UNAVAILABLE"
  elif [[ -z "${DISPLAY}" ]]; then
    reason="missing_display"
    code="IBKR_X11_REQUIRED"
  fi
  printf '{"event_type":"x11_error","ts":"%s","code":"%s","reason":"%s","action":"LOCK_EXECUTION_SHADOW_ONLY","display":"%s"}\n' \
    "${UTC_NOW}" "${code}" "${reason}" "${DISPLAY}" >> "${BOOT_DIR}/events.jsonl"
  exit 2
fi
