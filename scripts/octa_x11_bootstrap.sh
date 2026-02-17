#!/usr/bin/env bash
set -euo pipefail

: "${OCTA_REPO:?OCTA_REPO is required}"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-${OCTA_REPO}/octa/var/evidence/systemd_boot_$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${BOOT_DIR}"

USE_XVFB="${OCTA_USE_XVFB:-0}"
REQUIRE_X11="${OCTA_REQUIRE_X11:-1}"
DISPLAY_VAL="${DISPLAY:-}"

if [[ "${USE_XVFB}" == "1" ]]; then
  export DISPLAY="${DISPLAY_VAL:-:99}"
  if ! pgrep -f "Xvfb ${DISPLAY}" >/dev/null 2>&1; then
    Xvfb "${DISPLAY}" -screen 0 1920x1080x24 >/dev/null 2>&1 &
    sleep 1
  fi
fi

if [[ -z "${DISPLAY:-}" && "${REQUIRE_X11}" == "1" ]]; then
  printf '{"event_type":"x11_error","code":"IBKR_X11_UNAVAILABLE","action":"LOCK_EXECUTION_SHADOW_ONLY"}\n' >> "${BOOT_DIR}/events.jsonl"
  exit 2
fi

XDG_VAL="${XDG_SESSION_TYPE:-}"
if [[ "${USE_XVFB}" != "1" && "${REQUIRE_X11}" == "1" && "${XDG_VAL}" != "x11" ]]; then
  printf '{"event_type":"x11_error","code":"IBKR_X11_REQUIRED","action":"LOCK_EXECUTION_SHADOW_ONLY"}\n' >> "${BOOT_DIR}/events.jsonl"
  exit 2
fi

cat > "${BOOT_DIR}/x11_state.json" <<JSON
{"event_type":"x11_ok","display":"${DISPLAY:-}","xdg_session_type":"${XDG_VAL}"}
JSON
printf '{"event_type":"x11_ok","display":"%s","xdg_session_type":"%s"}\n' "${DISPLAY:-}" "${XDG_VAL}" >> "${BOOT_DIR}/events.jsonl"
