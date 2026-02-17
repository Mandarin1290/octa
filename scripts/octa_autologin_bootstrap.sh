#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# OCTA IBKR autologin bootstrap — login step-machine + popup watcher
# ──────────────────────────────────────────────────────────────────────────────

# ── Step 0: Python interpreter — absolute venv path, never bare "python" ─────
REPO="/home/n-b/Octa"
PY="${OCTA_PY:-${REPO}/.venv/bin/python}"

if [[ ! -x "${PY}" ]]; then
  _err="{\"event_type\":\"autologin_error\",\"reason\":\"python_not_executable\",\"value\":\"${PY}\"}"
  echo "${_err}" >&2
  mkdir -p "${REPO}/octa/var/evidence" 2>/dev/null || true
  printf '%s\n' "${_err}" >> "${REPO}/octa/var/evidence/events_preflight.jsonl" 2>/dev/null || true
  exit 1
fi

echo "Using Python: ${PY}"
"${PY}" -c "import sys; print(sys.executable)"

: "${OCTA_REPO:=${REPO}}"
export OCTA_REPO

DB_PATH="${OCTA_IBKR_DB:-octa/var/runtime/ibkr_autologin.sqlite3}"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-}"
if [[ -z "${BOOT_DIR}" ]] && [[ -f "${OCTA_REPO}/octa/var/runtime/systemd_boot_dir" ]]; then
  BOOT_DIR="$(cat "${OCTA_REPO}/octa/var/runtime/systemd_boot_dir" 2>/dev/null || true)"
fi
if [[ -z "${BOOT_DIR}" ]]; then
  BOOT_DIR="${OCTA_REPO}/octa/var/evidence/systemd_boot_$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "${BOOT_DIR}"
EVENTS_PATH="${BOOT_DIR}/ibkr_autologin_events.jsonl"

# ── Step 1: DISPLAY + X11 validation ─────────────────────────────────────────
if [[ -z "${DISPLAY:-}" ]]; then
  export DISPLAY=":99"
else
  export DISPLAY="${OCTA_DISPLAY:-${OCTA_XVFB_DISPLAY:-${DISPLAY}}}"
fi

if [[ "${DISPLAY}" == ":99" ]]; then
  if ! pgrep -f "Xvfb ${DISPLAY}" >/dev/null 2>&1; then
    echo "Xvfb not running on ${DISPLAY}, attempting to start..."
    Xvfb "${DISPLAY}" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
    sleep 2
  fi
fi

if ! xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; then
  _err="{\"event_type\":\"autologin_error\",\"reason\":\"x11_not_reachable\",\"display\":\"${DISPLAY}\"}"
  printf '%s\n' "${_err}" >> "${BOOT_DIR}/events.jsonl"
  echo "${_err}" >&2
  exit 1
fi

LOGIN_TIMEOUT="${OCTA_IBKR_LOGIN_TIMEOUT_SEC:-120}"

printf '{"event_type":"autologin_bootstrap","display":"%s","python":"%s"}\n' "${DISPLAY}" "${PY}" >> "${BOOT_DIR}/events.jsonl"

# ── Step 2: Login step-machine (credentials + disclaimer) ────────────────────
# Only if credentials are configured (OCTA_IBKR_USER or OCTA_IBKR_SECRETS_FILE).
# If no credentials, skip login automation — user may handle it manually or via IBC.
if [[ -n "${OCTA_IBKR_USER:-}" ]] || [[ -n "${OCTA_IBKR_SECRETS_FILE:-}" ]]; then
  LOGIN_EVIDENCE="${BOOT_DIR}/login_steps"
  mkdir -p "${LOGIN_EVIDENCE}"
  printf '{"event_type":"login_step_machine_start","display":"%s"}\n' "${DISPLAY}" >> "${BOOT_DIR}/events.jsonl"

  set +e
  LOGIN_OUT="$("${PY}" -m octa.execution.ibkr_x11_login \
    --display "${DISPLAY}" \
    --timeout-sec "${LOGIN_TIMEOUT}" \
    --evidence-dir "${LOGIN_EVIDENCE}" 2>&1)"
  LOGIN_RC=$?
  set -e

  printf '{"event_type":"login_step_machine_done","rc":%s,"output":%s}\n' \
    "${LOGIN_RC}" \
    "$(printf '%s' "${LOGIN_OUT}" | "${PY}" -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" \
    >> "${BOOT_DIR}/events.jsonl"

  if [[ ${LOGIN_RC} -ne 0 ]]; then
    printf '{"event_type":"autologin_error","reason":"login_step_machine_failed","rc":%s}\n' "${LOGIN_RC}" >> "${BOOT_DIR}/events.jsonl"
    exit "${LOGIN_RC}"
  fi
else
  printf '{"event_type":"login_skip","reason":"no_credentials_configured"}\n' >> "${BOOT_DIR}/events.jsonl"
fi

# ── Step 3: Popup watcher (recurring disclaimers, reconnect dialogs) ─────────
printf '{"event_type":"watcher_started","display":"%s"}\n' "${DISPLAY}" >> "${BOOT_DIR}/events.jsonl"
"${PY}" -m octa.execution.ibkr_x11_autologin --run --db "${DB_PATH}" --keepalive --timeout-sec 0 --events-path "${EVENTS_PATH}" > "${BOOT_DIR}/autologin_state.json"
