#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# OCTA IBKR bootstrap — deterministic TWS/Gateway lifecycle + health loop
# ──────────────────────────────────────────────────────────────────────────────

# ── Step 0: Python interpreter — absolute venv path, never bare "python" ─────
REPO="/home/n-b/Octa"
PY="${OCTA_PY:-${REPO}/.venv/bin/python}"

if [[ ! -x "${PY}" ]]; then
  _err="{\"event_type\":\"ibkr_error\",\"reason\":\"python_not_executable\",\"value\":\"${PY}\"}"
  echo "${_err}" >&2
  mkdir -p "${REPO}/octa/var/evidence" 2>/dev/null || true
  printf '%s\n' "${_err}" >> "${REPO}/octa/var/evidence/events_preflight.jsonl" 2>/dev/null || true
  exit 1
fi

echo "Using Python: ${PY}"
"${PY}" -c "import sys; print(sys.executable)"

: "${OCTA_REPO:=${REPO}}"
export OCTA_REPO

MODE="${OCTA_IBKR_MODE:-tws}"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-}"
if [[ -z "${BOOT_DIR}" ]] && [[ -f "${OCTA_REPO}/octa/var/runtime/systemd_boot_dir" ]]; then
  BOOT_DIR="$(cat "${OCTA_REPO}/octa/var/runtime/systemd_boot_dir" 2>/dev/null || true)"
fi
if [[ -z "${BOOT_DIR}" ]]; then
  BOOT_DIR="${OCTA_REPO}/octa/var/evidence/systemd_boot_$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "${BOOT_DIR}"

# ── Step 1: DISPLAY + X11 validation ─────────────────────────────────────────
if [[ -z "${DISPLAY:-}" ]]; then
  export DISPLAY=":99"
else
  export DISPLAY="${OCTA_DISPLAY:-${OCTA_XVFB_DISPLAY:-${DISPLAY}}}"
fi

# If DISPLAY=:99, ensure Xvfb is running
if [[ "${DISPLAY}" == ":99" ]]; then
  if ! pgrep -f "Xvfb ${DISPLAY}" >/dev/null 2>&1; then
    echo "Xvfb not running on ${DISPLAY}, attempting to start..."
    Xvfb "${DISPLAY}" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
    XVFB_PID=$!
    sleep 2
    printf '{"event_type":"xvfb_started","pid":%s,"display":"%s"}\n' "${XVFB_PID}" "${DISPLAY}" >> "${BOOT_DIR}/events.jsonl"
  fi
fi

# Verify display reachable
if ! xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; then
  _err="{\"event_type\":\"ibkr_error\",\"reason\":\"x11_not_reachable\",\"display\":\"${DISPLAY}\"}"
  printf '%s\n' "${_err}" >> "${BOOT_DIR}/events.jsonl"
  echo "${_err}" >&2
  exit 1
fi

printf '{"event_type":"runtime_env","python":"%s","display":"%s","mode":"%s"}\n' "${PY}" "${DISPLAY}" "${MODE}" >> "${BOOT_DIR}/events.jsonl"

PROC_MATCH="${OCTA_IBKR_PROCESS_MATCH:-}"
HOST="${OCTA_IBKR_HOST:-127.0.0.1}"
PORT="${OCTA_IBKR_PORT:-7497}"
HEALTH_INTERVAL="${OCTA_IBKR_HEALTH_INTERVAL_SEC:-5}"
STARTUP_GRACE="${OCTA_IBKR_STARTUP_GRACE_SEC:-180}"

# ── Step 2: Deterministic TWS/Gateway command resolution ─────────────────────
# Priority: env var > fixed known paths > PATH lookup
# Fail-closed with diagnostics if nothing found.

resolve_tws_cmd() {
  local explicit="${OCTA_TWS_CMD:-}"
  if [[ -n "${explicit}" ]]; then
    printf '%s' "${explicit}"; return 0
  fi
  local candidates=(
    "${HOME}/Jts/tws"
    "${HOME}/Jts/tws/tws"
    "${HOME}/Jts/tws/tws.sh"
  )
  for c in "${candidates[@]}"; do
    if [[ -x "${c}" ]]; then
      printf '%s' "${c}"; return 0
    fi
  done
  if command -v tws >/dev/null 2>&1; then
    command -v tws; return 0
  fi
  return 1
}

resolve_gateway_cmd() {
  local explicit="${OCTA_GATEWAY_CMD:-}"
  if [[ -n "${explicit}" ]]; then
    printf '%s' "${explicit}"; return 0
  fi
  local candidates=(
    "${HOME}/Jts/ibgateway/1041/ibgateway"
    "${HOME}/Jts/ibgateway/ibgateway"
    "${HOME}/Jts/ibgateway/ibgateway.sh"
  )
  for c in "${candidates[@]}"; do
    if [[ -x "${c}" ]]; then
      printf '%s' "${c}"; return 0
    fi
  done
  if command -v ibgateway >/dev/null 2>&1; then
    command -v ibgateway; return 0
  fi
  return 1
}

TWS_CMD=""
GW_CMD=""

if [[ "${MODE}" == "tws" ]]; then
  TWS_CMD="$(resolve_tws_cmd)" || true
  if [[ -z "${TWS_CMD}" ]]; then
    printf '{"event_type":"ibkr_error","reason":"missing_tws_cmd","detail":"checked OCTA_TWS_CMD env, ~/Jts/tws, ~/Jts/tws/tws, ~/Jts/tws/tws.sh, PATH"}\n' >> "${BOOT_DIR}/events.jsonl"
    exit 2
  fi
  printf '{"event_type":"ibkr_cmd_resolved","mode":"tws","cmd":"%s"}\n' "${TWS_CMD}" >> "${BOOT_DIR}/events.jsonl"
fi
if [[ "${MODE}" == "gateway" ]]; then
  GW_CMD="$(resolve_gateway_cmd)" || true
  if [[ -z "${GW_CMD}" ]]; then
    printf '{"event_type":"ibkr_error","reason":"missing_gateway_cmd","detail":"checked OCTA_GATEWAY_CMD env, ~/Jts/ibgateway/1041/ibgateway, ~/Jts/ibgateway/ibgateway, ~/Jts/ibgateway/ibgateway.sh, PATH"}\n' >> "${BOOT_DIR}/events.jsonl"
    exit 2
  fi
  printf '{"event_type":"ibkr_cmd_resolved","mode":"gateway","cmd":"%s"}\n' "${GW_CMD}" >> "${BOOT_DIR}/events.jsonl"
fi

# ── Step 3: Launch TWS/Gateway ────────────────────────────────────────────────
ARGS=("${PY}" -m octa.execution.ibkr_runtime --mode "${MODE}" --ensure-running --host "${HOST}" --port "${PORT}")
if [[ -n "${PROC_MATCH}" ]]; then
  ARGS+=(--process-match "${PROC_MATCH}")
fi
if [[ -n "${TWS_CMD}" ]]; then
  ARGS+=(--tws-cmd "${TWS_CMD}")
fi
if [[ -n "${GW_CMD}" ]]; then
  ARGS+=(--gateway-cmd "${GW_CMD}")
fi

set +e
OUT="$("${ARGS[@]}")"
RC=$?
set -e
printf '%s\n' "${OUT}" > "${BOOT_DIR}/ibkr_process_state.json"
printf '{"event_type":"ibkr_started","rc":%s,"payload":%s}\n' "${RC}" "$(printf '%s' "${OUT}" | "${PY}" -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" >> "${BOOT_DIR}/events.jsonl"
if [[ ${RC} -ne 0 ]]; then
  exit ${RC}
fi

# ── Step 4: Health loop with startup grace ────────────────────────────────────
BOOT_EPOCH=$(date +%s)

while true; do
  HEALTH_ARGS=("${PY}" -m octa.execution.ibkr_runtime --mode "${MODE}" --health --host "${HOST}" --port "${PORT}")
  if [[ -n "${PROC_MATCH}" ]]; then
    HEALTH_ARGS+=(--process-match "${PROC_MATCH}")
  fi
  if [[ -n "${TWS_CMD}" ]]; then
    HEALTH_ARGS+=(--tws-cmd "${TWS_CMD}")
  fi
  if [[ -n "${GW_CMD}" ]]; then
    HEALTH_ARGS+=(--gateway-cmd "${GW_CMD}")
  fi

  set +e
  HEALTH_OUT="$("${HEALTH_ARGS[@]}")"
  HEALTH_RC=$?
  set -e
  ELAPSED=$(( $(date +%s) - BOOT_EPOCH ))
  printf '{"event_type":"ibkr_health_tick","rc":%s,"elapsed_sec":%s,"grace_sec":%s,"payload":%s}\n' \
    "${HEALTH_RC}" "${ELAPSED}" "${STARTUP_GRACE}" \
    "$(printf '%s' "${HEALTH_OUT}" | "${PY}" -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" \
    >> "${BOOT_DIR}/events.jsonl"
  if [[ ${HEALTH_RC} -ne 0 ]]; then
    # During startup grace period, tolerate port-not-reachable if process is alive.
    if [[ ${ELAPSED} -lt ${STARTUP_GRACE} ]]; then
      PROC_ALIVE="$(printf '%s' "${HEALTH_OUT}" | "${PY}" -c 'import json,sys; d=json.load(sys.stdin); print("1" if d.get("process_alive") else "0")' 2>/dev/null || echo "0")"
      if [[ "${PROC_ALIVE}" == "1" ]]; then
        printf '{"event_type":"ibkr_grace_period","elapsed_sec":%s,"grace_sec":%s,"action":"tolerate_port_down"}\n' "${ELAPSED}" "${STARTUP_GRACE}" >> "${BOOT_DIR}/events.jsonl"
        sleep "${HEALTH_INTERVAL}"
        continue
      fi
    fi
    printf '{"event_type":"restart_reason","reason":"ibkr_health_probe_failed","rc":%s,"elapsed_sec":%s}\n' "${HEALTH_RC}" "${ELAPSED}" >> "${BOOT_DIR}/events.jsonl"
    exit "${HEALTH_RC}"
  fi
  sleep "${HEALTH_INTERVAL}"
done
