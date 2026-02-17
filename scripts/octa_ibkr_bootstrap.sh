#!/usr/bin/env bash
set -euo pipefail

: "${OCTA_REPO:?OCTA_REPO is required}"
: "${OCTA_PY:?OCTA_PY is required}"
MODE="${OCTA_IBKR_MODE:-tws}"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-${OCTA_REPO}/octa/var/evidence/systemd_boot_$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${BOOT_DIR}"

TWS_CMD="${OCTA_TWS_CMD:-}"
GW_CMD="${OCTA_GATEWAY_CMD:-}"
PROC_MATCH="${OCTA_IBKR_PROCESS_MATCH:-}"
HOST="${OCTA_IBKR_HOST:-127.0.0.1}"
PORT="${OCTA_IBKR_PORT:-7497}"

if [[ "${MODE}" == "tws" && -z "${TWS_CMD}" ]]; then
  printf '{"event_type":"ibkr_error","reason":"missing_tws_cmd"}\n' >> "${BOOT_DIR}/events.jsonl"
  exit 2
fi
if [[ "${MODE}" == "gateway" && -z "${GW_CMD}" ]]; then
  printf '{"event_type":"ibkr_error","reason":"missing_gateway_cmd"}\n' >> "${BOOT_DIR}/events.jsonl"
  exit 2
fi

ARGS=("${OCTA_PY}" -m octa.execution.ibkr_runtime --mode "${MODE}" --ensure-running --host "${HOST}" --port "${PORT}")
if [[ -n "${PROC_MATCH}" ]]; then
  ARGS+=(--process-match "${PROC_MATCH}")
fi
if [[ -n "${TWS_CMD}" ]]; then
  ARGS+=(--tws-cmd "${TWS_CMD}")
fi
if [[ -n "${GW_CMD}" ]]; then
  ARGS+=(--gateway-cmd "${GW_CMD}")
fi

OUT="$(${ARGS[@]})"
RC=$?
printf '%s\n' "${OUT}" > "${BOOT_DIR}/ibkr_process_state.json"
printf '{"event_type":"ibkr_started","rc":%s,"payload":%s}\n' "${RC}" "$(printf '%s' "${OUT}" | python -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" >> "${BOOT_DIR}/events.jsonl"
if [[ ${RC} -ne 0 ]]; then
  exit ${RC}
fi
