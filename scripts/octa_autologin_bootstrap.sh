#!/usr/bin/env bash
set -euo pipefail

: "${OCTA_REPO:?OCTA_REPO is required}"
: "${OCTA_PY:?OCTA_PY is required}"
DB_PATH="${OCTA_IBKR_DB:-octa/var/runtime/ibkr_autologin.sqlite3}"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-${OCTA_REPO}/octa/var/evidence/systemd_boot_$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${BOOT_DIR}"
EVENTS_PATH="${BOOT_DIR}/ibkr_autologin_events.jsonl"

printf '{"event_type":"watcher_started"}\n' >> "${BOOT_DIR}/events.jsonl"
"${OCTA_PY}" -m octa.execution.ibkr_x11_autologin --run --db "${DB_PATH}" --keepalive --timeout-sec 0 --events-path "${EVENTS_PATH}" > "${BOOT_DIR}/autologin_state.json"
