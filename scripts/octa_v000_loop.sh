#!/usr/bin/env bash
set -euo pipefail

: "${OCTA_REPO:?OCTA_REPO is required}"
: "${OCTA_PY:?OCTA_PY is required}"
: "${OCTA_V000_CONFIG:?OCTA_V000_CONFIG is required}"
BOOT_DIR="${OCTA_BOOT_EVIDENCE_DIR:-${OCTA_REPO}/octa/var/evidence/systemd_boot_$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${BOOT_DIR}"

BACKOFF=2
MAX_BACKOFF=30
while true; do
  TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '{"event_type":"v000_cycle_start","ts":"%s"}\n' "${TS}" >> "${BOOT_DIR}/events.jsonl"

  set +e
  "${OCTA_PY}" -m octa.support.ops.v000_full_universe_cascade_train --config "${OCTA_V000_CONFIG}"
  RC=$?
  set -e

  TS_END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '{"event_type":"v000_cycle_end","ts":"%s","rc":%s}\n' "${TS_END}" "${RC}" >> "${BOOT_DIR}/events.jsonl"

  if [[ ${RC} -eq 0 ]]; then
    BACKOFF=2
  else
    printf '{"event_type":"restart_reason","reason":"v000_nonzero_exit","rc":%s}\n' "${RC}" >> "${BOOT_DIR}/events.jsonl"
    sleep "${BACKOFF}"
    if [[ ${BACKOFF} -lt ${MAX_BACKOFF} ]]; then
      BACKOFF=$((BACKOFF * 2))
      if [[ ${BACKOFF} -gt ${MAX_BACKOFF} ]]; then
        BACKOFF=${MAX_BACKOFF}
      fi
    fi
  fi
done
