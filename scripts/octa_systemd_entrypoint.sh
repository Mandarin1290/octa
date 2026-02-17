#!/usr/bin/env bash
set -euo pipefail

UTC_NOW="$(date -u +%Y%m%dT%H%M%SZ)"
: "${OCTA_REPO:?OCTA_REPO is required}"

mkdir -p "${OCTA_REPO}/octa/var/runtime" "${OCTA_REPO}/octa/var/evidence"
BOOT_DIR="${OCTA_REPO}/octa/var/evidence/systemd_boot_${UTC_NOW}"
mkdir -p "${BOOT_DIR}"
export OCTA_BOOT_EVIDENCE_DIR="${BOOT_DIR}"

cat > "${BOOT_DIR}/boot_start.json" <<JSON
{"event_type":"boot_start","ts":"${UTC_NOW}","repo":"${OCTA_REPO}"}
JSON

printf '{"event_type":"boot_start","ts":"%s","repo":"%s"}\n' "${UTC_NOW}" "${OCTA_REPO}" >> "${BOOT_DIR}/events.jsonl"

echo "${BOOT_DIR}"
