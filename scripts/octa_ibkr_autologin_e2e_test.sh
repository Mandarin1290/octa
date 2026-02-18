#!/usr/bin/env bash
set -euo pipefail

MAX_WAIT_SEC="${MAX_WAIT_SEC:-180}"
POLL_SEC="${POLL_SEC:-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BOOT_PTR="${REPO_DIR}/octa/var/runtime/systemd_boot_dir"
ENV_FILE="${HOME}/.config/octa/env"
IBKR_SERVICE="octa-ibkr.service"
AUTOLOGIN_SERVICE="octa-autologin.service"

if command -v rg >/dev/null 2>&1; then
  SEARCH_TOOL="rg"
else
  SEARCH_TOOL="grep"
fi

info() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

print_diagnostics() {
  local evid="${1:-}"
  set +e
  echo "----- journalctl: ${IBKR_SERVICE} (last 120) -----"
  journalctl --user -u "${IBKR_SERVICE}" -n 120 --no-pager -l
  echo "----- journalctl: ${AUTOLOGIN_SERVICE} (last 120) -----"
  journalctl --user -u "${AUTOLOGIN_SERVICE}" -n 120 --no-pager -l
  if [[ -n "${evid}" && -f "${evid}" ]]; then
    echo "----- evidence tail (last 200) -----"
    tail -n 200 "${evid}"
  else
    echo "----- evidence tail unavailable -----"
  fi
  set -e
}

stop_services() {
  set +e
  systemctl --user stop "${AUTOLOGIN_SERVICE}" >/dev/null 2>&1
  systemctl --user stop "${IBKR_SERVICE}" >/dev/null 2>&1
  set -e
}

fail_with() {
  local code="$1"
  local message="$2"
  local evid="${3:-}"
  warn "${message}"
  print_diagnostics "${evid}"
  stop_services
  exit "${code}"
}

file_has_pattern() {
  local pattern="$1"
  local file="$2"
  if [[ ! -f "${file}" ]]; then
    return 1
  fi
  if [[ "${SEARCH_TOOL}" == "rg" ]]; then
    rg -q "${pattern}" "${file}"
  else
    grep -E -q "${pattern}" "${file}"
  fi
}

count_pattern() {
  local pattern="$1"
  local file="$2"
  if [[ ! -f "${file}" ]]; then
    echo "0"
    return 0
  fi
  if [[ "${SEARCH_TOOL}" == "rg" ]]; then
    rg -c "${pattern}" "${file}" 2>/dev/null || echo "0"
  else
    grep -E -c "${pattern}" "${file}" 2>/dev/null || echo "0"
  fi
}

set_env_value() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp
  local found=0
  mkdir -p "$(dirname "${file}")"
  touch "${file}"
  tmp="$(mktemp)"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" =~ ^[[:space:]]*(export[[:space:]]+)?${key}= ]]; then
      printf '%s=%s\n' "${key}" "${value}" >> "${tmp}"
      found=1
    else
      printf '%s\n' "${line}" >> "${tmp}"
    fi
  done < "${file}"
  if [[ "${found}" -eq 0 ]]; then
    printf '%s=%s\n' "${key}" "${value}" >> "${tmp}"
  fi
  mv "${tmp}" "${file}"
  chmod 600 "${file}"
}

count_ibkr_java() {
  local uid
  uid="$(id -u)"
  pgrep -u "${uid}" -fa java 2>/dev/null | awk '
    {
      low=tolower($0)
      if (index(low, "/jts/") || index(low, "trader workstation") || index(low, "ibgateway")) c++
    }
    END { print c+0 }'
}

wait_for_file() {
  local path="$1"
  local timeout_sec="$2"
  local waited=0
  while (( waited < timeout_sec )); do
    if [[ -f "${path}" ]]; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

main() {
  local evid_file=""
  local boot_dir=""
  local deadline now elapsed
  local seen_progress=0

  info "Reloading user systemd units"
  systemctl --user daemon-reload

  info "Forcing OCTA_AUTOLOGIN_MODE=monitor in ${ENV_FILE}"
  set_env_value "${ENV_FILE}" "OCTA_AUTOLOGIN_MODE" "monitor"

  info "Restarting ${IBKR_SERVICE}"
  systemctl --user restart "${IBKR_SERVICE}"
  info "Restarting ${AUTOLOGIN_SERVICE}"
  systemctl --user restart "${AUTOLOGIN_SERVICE}"

  if ! systemctl --user is-active --quiet "${IBKR_SERVICE}"; then
    fail_with 4 "${IBKR_SERVICE} is not active after restart" "${evid_file}"
  fi
  if ! systemctl --user is-active --quiet "${AUTOLOGIN_SERVICE}"; then
    fail_with 4 "${AUTOLOGIN_SERVICE} is not active after restart" "${evid_file}"
  fi

  if ! wait_for_file "${BOOT_PTR}" 20; then
    fail_with 4 "missing boot dir pointer: ${BOOT_PTR}" "${evid_file}"
  fi
  boot_dir="$(<"${BOOT_PTR}")"
  if [[ -z "${boot_dir}" ]]; then
    fail_with 4 "boot dir pointer is empty: ${BOOT_PTR}" "${evid_file}"
  fi
  evid_file="${boot_dir}/octa_ibkr_autologin_watch/events.jsonl"
  if ! wait_for_file "${evid_file}" 30; then
    fail_with 4 "missing evidence file: ${evid_file}" "${evid_file}"
  fi
  info "Evidence file: ${evid_file}"

  deadline=$((SECONDS + MAX_WAIT_SEC))

  while (( SECONDS < deadline )); do
    local tws_count
    local mode_selected mode_monitor
    local login_detected login_success login_not_needed
    local disclaimer_detected disclaimer_success disclaimer_not_present
    local popup_detected popup_success popup_not_present

    tws_count="$(count_ibkr_java)"
    if [[ "${tws_count}" -gt 1 ]]; then
      fail_with 3 "multiple IBKR java processes detected (${tws_count}); expected exactly one" "${evid_file}"
    fi

    if file_has_pattern '"event_type":"stuck"' "${evid_file}"; then
      fail_with 2 "stuck event detected" "${evid_file}"
    fi
    if file_has_pattern '"event_type":"autologin_error"' "${evid_file}"; then
      fail_with 3 "autologin_error detected" "${evid_file}"
    fi
    if file_has_pattern 'persisted_after_|attempted_but_persisted' "${evid_file}"; then
      fail_with 2 "persisted popup/disclaimer detected" "${evid_file}"
    fi

    mode_selected=0
    mode_monitor=0
    login_detected=0
    login_success=0
    login_not_needed=0
    disclaimer_detected=0
    disclaimer_success=0
    disclaimer_not_present=0
    popup_detected=0
    popup_success=0
    popup_not_present=0

    file_has_pattern '"event_type":"mode_selected"' "${evid_file}" && mode_selected=1 || true
    file_has_pattern '"event_type":"mode_selected".*"mode":"monitor"' "${evid_file}" && mode_monitor=1 || true

    file_has_pattern '"event_type":"window_detected".*"role":"login"' "${evid_file}" && login_detected=1 || true
    file_has_pattern '"event_type":"login_attempt_done".*"success":true' "${evid_file}" && login_success=1 || true
    file_has_pattern '"event_type":"state_change".*"to_state":"S2_DISCLAIMER"' "${evid_file}" && login_success=1 || true
    file_has_pattern '"reason":"login_window_not_present_main_visible"' "${evid_file}" && login_not_needed=1 || true

    file_has_pattern '"event_type":"disclaimer_detected"' "${evid_file}" && disclaimer_detected=1 || true
    file_has_pattern '"event_type":"disclaimer_action_done".*"ok":true' "${evid_file}" && disclaimer_success=1 || true
    file_has_pattern '"reason":"no_disclaimer_main_visible"' "${evid_file}" && disclaimer_not_present=1 || true

    file_has_pattern '"event_type":"window_detected".*"role":"login_message_popup"' "${evid_file}" && popup_detected=1 || true
    file_has_pattern '"event_type":"popup_closed".*"role":"login_message_popup".*"success":true' "${evid_file}" && popup_success=1 || true
    file_has_pattern '"reason":"main_visible_no_modals_10s"' "${evid_file}" && popup_not_present=1 || true

    if file_has_pattern '"event_type":"window_detected".*"role":"(login|disclaimer|login_message_popup)"' "${evid_file}" \
      || file_has_pattern '"event_type":"action_performed".*"role":"(login|disclaimer|login_message_popup)"' "${evid_file}" \
      || file_has_pattern '"event_type":"disclaimer_action_done"' "${evid_file}" \
      || file_has_pattern '"event_type":"popup_closed"' "${evid_file}" \
      || file_has_pattern '"event_type":"login_attempt_done".*"success":true' "${evid_file}"; then
      seen_progress=1
    fi

    local login_ok disclaimer_ok popup_ok
    login_ok=0
    disclaimer_ok=0
    popup_ok=0

    if (( login_detected == 1 )); then
      (( login_success == 1 )) && login_ok=1 || true
    else
      (( login_not_needed == 1 )) && login_ok=1 || true
    fi

    if (( disclaimer_detected == 1 )); then
      (( disclaimer_success == 1 )) && disclaimer_ok=1 || true
    else
      (( disclaimer_not_present == 1 )) && disclaimer_ok=1 || true
    fi

    if (( popup_detected == 1 )); then
      (( popup_success == 1 )) && popup_ok=1 || true
    else
      (( popup_not_present == 1 )) && popup_ok=1 || true
    fi

    if (( mode_selected == 1 && mode_monitor == 1 && login_ok == 1 && disclaimer_ok == 1 && popup_ok == 1 )); then
      local c_mode c_unknown c_login c_disc c_pop c_actions
      c_mode="$(count_pattern '"event_type":"mode_selected"' "${evid_file}")"
      c_unknown="$(count_pattern '"event_type":"unknown_window_ignored"' "${evid_file}")"
      c_login="$(count_pattern '"event_type":"window_detected".*"role":"login"' "${evid_file}")"
      c_disc="$(count_pattern '"event_type":"disclaimer_detected"' "${evid_file}")"
      c_pop="$(count_pattern '"event_type":"window_detected".*"role":"login_message_popup"' "${evid_file}")"
      c_actions="$(count_pattern '"event_type":"action_performed".*"role":"(login|disclaimer|login_message_popup)"' "${evid_file}")"
      printf 'PASS mode_selected=%s login_detected=%s disclaimer_detected=%s popup_detected=%s actions=%s unknown_ignored=%s\n' \
        "${c_mode}" "${c_login}" "${c_disc}" "${c_pop}" "${c_actions}" "${c_unknown}"
      return 0
    fi

    sleep "${POLL_SEC}"
  done

  now="${SECONDS}"
  elapsed=$(( now - (deadline - MAX_WAIT_SEC) ))
  if [[ "$(count_ibkr_java)" -eq 1 ]] \
    && ! file_has_pattern '"event_type":"window_detected".*"role":"(login|disclaimer|login_message_popup)"' "${evid_file}" \
    && ! file_has_pattern '"event_type":"disclaimer_detected"' "${evid_file}"; then
    fail_with 4 "timeout ${elapsed}s: TWS running but no login/disclaimer/popup windows detected (DISPLAY/X11 visibility issue)" "${evid_file}"
  fi
  if (( seen_progress == 0 )); then
    fail_with 4 "timeout ${elapsed}s: no progress events seen" "${evid_file}"
  fi
  fail_with 4 "timeout ${elapsed}s: required evidence conditions not satisfied" "${evid_file}"
}

main "$@"
