#!/usr/bin/env bash
# =============================================================================
# octa/support/x11/tws_popup_controller.sh
#
# Deterministic TWS X11 popup handler.
# Watches for blocking popup windows and dismisses them via a strict
# three-step sequence: wmctrl polite close → xdotool key sequence →
# xwininfo geometry + relative click.
#
# Exit codes:
#   0  = OK (no blocking popups present or all dismissed)
#  10  = PRECHECK_FAIL (X11 not reachable or required tool missing)
#  25  = POPUP_STILL_PRESENT (popup not dismissed within watch timeout)
#
# Called from: run_tws_autologin.sh (after Python chain + drain loop)
# Working directory: ~/Octa (set by run_tws_autologin.sh)
# Environment: DISPLAY and XAUTHORITY must be exported by caller.
# =============================================================================
set -uo pipefail

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
readonly WATCH_DURATION=180   # total watch wall-clock seconds
readonly WATCH_INTERVAL=2     # seconds between scan iterations
readonly SETTLE_WAIT="0.3"    # seconds to wait after each close attempt
readonly MIN_CLEAN_STREAK=5   # consecutive clean iterations before exit OK

readonly EXIT_OK=0
readonly EXIT_PRECHECK_FAIL=10
readonly EXIT_POPUP_STILL_PRESENT=25

# Popup title tokens — case-insensitive substring match.
# WARNING: "Trader Workstation" and "Interactive Brokers" also appear
# on the main TWS window; the TWS PID filter prevents accidental closure,
# but operators should verify if the main window is unexpectedly targeted.
readonly -a POPUP_TOKENS=(
    "Warnhinweis"
    "Disclaimer"
    "Haftung"
    "Login Messages"
    "Message Center"
    "Börsenspiegel"
    "Programm wird geschlossen"
    "Trader Workstation"
    "Interactive Brokers"
)

# Hardcoded relative click offsets (from window upper-left corner, pixels).
# These target the primary dismiss button for each popup category.
readonly REL_X_DISCLAIMER=260   # Disclaimer / Warnhinweis / Haftung
readonly REL_Y_DISCLAIMER=410
readonly REL_X_LOGIN_MSG=300    # Login Messages / Message Center
readonly REL_Y_LOGIN_MSG=380

# ---------------------------------------------------------------------------
# EVIDENCE DIRECTORY
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="${REPO_DIR}/octa/var/evidence/tws_popup_controller_${STAMP}"
mkdir -p "${EVIDENCE_DIR}"

# Tee all stdout/stderr to evidence log files while preserving console output.
# Must come before any _log calls.
exec > >(tee -a "${EVIDENCE_DIR}/controller_stdout.log") \
     2> >(tee -a "${EVIDENCE_DIR}/controller_stderr.log" >&2)

readonly CSV="${EVIDENCE_DIR}/popups_seen.csv"
printf 'ts,wid,title,pid,action,result\n' > "${CSV}"

# ---------------------------------------------------------------------------
# LOGGING HELPERS
# ---------------------------------------------------------------------------
_log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

_csv_row() {
    # args: wid title pid action result
    local ts wid title pid action result
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    wid="${1:-}"
    title="${2:-}"
    pid="${3:-}"
    action="${4:-}"
    result="${5:-}"
    printf '%s,%s,%s,%s,%s,%s\n' \
        "${ts}" "${wid}" "${title}" "${pid}" "${action}" "${result}" >> "${CSV}"
}

_write_result_json() {
    local code="${1}" reason="${2}"
    printf '{\n  "code": %s,\n  "evidence_dir": "%s",\n  "reason": "%s"\n}\n' \
        "${code}" "${EVIDENCE_DIR}" "${reason}" \
        > "${EVIDENCE_DIR}/result.json"
}

# ---------------------------------------------------------------------------
# PHASE A — X11 PRECHECK
# ---------------------------------------------------------------------------
_log "=== TWS POPUP CONTROLLER START === stamp=${STAMP}"
_log "EVIDENCE_DIR=${EVIDENCE_DIR}"
_log "DISPLAY=${DISPLAY:-<unset>}"

if [[ -z "${DISPLAY:-}" ]]; then
    _log "PRECHECK_FAIL: DISPLAY is not set"
    _write_result_json "${EXIT_PRECHECK_FAIL}" "PRECHECK_FAIL"
    exit "${EXIT_PRECHECK_FAIL}"
fi

if ! xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; then
    _log "PRECHECK_FAIL: xdpyinfo cannot reach display ${DISPLAY}"
    _write_result_json "${EXIT_PRECHECK_FAIL}" "PRECHECK_FAIL"
    exit "${EXIT_PRECHECK_FAIL}"
fi

if ! wmctrl -m >/dev/null 2>&1; then
    _log "PRECHECK_FAIL: wmctrl -m failed (window manager not reachable)"
    _write_result_json "${EXIT_PRECHECK_FAIL}" "PRECHECK_FAIL"
    exit "${EXIT_PRECHECK_FAIL}"
fi

# Verify required tools are present.
for _tool in wmctrl xdotool xwininfo xdpyinfo; do
    if ! command -v "${_tool}" >/dev/null 2>&1; then
        _log "PRECHECK_FAIL: required tool '${_tool}' not found in PATH"
        _write_result_json "${EXIT_PRECHECK_FAIL}" "PRECHECK_FAIL"
        exit "${EXIT_PRECHECK_FAIL}"
    fi
done

_log "X11 precheck: PASS"

# ---------------------------------------------------------------------------
# CAPTURE INITIAL WINDOW LIST
# ---------------------------------------------------------------------------
wmctrl -lp > "${EVIDENCE_DIR}/windows_before.txt" 2>/dev/null || true
_log "windows_before.txt: $(wc -l < "${EVIDENCE_DIR}/windows_before.txt") windows"

# ---------------------------------------------------------------------------
# TWS PID DISCOVERY
# Per spec: detect via pgrep -f tws or java (JTS)
# ---------------------------------------------------------------------------
mapfile -t TWS_PIDS < <(
    { pgrep -f 'tws' 2>/dev/null; pgrep -f '[Jj][Tt][Ss]' 2>/dev/null; true; } \
    | sort -u
)

if [[ ${#TWS_PIDS[@]} -eq 0 ]]; then
    _log "WARN: no TWS PIDs found — proceeding with title-only matching (degraded mode)"
else
    _log "TWS PIDs: ${TWS_PIDS[*]}"
fi

_is_tws_pid() {
    local target="${1:-}"
    # Allow '0' pid (wmctrl reports 0 for some windows) — reject it.
    [[ -z "${target}" || "${target}" == "0" ]] && return 1
    # If TWS PID set is empty, accept all (degraded mode: title filter only).
    if [[ ${#TWS_PIDS[@]} -eq 0 ]]; then
        return 0
    fi
    local p
    for p in "${TWS_PIDS[@]}"; do
        [[ "${p}" == "${target}" ]] && return 0
    done
    return 1
}

# ---------------------------------------------------------------------------
# TITLE TOKEN MATCHING
# Returns the matched token (stdout) and exits 0; exits 1 if no match.
# ---------------------------------------------------------------------------
_title_matches_token() {
    local title_l="${1,,}"  # lowercase input
    local tok
    for tok in "${POPUP_TOKENS[@]}"; do
        if [[ "${title_l}" == *"${tok,,}"* ]]; then
            printf '%s' "${tok}"
            return 0
        fi
    done
    return 1
}

# ---------------------------------------------------------------------------
# WINDOW EXISTENCE CHECK
# Returns 0 if window still present, 1 if gone.
# ---------------------------------------------------------------------------
_window_exists() {
    local wid="${1:-}" title="${2:-}"
    local raw
    raw="$(wmctrl -lp 2>/dev/null)" || return 0  # fail-safe: assume present
    # Primary: exact wid match at start of line (case-insensitive hex).
    if printf '%s\n' "${raw}" | grep -qi "^${wid}[[:space:]]"; then
        return 0
    fi
    # Fallback: title substring match (handles wid format variations).
    if [[ -n "${title}" ]] && printf '%s\n' "${raw}" | grep -qi "${title}"; then
        return 0
    fi
    return 1
}

# ---------------------------------------------------------------------------
# STEP 3: RELATIVE CLICK via xwininfo geometry
# Returns 0 on success, 1 if no coords defined or geometry unavailable.
# ---------------------------------------------------------------------------
_relative_click() {
    local wid="${1:-}" title="${2:-}"
    local title_l="${title,,}"
    local rel_x rel_y

    # Select hardcoded relative coords by popup category.
    if   [[ "${title_l}" == *"disclaimer"* ]] \
      || [[ "${title_l}" == *"warnhinweis"* ]] \
      || [[ "${title_l}" == *"haftung"* ]]; then
        rel_x=${REL_X_DISCLAIMER}
        rel_y=${REL_Y_DISCLAIMER}
    elif [[ "${title_l}" == *"login messages"* ]] \
      || [[ "${title_l}" == *"login message"* ]] \
      || [[ "${title_l}" == *"message center"* ]]; then
        rel_x=${REL_X_LOGIN_MSG}
        rel_y=${REL_Y_LOGIN_MSG}
    else
        _log "  STEP3: no click coords defined for title='${title}' — key sequence only"
        return 1
    fi

    # Get absolute window position via xwininfo.
    local wininfo win_x win_y
    wininfo="$(xwininfo -id "${wid}" 2>/dev/null)" || {
        _log "  STEP3: xwininfo -id ${wid} failed"
        return 1
    }

    win_x="$(printf '%s\n' "${wininfo}" | awk '/Absolute upper-left X:/{print $NF}')"
    win_y="$(printf '%s\n' "${wininfo}" | awk '/Absolute upper-left Y:/{print $NF}')"

    if [[ -z "${win_x}" || -z "${win_y}" ]]; then
        _log "  STEP3: could not parse geometry from xwininfo output"
        return 1
    fi

    local click_x click_y
    click_x=$(( ${win_x} + rel_x ))
    click_y=$(( ${win_y} + rel_y ))

    if [[ ${click_x} -le 0 || ${click_y} -le 0 ]]; then
        _log "  STEP3: computed coords invalid (${click_x},${click_y}) — skipping click"
        return 1
    fi

    _log "  STEP3: xwininfo winXY=(${win_x},${win_y}) rel=(${rel_x},${rel_y}) clickAbs=(${click_x},${click_y})"
    xdotool mousemove "${click_x}" "${click_y}" 2>/dev/null || true
    xdotool click 1 2>/dev/null || true
    return 0
}

# ---------------------------------------------------------------------------
# CLOSE SEQUENCE FOR ONE POPUP WINDOW
# Returns 0 if window is gone after attempts, 1 if still present.
# ---------------------------------------------------------------------------
_close_window() {
    local wid="${1:-}" title="${2:-}" pid="${3:-}"

    # Convert wmctrl hex wid to decimal for xdotool (xdotool accepts both,
    # but explicit decimal avoids any parsing edge cases).
    local wid_dec
    wid_dec="$(printf '%d' "${wid}" 2>/dev/null)" || wid_dec="${wid}"

    _log "CLOSE: wid=${wid} wid_dec=${wid_dec} pid=${pid} title='${title}'"

    # --- STEP 1: wmctrl polite close (WM_DELETE_WINDOW) ---
    _log "  STEP1: wmctrl -ic ${wid}"
    wmctrl -ic "${wid}" 2>/dev/null || true
    sleep "${SETTLE_WAIT}"

    _csv_row "${wid}" "${title}" "${pid}" "wmctrl_ic" "attempted"
    if ! _window_exists "${wid}" "${title}"; then
        _log "  STEP1: window closed OK"
        _csv_row "${wid}" "${title}" "${pid}" "wmctrl_ic" "closed"
        return 0
    fi
    _log "  STEP1: window still present — proceeding to STEP2"

    # --- STEP 2: xdotool activate + key sequence ---
    _log "  STEP2: xdotool windowactivate --sync ${wid_dec}"
    xdotool windowactivate --sync "${wid_dec}" 2>/dev/null || true
    xdotool key Escape  2>/dev/null || true
    xdotool key Return  2>/dev/null || true
    xdotool key alt+F4  2>/dev/null || true
    sleep "${SETTLE_WAIT}"

    _csv_row "${wid}" "${title}" "${pid}" "xdotool_keys" "attempted"
    if ! _window_exists "${wid}" "${title}"; then
        _log "  STEP2: window closed OK"
        _csv_row "${wid}" "${title}" "${pid}" "xdotool_keys" "closed"
        return 0
    fi
    _log "  STEP2: window still present — proceeding to STEP3"

    # --- STEP 3: relative click via xwininfo geometry ---
    if _relative_click "${wid}" "${title}"; then
        sleep "${SETTLE_WAIT}"
        _csv_row "${wid}" "${title}" "${pid}" "relative_click" "attempted"
        if ! _window_exists "${wid}" "${title}"; then
            _log "  STEP3: window closed OK"
            _csv_row "${wid}" "${title}" "${pid}" "relative_click" "closed"
            return 0
        fi
        _log "  STEP3: window still present after click"
        _csv_row "${wid}" "${title}" "${pid}" "relative_click" "still_present"
    fi

    _log "  all steps exhausted — window still present"
    _csv_row "${wid}" "${title}" "${pid}" "all_steps" "still_present"
    return 1
}

# ---------------------------------------------------------------------------
# MAIN WATCH LOOP
# Runs for up to WATCH_DURATION seconds.
# Exits 0 after MIN_CLEAN_STREAK consecutive clean iterations (no popups).
# Exits 25 if popups persist at deadline.
# ---------------------------------------------------------------------------
_log "Watch loop start: duration=${WATCH_DURATION}s interval=${WATCH_INTERVAL}s clean_streak_needed=${MIN_CLEAN_STREAK}"

deadline=$(( SECONDS + WATCH_DURATION ))
iter=0
clean_streak=0

while [[ ${SECONDS} -lt ${deadline} ]]; do
    iter=$(( iter + 1 ))
    remaining=$(( deadline - SECONDS ))
    _log "--- iter=${iter} remaining=${remaining}s clean_streak=${clean_streak} ---"

    # Enumerate current windows.
    local_raw="$(wmctrl -lp 2>/dev/null)" || {
        _log "WARN: wmctrl -lp failed (iter=${iter}) — will retry"
        clean_streak=0
        sleep "${WATCH_INTERVAL}"
        continue
    }

    found_any=false

    # Parse each window line: wid desktop pid host title
    while IFS= read -r line; do
        [[ -z "${line}" ]] && continue

        wid="$(   printf '%s' "${line}" | awk '{print $1}')"
        pid="$(   printf '%s' "${line}" | awk '{print $3}')"
        title="$( printf '%s' "${line}" | awk '{$1=$2=$3=$4=""; sub(/^[[:space:]]+/,""); print}')"

        # Skip if title does not match any popup token.
        matched_tok="$(_title_matches_token "${title}")" || continue

        # Skip if PID does not belong to a TWS process.
        if ! _is_tws_pid "${pid}"; then
            _log "SKIP: wid=${wid} pid=${pid} not in TWS PID set — title='${title}'"
            continue
        fi

        found_any=true
        _log "POPUP DETECTED: wid=${wid} pid=${pid} token='${matched_tok}' title='${title}'"
        _csv_row "${wid}" "${title}" "${pid}" "detected" "found"

        _close_window "${wid}" "${title}" "${pid}" || true

    done <<< "${local_raw}"

    if [[ "${found_any}" == "false" ]]; then
        clean_streak=$(( clean_streak + 1 ))
        _log "Clean iteration (streak=${clean_streak}/${MIN_CLEAN_STREAK})"
        if [[ ${clean_streak} -ge ${MIN_CLEAN_STREAK} ]]; then
            _log "Clean streak reached — declaring OK"
            wmctrl -lp > "${EVIDENCE_DIR}/windows_after.txt" 2>/dev/null || true
            _write_result_json "${EXIT_OK}" "OK"
            _log "=== POPUP CONTROLLER DONE: OK (clean streak) ==="
            exit "${EXIT_OK}"
        fi
    else
        clean_streak=0
    fi

    sleep "${WATCH_INTERVAL}"
done

# ---------------------------------------------------------------------------
# TIMEOUT EXPIRED — FINAL CHECK
# ---------------------------------------------------------------------------
_log "Watch timeout (${WATCH_DURATION}s) expired — performing final check"

raw_final="$(wmctrl -lp 2>/dev/null)" || raw_final=""
wmctrl -lp > "${EVIDENCE_DIR}/windows_after.txt" 2>/dev/null || true

remaining_count=0
while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    wid="$(   printf '%s' "${line}" | awk '{print $1}')"
    pid="$(   printf '%s' "${line}" | awk '{print $3}')"
    title="$( printf '%s' "${line}" | awk '{$1=$2=$3=$4=""; sub(/^[[:space:]]+/,""); print}')"
    _title_matches_token "${title}" >/dev/null 2>&1 || continue
    _is_tws_pid "${pid}"            || continue
    remaining_count=$(( remaining_count + 1 ))
    _log "STILL_PRESENT: wid=${wid} pid=${pid} title='${title}'"
    _csv_row "${wid}" "${title}" "${pid}" "final_check" "still_present"
done <<< "${raw_final}"

if [[ ${remaining_count} -gt 0 ]]; then
    _log "FAIL: ${remaining_count} blocking popup(s) remain after ${WATCH_DURATION}s"
    _write_result_json "${EXIT_POPUP_STILL_PRESENT}" "POPUP_STILL_PRESENT"
    _log "=== POPUP CONTROLLER DONE: POPUP_STILL_PRESENT ==="
    exit "${EXIT_POPUP_STILL_PRESENT}"
fi

_log "Final check: clean"
_write_result_json "${EXIT_OK}" "OK"
_log "=== POPUP CONTROLLER DONE: OK (timeout clean) ==="
exit "${EXIT_OK}"
