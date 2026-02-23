# RUNBOOK: Host X11 + IBKR Boundary Verification (Outside Sandbox)

Run this only from a normal host terminal session (not agent/sandbox command runner).

## Preconditions Checklist
- Repo is present at `/home/n-b/Octa`.
- Host venv exists: `/home/n-b/Octa/.venv_host`.
- `Xvfb` is installed (`command -v Xvfb`).
- `wmctrl` is installed (`command -v wmctrl`).
- `xdpyinfo` is installed (`command -v xdpyinfo`).

## One Copy/Paste Block
```bash
set -euo pipefail

cd /home/n-b/Octa

if [[ ! -d .venv_host ]]; then
  echo "MISSING: .venv_host. Fix: python3 -m venv .venv_host && source .venv_host/bin/activate"
  exit 2
fi
source .venv_host/bin/activate

for c in python timeout Xvfb xdpyinfo wmctrl git; do
  if ! command -v "$c" >/dev/null 2>&1; then
    echo "MISSING: $c. Fix: install '$c' on host and rerun."
    exit 2
  fi
done

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="octa/var/evidence/v001_host_x11_ibkr_verify_${TS}"
mkdir -p "$OUT"

Xvfb_PID=""
cleanup() {
  if [[ -n "${Xvfb_PID}" ]] && kill -0 "${Xvfb_PID}" >/dev/null 2>&1; then
    kill "${Xvfb_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

{
  echo "ts_utc=${TS}"
  uname -a
  id
  python -V
  git rev-parse HEAD
  pwd
} > "$OUT/host_context.txt" 2>&1

set +e
python - <<'PY' > "$OUT/socket_probe.log" 2>&1
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print("SOCKET_OK", s.getsockname())
s.close()
PY
SOCKET_RC=$?
set -e
echo "$SOCKET_RC" > "$OUT/socket_probe_exit_code.txt"
if [[ "$SOCKET_RC" -ne 0 ]]; then
  echo "SOCKET_FAIL: see $OUT/socket_probe.log"
fi

pkill -f "Xvfb :99" || true
set +e
Xvfb :99 -screen 0 1280x720x24 -nolisten tcp -ac > "$OUT/xvfb_start.log" 2>&1 &
Xvfb_PID=$!
sleep 2
if ! kill -0 "$Xvfb_PID" >/dev/null 2>&1; then
  wait "$Xvfb_PID" || true
fi
set -e

export DISPLAY=:99
echo "$DISPLAY" > "$OUT/display.txt"

set +e
timeout 15s xdpyinfo > "$OUT/xdpyinfo.log" 2>&1
X11_RC=$?
set -e
echo "$X11_RC" > "$OUT/x11_probe_exit_code.txt"
if [[ "$X11_RC" -eq 0 ]]; then
  echo "X11_OK" | tee "$OUT/x11_probe.txt"
else
  echo "X11_FAIL" | tee "$OUT/x11_probe.txt"
  echo "X11_FAIL: Xvfb/xdpyinfo preflight failed; see $OUT/xvfb_start.log and $OUT/xdpyinfo.log"
  exit 2
fi

set +e
timeout 30s wmctrl -lpG > "$OUT/wmctrl_windows.log" 2>&1
WMCTRL_RC=$?
set -e
echo "$WMCTRL_RC" > "$OUT/wmctrl_exit_code.txt"

set +e
timeout 240s python scripts/tws_x11_autologin_chain.py --config configs/execution_ibkr.yaml --timeout-sec 180 > "$OUT/autologin.log" 2>&1
AUTOLOGIN_RC=$?
set -e
echo "$AUTOLOGIN_RC" > "$OUT/autologin_exit_code.txt"

set +e
timeout 300s python scripts/octa_smoke_chain.py --autopilot-config configs/dev.yaml --limit 5 > "$OUT/live_smoke.log" 2>&1
SMOKE_RC=$?
set -e
echo "$SMOKE_RC" > "$OUT/live_smoke_exit_code.txt"

set +e
timeout 300s python scripts/octa_smoke_chain.py --autopilot-config configs/dev.yaml --limit 5 --offline > "$OUT/offline_smoke.log" 2>&1
OFFLINE_RC=$?
set -e
echo "$OFFLINE_RC" > "$OUT/offline_smoke_exit_code.txt"

SOCKET_FLAG="FAIL"
X11_FLAG="FAIL"
AUTOLOGIN_FLAG="FAIL"
SMOKE_FLAG="FAIL_CLOSED"
OFFLINE_FLAG="FAIL"

[[ "$SOCKET_RC" -eq 0 ]] && SOCKET_FLAG="PASS"
[[ "$X11_RC" -eq 0 ]] && X11_FLAG="PASS"
[[ "$AUTOLOGIN_RC" -eq 0 ]] && AUTOLOGIN_FLAG="PASS"
[[ "$SMOKE_RC" -eq 0 ]] && SMOKE_FLAG="PASS"
[[ "$SMOKE_RC" -eq 124 ]] && SMOKE_FLAG="TIMEOUT"
[[ "$OFFLINE_RC" -eq 0 ]] && OFFLINE_FLAG="PASS"

{
  echo "OUT=$OUT"
  echo "SOCKET=${SOCKET_FLAG} (rc=${SOCKET_RC})"
  echo "X11=${X11_FLAG} (rc=${X11_RC})"
  echo "AUTOLOGIN=${AUTOLOGIN_FLAG} (rc=${AUTOLOGIN_RC})"
  echo "LIVE_SMOKE=${SMOKE_FLAG} (rc=${SMOKE_RC})"
  echo "OFFLINE_SMOKE=${OFFLINE_FLAG} (rc=${OFFLINE_RC})"
} | tee "$OUT/summary.txt"

echo "Evidence written to: $OUT"
```

## Interpreting Results
- `SOCKET_OK` is required. If missing, host runtime is still restricted.
- `X11_OK` is required for GUI autologin checks.
- Live smoke status:
  - `PASS` means smoke completed successfully (`rc=0`).
  - `FAIL_CLOSED` means non-zero non-timeout exit (`rc!=0 && rc!=124`), which can be acceptable only when `live_smoke.log` shows an explicit fail-closed IBKR boundary condition.
  - Confirm fail-closed boundary by checking `live_smoke.log` for explicit boundary/error classification (not a hang).
  - `TIMEOUT` (`rc=124`) is a failure and indicates the run did not finish within the bound.
- Optional offline smoke status:
  - `PASS` means offline chain completed (`rc=0`).
  - `FAIL` means the offline chain failed (`rc!=0`); inspect `offline_smoke.log`.
- `PermissionError: [Errno 1] Operation not permitted` must not occur outside sandbox host terminal.

## If It Fails
- Socket probe fails:
  - You are likely still in a restricted/sandboxed runner. Move to a normal host terminal and rerun.
- Xvfb fails to bind:
  - Check for existing X server on `:99`, permissions on `/tmp` and `/tmp/.X11-unix`, and host policy restrictions.
- `xdpyinfo` fails:
  - `DISPLAY` is wrong or Xvfb did not start successfully. Inspect `display.txt`, `xvfb_start.log`, `xdpyinfo.log`.
- Autologin fails:
  - Inspect `autologin.log`, `wmctrl_windows.log`, and any evidence under `octa/var/evidence/tws_x11_autologin_*`.
  - Validate expected window titles in `wmctrl_windows.log` for login/main/popup matching.
