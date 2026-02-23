#!/usr/bin/env bash
set -euo pipefail

cd ~/Octa
source .venv_host/bin/activate
export PYTHONPATH="$(pwd)"

export DISPLAY=:0
AUTH="$(ps -u "$USER" -o cmd= | sed -n 's/.*Xwayland :0 .* -auth \([^ ]*\).*/\1/p' | head -n1)"
if [[ -z "${AUTH:-}" || ! -f "$AUTH" ]]; then
  echo "ERROR: XAUTHORITY not found from Xwayland"
  ps -u "$USER" -o pid,cmd | egrep -i 'Xwayland|Xorg' || true
  exit 11
fi
export XAUTHORITY="$AUTH"

if ! xdpyinfo -display :0 >/dev/null 2>/tmp/tws_xdpyinfo.err; then
  cat /tmp/tws_xdpyinfo.err || true
  exit 11
fi
if ! xset q >/dev/null 2>/tmp/tws_xset.err; then
  cat /tmp/tws_xset.err || true
  exit 11
fi
if ! wmctrl -m >/dev/null 2>/tmp/tws_wmctrlm.err; then
  cat /tmp/tws_wmctrlm.err || true
  exit 11
fi

set -a
source ~/.config/octa/ibkr.env
set +a

TOKENS='Warnhinweis|Disclaimer|Login Messages|Login Message|Messages|Login Messenger|IBKR Login Messenger|Messenger|Börsenspiegel|Boersenspiegel|Programm wird geschlossen|win0'
set +e
python scripts/tws_x11_autologin_chain.py --config configs/execution_ibkr.yaml --timeout-sec 180
base_rc=$?
set -e

for iter in $(seq 1 20); do
  if ! wmctrl -lp >/tmp/tws_wmctrl_iter.txt 2>/tmp/tws_wmctrl_iter.err; then
    echo "[tws-autologin] FAIL: wmctrl unreachable during assert loop"
    cat /tmp/tws_wmctrl_iter.err || true
    exit 11
  fi
  remain="$(egrep -i "$TOKENS" /tmp/tws_wmctrl_iter.txt || true)"
  if [[ -z "$remain" ]]; then
    echo "[tws-autologin] SUCCESS: no blocking popups after base run/drain"
    exit 0
  fi
  echo "[tws-autologin] drain-only pass $iter/20"
  set +e
  python scripts/tws_x11_autologin_chain.py --drain-only --config configs/execution_ibkr.yaml --timeout-sec 180
  drain_rc=$?
  set -e
  if [[ $drain_rc -eq 11 ]]; then
    exit 11
  fi
  sleep 0.35
done

# ---------------------------------------------------------------------------
# Deterministic popup controller — supplementary shell-level watcher.
# Runs after the Python drain loop. Handles late-appearing popups via a
# strict three-step sequence: wmctrl -ic → xdotool keys → relative click.
# RC 25 from the controller is non-fatal here; the final assert below decides.
# ---------------------------------------------------------------------------
set +e
bash octa/support/x11/tws_popup_controller.sh
_ctrl_rc=$?
set -e
echo "[tws-autologin] popup controller exited rc=${_ctrl_rc}"
if [[ ${_ctrl_rc} -eq 10 ]]; then
  echo "[tws-autologin] FAIL: popup controller X11 precheck failed"
  exit 11
fi

if ! wmctrl -lp >/tmp/tws_wmctrl_final.txt 2>/tmp/tws_wmctrl_final.err; then
  echo "[tws-autologin] FAIL: wmctrl unreachable at final assert"
  cat /tmp/tws_wmctrl_final.err || true
  exit 11
fi
final_remain="$(egrep -i "$TOKENS" /tmp/tws_wmctrl_final.txt || true)"
if [[ -n "$final_remain" ]]; then
  echo "[tws-autologin] FAIL: blocking popups still present after drain loop"
  printf '%s\n' "$final_remain"
  exit 25
fi

exit 0
