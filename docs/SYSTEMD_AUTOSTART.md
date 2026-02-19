# OCTA Systemd Autostart (User Services)

## 1) Create env file
```bash
mkdir -p ~/.config/octa
cat > ~/.config/octa/env <<'EOF'
OCTA_REPO=/absolute/path/to/Octa
OCTA_PY=/absolute/path/to/Octa/.venv/bin/python
OCTA_USE_XVFB=1
OCTA_REQUIRE_X11=1
OCTA_XVFB_DISPLAY=:99
OCTA_XVFB_SCREEN=1920x1080x24
OCTA_XVFB_DPI=96
OCTA_IBKR_MODE=tws
OCTA_TWS_CMD=/path/to/tws/start/script/or/binary
OCTA_GATEWAY_CMD=/path/to/gateway/start/script/or/binary
OCTA_IBKR_DB=octa/var/runtime/ibkr_autologin.sqlite3
OCTA_IBKR_HOST=127.0.0.1
OCTA_IBKR_PORT=7497
OCTA_V000_CONFIG=configs/dev.yaml
EOF
```

For boot without interactive login:
```bash
loginctl enable-linger "$USER"
```

## 2) Install user services
```bash
mkdir -p ~/.config/systemd/user
cp systemd/*.service systemd/octa.target ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now octa.target
```

## 3) Logs
```bash
journalctl --user -u octa-ibkr.service -f
journalctl --user -u octa-autologin.service -f
journalctl --user -u octa-v000.service -f
```

## 4) Restart / Stop
```bash
systemctl --user restart octa.target
systemctl --user disable --now octa.target
```

## 5) Canonical IBKR Bring-Up + Verify
Start broker runtime (paper/shadow setup):
```bash
systemctl --user restart octa-ibkr.service
```

Verify API readiness with the supervisor (no-skip):
```bash
python scripts/octa_ibkr_supervisor.py --config configs/execution_ibkr.yaml --no-skip --timeout-sec 60
```

Stop broker runtime:
```bash
systemctl --user stop octa-ibkr.service
```

## Notes
- Services are fail-closed and restart on failure (`Restart=always`, `RestartSec=2`).
- No password automation is implemented.
- X11 popup auto-click is opt-in and controlled by runtime env + profile DB.
- Execution defaults are unchanged; this setup does not enable live trading by itself.

## Known-good Xvfb env
```bash
OCTA_XVFB_DISPLAY=:99
OCTA_XVFB_SCREEN=1920x1080x24
OCTA_XVFB_DPI=96
```

## Host prerequisites
- `Xvfb` (required)
- `xdpyinfo` (optional but recommended)
- `xdotool` and `xprop` (required for teach/run popup automation)

## Broker Mode Runbook

### Gateway mode (preferred headless)
- Confirm gateway binary exists:
```bash
ls -l /home/n-b/Jts/ibgateway/1041/ibgateway
```
- Use explicit launch config in `configs/execution_ibkr.yaml` (`launch.providers[0].provider=direct`).
- Required API settings in IB Gateway:
  - Enable API socket clients.
  - Paper API port matches config (commonly `4002`).
  - Allow localhost / trusted IPs.
  - Ensure clientId is not in conflict.

### TWS mode (requires real X11 session)
- Ensure an X11 display is reachable:
```bash
xset q
xdpyinfo -display "${DISPLAY}"
```
- If not using active desktop X11, configure Xvfb and `DISPLAY`/`XAUTHORITY` explicitly.
- Set `ibkr.mode=tws` and provide a valid TWS executable in launch command.
