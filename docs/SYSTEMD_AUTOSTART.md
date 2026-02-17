# OCTA Systemd Autostart (User Services)

## 1) Create env file
```bash
mkdir -p ~/.config/octa
cat > ~/.config/octa/env <<'EOF'
OCTA_REPO=/absolute/path/to/Octa
OCTA_PY=/absolute/path/to/Octa/.venv/bin/python
OCTA_USE_XVFB=1
OCTA_REQUIRE_X11=1
OCTA_IBKR_MODE=tws
OCTA_TWS_CMD=/path/to/tws/start/script/or/binary
OCTA_GATEWAY_CMD=/path/to/gateway/start/script/or/binary
OCTA_IBKR_DB=octa/var/runtime/ibkr_autologin.sqlite3
OCTA_IBKR_HOST=127.0.0.1
OCTA_IBKR_PORT=7497
OCTA_V000_CONFIG=configs/dev.yaml
EOF
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

## Notes
- Services are fail-closed and restart on failure (`Restart=always`, `RestartSec=2`).
- No password automation is implemented.
- X11 popup auto-click is opt-in and controlled by runtime env + profile DB.
- Execution defaults are unchanged; this setup does not enable live trading by itself.
