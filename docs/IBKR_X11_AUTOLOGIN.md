# IBKR X11 Autologin

Automated TWS/IB Gateway startup, login, and disclaimer acceptance under X11.

## Architecture

Three layers, each a separate systemd service:

```
octa-x11.service         Xvfb :99 display server
    |
octa-ibkr.service        TWS/Gateway process lifecycle + health loop
    |
octa-autologin.service   Login step-machine + popup watcher
```

### Login step-machine (`octa.execution.ibkr_x11_login`)

Runs once per boot cycle, in strict deterministic order:

| # | State | Description |
|---|-------|-------------|
| 1 | `VERIFY_X11` | Confirm X11 display reachable via xdpyinfo |
| 2 | `START_TWS` | Ensure TWS process is launched (or already running) |
| 3 | `WAIT_LOGIN_WINDOW` | Wait for login window (xdotool search with backoff) |
| 4 | `INJECT_CREDENTIALS` | Focus window, TAB-navigate, type user/pass, Enter |
| 5 | `WAIT_POST_LOGIN` | Wait for login window to disappear |
| 6 | `WAIT_DISCLAIMER` | Wait for disclaimer/agreement dialog |
| 7 | `ACCEPT_DISCLAIMER` | Accept via TAB+Enter, fallback: window-relative click |
| 8 | `VERIFY_STABLE` | Confirm TWS stays alive for 10s |

Each state: PASS or FAIL. On any FAIL -> screenshot + evidence -> exit nonzero -> systemd restarts.

### Popup watcher (`octa.execution.ibkr_x11_autologin`)

Long-running daemon that handles recurring popups (reconnect dialogs, periodic disclaimers) using taught profiles stored in SQLite.

### Health loop (`scripts/octa_ibkr_bootstrap.sh`)

Polls TWS process alive + API port reachable. Has a **startup grace period** (default 180s) where process-alive alone is sufficient, allowing time for login and disclaimer.

### Python interpreter

Both bootstrap scripts define:
```bash
REPO="/home/n-b/Octa"
PY="${OCTA_PY:-${REPO}/.venv/bin/python}"
```
**No bare `python` or `/usr/bin/env python` is ever used for module calls.**
If `$PY` does not exist or is not executable, the script exits 1 immediately with a JSON diagnostic.

## Environment Variables

### Required
| Variable | Description |
|----------|-------------|
| `OCTA_REPO` | Absolute path to Octa repo (default: `/home/n-b/Octa`) |
| `OCTA_PY` | Absolute path to venv python (default: `$REPO/.venv/bin/python`) |

### IBKR Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `OCTA_IBKR_MODE` | `tws` | `tws` or `gateway` |
| `OCTA_TWS_CMD` | *(auto-detected)* | Path to TWS launcher. Auto-resolves `~/Jts/tws` if unset |
| `OCTA_GATEWAY_CMD` | *(auto-detected)* | Path to Gateway launcher |
| `OCTA_IBKR_HOST` | `127.0.0.1` | API host |
| `OCTA_IBKR_PORT` | `7497` | API port |
| `OCTA_IBKR_STARTUP_GRACE_SEC` | `180` | Seconds to tolerate port-down during startup |
| `OCTA_IBKR_HEALTH_INTERVAL_SEC` | `5` | Health poll interval |
| `OCTA_IBKR_DB` | `octa/var/runtime/ibkr_autologin.sqlite3` | Profile database |

### Credentials (NEVER stored in git)
| Variable | Description |
|----------|-------------|
| `OCTA_IBKR_USER` | IBKR username |
| `OCTA_IBKR_PASS` | IBKR password |
| `OCTA_IBKR_SECRETS_FILE` | Alternative: path to file with line1=user, line2=pass |
| `OCTA_IBKR_LOGIN_TIMEOUT_SEC` | Login step timeout (default 120) |

If neither `OCTA_IBKR_USER` nor `OCTA_IBKR_SECRETS_FILE` is set, the login step-machine is skipped.

### X11 / Display
| Variable | Default | Description |
|----------|---------|-------------|
| `DISPLAY` | `:99` | X11 display |
| `OCTA_XVFB_DISPLAY` | `:99` | Xvfb display (fallback) |

## Setup

### 1. Env file (`~/.config/octa/env`)

```bash
mkdir -p ~/.config/octa
cat > ~/.config/octa/env <<'EOF'
OCTA_REPO=/home/n-b/Octa
OCTA_PY=/home/n-b/Octa/.venv/bin/python
OCTA_XVFB_DISPLAY=:99
OCTA_IBKR_MODE=tws

# Credentials (choose one):
# Option A: env vars
OCTA_IBKR_USER=your_username
OCTA_IBKR_PASS=your_password
# Option B: secrets file
# OCTA_IBKR_SECRETS_FILE=/home/n-b/.config/octa/ibkr_secrets
EOF
chmod 600 ~/.config/octa/env
```

### 2. Install systemd services

```bash
mkdir -p ~/.config/systemd/user
cp systemd/*.service systemd/octa.target ~/.config/systemd/user/
systemctl --user daemon-reload
loginctl enable-linger "$USER"
```

### 3. Teach disclaimer profile (one-time, for popup watcher)

On a real display with TWS open showing the disclaimer:
```bash
DISPLAY=:99 .venv/bin/python scripts/octa_ibkr_teach.py --teach --profile-name tws_disclaimer
```

### 4. Start

```bash
systemctl --user enable --now octa.target
```

## Manual testing

### Dry-run the login step-machine
```bash
OCTA_IBKR_USER=test OCTA_IBKR_PASS=test DISPLAY=:99 \
  /home/n-b/Octa/.venv/bin/python -m octa.execution.ibkr_x11_login --dry-run --timeout-sec 30
```

### Run unit tests
```bash
/home/n-b/Octa/.venv/bin/python -m pytest tests/test_ibkr_x11_login_steps.py -v
/home/n-b/Octa/.venv/bin/python -m pytest tests/test_bootstrap_no_bare_python.py -v
```

### Check service status
```bash
systemctl --user status octa-ibkr.service --no-pager -l
systemctl --user status octa-autologin.service --no-pager -l
journalctl --user -u octa-ibkr.service -n 100 --no-pager -l
journalctl --user -u octa-autologin.service -n 100 --no-pager -l
```

### Check API port
```bash
ss -ltnp | grep 7497 || echo "API port not open yet"
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `python: Befehl nicht gefunden` | Bare `python` in systemd | Fixed: scripts use absolute `$PY` path |
| `python_not_executable` | `$PY` not found or not executable | Check `~/.config/octa/env` has correct `OCTA_PY` |
| `x11_not_reachable` | Display not available | Check `xdpyinfo -display :99` |
| `login_window_not_found` | TWS not started or different title | Verify TWS starts on `:99` |
| `no_credentials_configured` | No env vars or secrets file | Set `OCTA_IBKR_USER`/`OCTA_IBKR_PASS` |
| `port_reachable: false` during grace | Normal during startup | Wait up to 180s |
| `no_profiles_configured` | Popup watcher has no profiles | Run teach step (section 3 above) |

## Evidence

Every run produces evidence under `octa/var/evidence/`:
- `run.json` — state-by-state results with timestamps and status
- `sha256.txt` — integrity hash of run.json
- `events.jsonl` — append-only event log
- `failure_*.png` — screenshots on failure (if ImageMagick `import` available)

## Security

- Passwords are NEVER logged in evidence files
- Credentials come ONLY from env vars or an external secrets file
- The secrets file should be `chmod 600` and outside the git repo
- No `shell=True` — all subprocess calls use explicit argument lists
