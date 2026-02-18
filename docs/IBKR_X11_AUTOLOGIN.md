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

Each state: PASS or FAIL. A screenshot is captured **after every state transition** (both PASS and FAIL) for forensic review. On any FAIL -> exit nonzero -> systemd restarts.

### Popup watcher (`octa.execution.ibkr_x11_autologin`)

Long-running daemon that handles recurring popups (reconnect dialogs, periodic disclaimers) using taught profiles stored in SQLite.

### Health loop (`scripts/octa_ibkr_bootstrap.sh`)

Polls TWS process alive + API port reachable. Has a **startup grace period** (default 180s) where process-alive alone is sufficient, allowing time for login and disclaimer.

### Env file loading

Both bootstrap scripts source `~/.config/octa/env` before anything else:
```bash
OCTA_ENV_FILE="${HOME}/.config/octa/env"
if [[ -f "${OCTA_ENV_FILE}" ]]; then
  _perms="$(stat -c '%a' "${OCTA_ENV_FILE}")"
  if [[ "${_perms}" != "600" ]] && [[ "${_perms}" != "400" ]]; then
    # FAIL-CLOSED: refuse to start with insecure permissions
    exit 1
  fi
  octa_source_env_strict "${OCTA_ENV_FILE}"
fi
```
The env file **must** be `chmod 600` or `chmod 400`. If it is group- or world-readable, the script exits 1 with `env_file_insecure_permissions` and refuses to start.

The env file must contain only assignment lines:
- `KEY=VALUE`
- `export KEY=VALUE`

Any other non-empty/non-comment line is rejected fail-closed with `env_invalid_line` (includes line number and a redacted preview).

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
- `<boot_dir>/octa_ibkr_bootstrap/run.json` + `events.jsonl`
- `<boot_dir>/octa_autologin_bootstrap/run.json` + `events.jsonl`
- `<boot_dir>/*/python_stderr.log` for captured Python stderr and `--help` evidence on rc=2
- `login_steps/*.png` screenshots from the login step-machine (when executed)

## WAY A vs WAY B

### WAY A (production, default)

- Leave `OCTA_DEBUG_VISIBLE` unset (or set `0`).
- Bootstrap scripts use headless Xvfb on `:99` by default.
- This mode remains unchanged for unattended operation.

### WAY B (visible desktop debug)

When `OCTA_DEBUG_VISIBLE=1`, display selection is:
1. `OCTA_DEBUG_DISPLAY` (if set)
2. fallback `:0`

Visible mode is fail-closed if selected display is `:99` (treated as Xvfb misconfig).

`XAUTHORITY` is set to `${OCTA_DEBUG_XAUTHORITY:-$HOME/.Xauthority}` in visible mode.

Each run records deterministic X11 diagnostics:
- chosen `DISPLAY`
- `XAUTHORITY` existence + owner + mode
- `xdpyinfo` success/failure
- `xset q` (or fallback `xprop -root`) success/failure

Known permanent visible-mode misconfig (missing `XAUTHORITY`, unreachable display) fails closed with nonzero exit after a short backoff to avoid rapid StartLimitHit loops.

## How To Run WAY B

Run these checks in an active desktop terminal (not plain SSH):

```bash
echo "DISPLAY=$DISPLAY"
test "$DISPLAY" = ":0" -o "$DISPLAY" = ":1"
```

Wayland/Xwayland: inspect the active Xauthority and set `OCTA_DEBUG_XAUTHORITY` if needed:

```bash
xauth info
set OCTA_DEBUG_DISPLAY=:0
set OCTA_DEBUG_XAUTHORITY=<authority file>
```

Run `xhost` from that same desktop terminal:

```bash
xhost +SI:localuser:$USER
```

If `~/.Xauthority` is missing, point it to the real session authority file:

```bash
AUTH_CANDIDATE="$(find /run/user/$(id -u) -maxdepth 5 -type f -name Xauthority 2>/dev/null | head -n1)"
echo "$AUTH_CANDIDATE"
[ -n "$AUTH_CANDIDATE" ] || { echo "No session Xauthority found under /run/user/$(id -u)"; exit 1; }
ln -sfn "$AUTH_CANDIDATE" "$HOME/.Xauthority"
chmod 600 "$HOME/.Xauthority"
```

Enable visible mode (optionally pin display):

```bash
cat >> ~/.config/octa/env <<'EOF'
OCTA_DEBUG_VISIBLE=1
OCTA_DEBUG_DISPLAY=:0
# Optional override if desktop authority is not ~/.Xauthority:
# OCTA_DEBUG_XAUTHORITY=/run/user/1000/<session>/Xauthority
EOF
```

Reset failed state and restart:

```bash
systemctl --user stop octa-autologin.service octa-ibkr.service
systemctl --user reset-failed octa-autologin.service octa-ibkr.service
systemctl --user start octa-ibkr.service octa-autologin.service
```

Confirm chosen display and X11 checks in logs:

```bash
journalctl --user -u octa-ibkr.service -n 120 --no-pager -l | rg 'x11_diag|display|ibkr_error'
journalctl --user -u octa-autologin.service -n 120 --no-pager -l | rg 'x11_diag|display|autologin_error'
```

Evidence location:

```bash
BOOT_DIR="$(cat /home/n-b/Octa/octa/var/runtime/systemd_boot_dir)"
echo "$BOOT_DIR"
ls -l \
  "$BOOT_DIR"/octa_ibkr_bootstrap/run.json \
  "$BOOT_DIR"/octa_ibkr_bootstrap/events.jsonl \
  "$BOOT_DIR"/octa_ibkr_bootstrap/python_stderr.log \
  "$BOOT_DIR"/octa_autologin_bootstrap/run.json \
  "$BOOT_DIR"/octa_autologin_bootstrap/events.jsonl \
  "$BOOT_DIR"/octa_autologin_bootstrap/python_stderr.log
```

Disable WAY B and return to WAY A:

```bash
sed -i '/^OCTA_DEBUG_VISIBLE=/d;/^OCTA_DEBUG_DISPLAY=/d;/^OCTA_DEBUG_XAUTHORITY=/d' ~/.config/octa/env
systemctl --user reset-failed octa-autologin.service octa-ibkr.service
systemctl --user restart octa-ibkr.service octa-autologin.service
```

## Foreground Reproduction (No systemd)

Deterministic foreground run for autologin debugging:

```bash
set -a; . ~/.config/octa/env; set +a
export OCTA_DEBUG_VISIBLE=1
export OCTA_DEBUG_DISPLAY=:0
# Optional: export OCTA_DEBUG_XAUTHORITY=/run/user/1000/<session>/Xauthority
/home/n-b/Octa/scripts/octa_autologin_bootstrap.sh
```

Foreground run for IBKR bootstrap:

```bash
env -i HOME=/home/n-b PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  DISPLAY=:0 XAUTHORITY=/path/to/authority OCTA_DEBUG_VISIBLE=1 \
  bash /home/n-b/Octa/scripts/octa_ibkr_bootstrap.sh
```

### Env var reference

| Variable | Default | Description |
|----------|---------|-------------|
| `OCTA_DEBUG_VISIBLE` | `0` | Set `1` to enable visible desktop debug mode |
| `OCTA_DEBUG_DISPLAY` | `:0` | Explicit display for visible mode (`:0` or `:1`) |
| `OCTA_DEBUG_XAUTHORITY` | `$HOME/.Xauthority` | Optional Xauthority path override for visible mode |
| `OCTA_MISCONFIG_BACKOFF_SEC` | `7` | Backoff before exiting on permanent visible-mode misconfig |

## Security

- Passwords are NEVER logged in evidence files
- Credentials come ONLY from env vars or an external secrets file
- The secrets file should be `chmod 600` and outside the git repo
- No `shell=True` — all subprocess calls use explicit argument lists

## TWS GUI Smoke Test

This test does not download or install anything. It only verifies visible X11 on desktop and starts TWS if already present.

```bash
export OCTA_DEBUG_DISPLAY=:0
export OCTA_DEBUG_XAUTHORITY=$(xauth info | awk -F': ' '/Authority file/ {print $2}')
bash scripts/octa_tws_gui_smoke.sh
```

## Wayland: Session Env Bridge

Import desktop session env into `systemd --user` at graphical session start so GUI services can reach Xwayland.

```bash
systemctl --user daemon-reload
systemctl --user enable --now octa-session-env.service
systemctl --user status octa-session-env.service
```

Verification:

```bash
systemd-run --user --pty -E DISPLAY -E XAUTHORITY -E XDG_RUNTIME_DIR -E WAYLAND_DISPLAY -E DBUS_SESSION_BUS_ADDRESS \
  bash scripts/octa_tws_gui_smoke.sh
```

## Visible Autologin Watcher

Create local-only credentials file (never commit this):

```bash
mkdir -p ~/.config/octa
cat > ~/.config/octa/ibkr_secrets.env <<'EOF'
IBKR_USERNAME=your_ibkr_username
IBKR_PASSWORD=your_ibkr_password
EOF
chmod 600 ~/.config/octa/ibkr_secrets.env
```

`octa-autologin.service` now runs `scripts/octa_ibkr_autologin_watch.py` and requires desktop session env from `octa-session-env.service`.

```bash
systemctl --user daemon-reload
systemctl --user restart octa-session-env.service
systemctl --user restart octa-ibkr.service octa-autologin.service
```

Deterministic foreground repro (single cycle):

```bash
env -i HOME=$HOME PATH=$PATH DISPLAY=:0 XAUTHORITY=$(xauth info | awk -F': ' '/Authority file/ {print $2}') \
  python3 scripts/octa_ibkr_autologin_watch.py --once
```

Verification:

```bash
journalctl --user -u octa-autologin.service -n 200 --no-pager -l | rg 'x11_checks_ok|window_detected|action_click|action_type|action_result|idle_noop|autologin_error'
```

## Visible E2E (Service Mode)

This sequence starts visible TWS and the 24/7 watcher on desktop `:0` via systemd user services:

```bash
systemctl --user daemon-reload
systemctl --user restart octa-session-env.service
systemctl --user restart octa-ibkr.service octa-autologin.service
```

Foreground one-shot watcher repro (single scan/action cycle):

```bash
env -i HOME=$HOME PATH=$PATH DISPLAY=:0 XAUTHORITY=$(xauth info | awk -F': ' '/Authority file/ {print $2}') \
  python3 scripts/octa_ibkr_autologin_watch.py --once
```

Service log verification:

```bash
journalctl --user -u octa-ibkr.service -n 200 --no-pager -l | rg 'tws_|x11_|error'
journalctl --user -u octa-autologin.service -n 200 --no-pager -l | rg 'window_detected|login_attempt|action_|idle_noop|error'
```

Lifecycle and watcher verification:

```bash
systemctl --user daemon-reload
systemctl --user restart octa-session-env.service
systemctl --user restart octa-ibkr.service
systemctl --user restart octa-autologin.service
systemctl --user status octa-ibkr.service octa-autologin.service --no-pager
journalctl --user -u octa-ibkr.service -n 200 --no-pager -l | rg 'bootstrap_|tws_|exec_|exit|error'
journalctl --user -u octa-autologin.service -n 200 --no-pager -l | rg 'window_snapshot|window_detected|login_strategy|action_|idle_noop|error'
```

## 2026-02-17 Reliability Note: IBKR Bootstrap Ownership

`scripts/octa_ibkr_bootstrap.sh` now resolves `TWS_CMD`, emits `tws_cmd_resolved`, then emits `tws_exec_starting` and performs:

```bash
exec "${TWS_CMD}" 1>>tws_stdout.log 2>>tws_stderr.log
```

This makes the service main process become the TWS exec target (no detached launcher lifetime mismatch). On `exec` failure, bootstrap emits `tws_exec_failed` and exits non-zero.

`argparse_or_usage_error` emission is now gated by actual argparse/usage stderr patterns, not by generic rc `2` alone.

## 2026-02-17 Watcher Reliability Update

`scripts/octa_ibkr_autologin_watch.py` now uses scored window classification with `role`, `score`, and `reasons` based on:
- `WM_CLASS` IBKR/JTS allowlist
- `WM_NAME`/`_NET_WM_NAME` login/disclaimer hints
- optional `WM_WINDOW_ROLE`

For each acted-on candidate, `window_detected` includes:
- `wid`
- `wm_class`
- `title`
- `role`
- `score`
- `reasons`
- bounded `xwininfo` geometry summary

Deterministic focus is applied before input:
1. `xdotool windowactivate --sync WID`
2. `xdotool windowraise WID`
3. `xdotool windowfocus --sync WID`
4. short fixed delay

Login sequence is deterministic and secret-safe:
- clear/type username
- tab
- clear/type password
- submit via Return
- logs only field lengths/redacted markers (never credential values)

Disclaimer acceptance uses deterministic ladder:
1. Return
2. Tab x5 then Return
3. Alt+A

Success is recognized only by state change (`window gone` or `title changed`) within grace polling.

Main loop behavior:
- X11 checks every iteration (`xdpyinfo`, `xset q`)
- exponential idle backoff (1s -> 2s -> 4s -> max 8s)
- action retries with bounded grace
- no tight loop

Optional debug mode:

```bash
OCTA_AUTOLOGIN_DEBUG=1
```

When enabled, watcher emits bounded debug snapshots including focused window id and a capped visible window id list.

## 2026-02-17 Relaunch + Stage2 Reliability Hardening

`scripts/octa_ibkr_bootstrap.sh` now hardens TWS launch control with:
- single-instance lock via `flock` on `${XDG_RUNTIME_DIR:-/tmp}/octa_tws.lock`
- pre-launch running-process guard:
  - `pgrep -u "$(id -u)" -fa 'java.*(Jts|Trader Workstation|tws|ibgateway)'`
  - emits `tws_already_running` and exits `0` (no relaunch)
- lock contention emits `tws_lock_busy` and exits `0`
- fresh launches still emit `tws_exec_starting` and use `exec "${TWS_CMD}"` for unit ownership.

`scripts/octa_ibkr_autologin_watch.py` now adds:
- deterministic field-focus ladder for login window:
  1. focus + relative click
  2. focus + `Tab` x1
  3. focus + `Tab` x2
  4. focus + `Shift+Tab` x1
- each ladder step performs clear/type user, tab, clear/type pass, submit Return.
- stage-2 detection (`Verification`, `Sicherheits`, `Code`, `Two-Factor`, `Challenge`, `Security`, `IB Key`, `Authenticator`) with `stage2_detected` event and no credential retyping while stage-2 is active.
- post-submit success accepts:
  - login window gone
  - login title changed
  - disclaimer detected
  - stage2 detected
  - IBKR post-login window detected.

## 2026-02-17 Stage B Disclaimer + Popup Flow

Watcher flow now uses a deterministic stage machine:
- Stage A: login handling (`role=login`)
- Stage B: disclaimer/popup handling (`role=disclaimer` / `role=popup_modal`)
- Stage C: steady-state (`role=main` visible, no modal windows)

Window recognition priority is:
1. `WM_CLASS`
2. `_NET_WM_NAME` / `WM_NAME`
3. `WM_WINDOW_ROLE`

Recognized classes of windows:
- Login: `Anmelden`, `Login`, auth/credential prompts
- Disclaimer/terms: `DISCLAIMER`, `Terms`, `Risk Disclosure`, `Agreements`, accept/continue prompts
- Popup modal: important/update/notice/warning/confirm dialogs
- Main TWS: `Trader Workstation`, `Mosaic`, `Classic TWS`, portfolio/monitor/chart main UI

Disclaimer handling emits:
- `disclaimer_detected`
- `disclaimer_action_start`
- `disclaimer_action_done`
- `popup_closed_ok` (when removed/changed)

Popup handling emits:
- `popup_detected`
- `popup_close_attempt`
- `popup_closed_ok`

Stage transitions emit:
- `state_transition` with `from`, `to`, `reason`

One-shot foreground run (secrets remain in env file, never printed):

```bash
env -i HOME=$HOME PATH=$PATH DISPLAY=:0 XAUTHORITY="$(xauth info | awk -F': ' '/Authority file/ {print $2}')" \
  python3 /home/n-b/Octa/scripts/octa_ibkr_autologin_watch.py --once
```

Verification commands:

```bash
journalctl --user -u octa-autologin.service -n 200 --no-pager -l | rg 'disclaimer_detected|disclaimer_action_|popup_detected|popup_close_attempt|popup_closed_ok|state_transition|steady_state'
BOOT_DIR="$(cat /home/n-b/Octa/octa/var/runtime/systemd_boot_dir)"
tail -n 120 "$BOOT_DIR/octa_ibkr_autologin_watch/events.jsonl" | rg 'disclaimer_|popup_|state_transition|steady_state'
```

## 2026-02-17 Paper Strict Workflow

Paper mode now enforces a strict finite workflow with irreversible forward states:
- `S0_WAIT_TWS`
- `S1_LOGIN`
- `S2_DISCLAIMER`
- `S3_CLOSE_POPUPS`
- `S4_DONE`

`S4_DONE` is latched when:
- main TWS window is visible
- and no login/disclaimer/modal windows are visible for at least 10 seconds

After `done_latched`, watcher performs no more actions (`action_click`, `action_key`, `action_type` stop) and only emits monitor events (`idle_noop` / `no_action`).

Strict action allowlist:
- watcher only interacts with windows that are both:
  - IBKR class allowlist matched (`WM_CLASS`)
  - phase-classified as `login`, `disclaimer`, or `popup_modal`
- non-allowlisted windows are never acted on; in debug mode watcher emits `allowlist_block`.

Single-instance enforcement (paper):
- exactly one IBKR/TWS java instance is expected
- if more than one is detected, watcher emits `multi_instance_detected` and exits non-zero (fail-closed)

Service coupling:
- `octa-ibkr.service` owns TWS lifecycle and terminates MainPID on stop (`ExecStop` + guarded `ExecStopPost`)
- `octa-autologin.service` has `PartOf=` and `BindsTo=` `octa-ibkr.service`, so it stops when IBKR stops.

## 2026-02-18 Paper One-Shot Finite FSM (Strict Allowlist)

For paper mode, the watcher now runs as a finite one-shot FSM under systemd `--once`:
- `S0_WAIT_TWS` -> `S1_LOGIN` -> `S2_DISCLAIMER` -> `S3_CLOSE_POPUPS` -> `S4_DONE`

Action safety policy:
- Actions are allowed only for high-confidence IBKR windows classified as:
  - `login`
  - `disclaimer`
  - `login_message_popup`
- Any non-matching window is ignored and logged as `unknown_window_ignored`.
- No random desktop/window clicks are permitted.

One-shot behavior:
- At most one action ladder per state in `--once` mode.
- On success path, watcher emits `done` and exits `0`.
- If no relevant windows are found, watcher emits `done(reason=no_window_found)` and exits `0`.

Login Messages popup handling:
- Detected as `role=login_message_popup` using strict class+title+context matching.
- Close ladder is deterministic and window-scoped:
  1. focus target popup
  2. `Escape`
  3. `Alt+F4`
  4. bounded popup-local click fallback
- On close, emits `popup_closed(role=login_message_popup, success=true, ...)`.

Verification (repo units):

```bash
systemctl --user daemon-reload
systemctl --user restart octa-ibkr.service
systemctl --user start octa-autologin.service
systemctl --user status octa-autologin.service --no-pager -l
journalctl --user -u octa-autologin.service -n 200 --no-pager -l | rg 'state_enter|window_detected|unknown_window_ignored|action_performed|state_change|popup_closed|done|login_flow_complete'
BOOT_DIR="$(cat /home/n-b/Octa/octa/var/runtime/systemd_boot_dir)"
EVID="$BOOT_DIR/octa_ibkr_autologin_watch"
tail -n 120 "$EVID/events.jsonl" | rg 'state_enter|window_detected|unknown_window_ignored|action_performed|state_change|popup_closed|done|login_flow_complete'
cat "$EVID/run.json"
```

## 2026-02-18 Monitor Mode (PAPER) - Diagnosing Login Not Completing

Watcher CLI now supports:

```bash
python3 /home/n-b/Octa/scripts/octa_ibkr_autologin_watch.py --mode once
python3 /home/n-b/Octa/scripts/octa_ibkr_autologin_watch.py --mode monitor --stuck-after-sec 120 --stuck-cooldown-sec 600
```

Behavior:
- `once` (default/safe): finite FSM run, exits with `done` or `idle_exit`.
- `monitor`: continuous observation + deterministic FSM interventions on allowlisted windows only.

Strict allowlist for actions:
- `role=login`
- `role=disclaimer`
- `role=login_message_popup`
- anything else: no action + `unknown_window_ignored`.

Monitor stuck diagnosis:
- Emits `stuck` once when no FSM progress beyond `--stuck-after-sec`.
- Includes `state`, `reason`, `elapsed_sec`, `window_snapshot`, and `last_action`.
- Re-emits only after state/fingerprint change or cooldown (`--stuck-cooldown-sec`).

Service mode selector (`systemd/octa-autologin.service`):
- Uses `OCTA_AUTOLOGIN_MODE` env (`once` default).
- To run monitor as long-running service, set in `~/.config/octa/env`:

```bash
OCTA_AUTOLOGIN_MODE=monitor
```

Start TWS + monitor:

```bash
systemctl --user start octa-ibkr.service
systemctl --user restart octa-autologin.service
```

Watch evidence:

```bash
BOOT_DIR="$(cat /home/n-b/Octa/octa/var/runtime/systemd_boot_dir)"
EVID="$BOOT_DIR/octa_ibkr_autologin_watch"
tail -f "$EVID/events.jsonl" | rg 'state_change|action_performed|stuck|reason|window_detected|unknown_window_ignored'
```

Confirm diagnosis:

```bash
rg -n '"event_type":"stuck"' "$EVID/events.jsonl"
rg -n '"reason":' "$EVID/events.jsonl" | tail -n 50
```

Operator commands (requested exact set):

```bash
systemctl --user start octa-ibkr.service
systemctl --user restart octa-autologin.service
BOOT_DIR="$(cat /home/n-b/Octa/octa/var/runtime/systemd_boot_dir)"
EVID="$BOOT_DIR/octa_ibkr_autologin_watch"
tail -f "$EVID/events.jsonl" | rg 'state_change|action_performed|stuck|reason|window_detected|unknown_window_ignored'
rg -n '"event":"stuck"' "$EVID/events.jsonl"
rg -n '"reason":' "$EVID/events.jsonl" | tail -n 50
```

Event schema note:

```bash
rg -n '"event_type":"stuck"' "$EVID/events.jsonl"
```

## 2026-02-18 Healthcheck: Start Monitor + Prove Progress

Use the operator healthcheck to start IBKR + autologin monitor mode and verify deterministic FSM progress.

Run:

```bash
bash scripts/octa_ibkr_autologin_healthcheck.sh
```

Optional bounds:

```bash
bash scripts/octa_ibkr_autologin_healthcheck.sh --timeout-sec 180 --progress-window-sec 30
```

Exit codes:
- `0` = healthy (`state_change` or `action_performed` or `login_flow_complete` observed)
- `2` = `stuck` detected
- `3` = `autologin_error` detected
- `4` = service/mode misconfiguration or inactive/timeout without progress

Healthcheck behavior:
- `systemctl --user daemon-reload`
- `systemctl --user set-environment OCTA_AUTOLOGIN_MODE=monitor`
- restart `octa-ibkr.service` and `octa-autologin.service`
- verify autologin starts with `--mode monitor` (or `${OCTA_AUTOLOGIN_MODE}` + env monitor)
- watch evidence since script start and return deterministic status
- on non-zero, stop both services and print last 60 journal/evidence lines

Operator commands:

```bash
systemctl --user start octa-ibkr.service
systemctl --user restart octa-autologin.service
BOOT_DIR="$(cat /home/n-b/Octa/octa/var/runtime/systemd_boot_dir)"
EVID="$BOOT_DIR/octa_ibkr_autologin_watch/events.jsonl"
tail -f "$EVID" | rg 'state_change|action_performed|stuck|reason|window_detected|unknown_window_ignored'
rg -n '"event":"stuck"' "$EVID"
rg -n '"reason":' "$EVID" | tail -n 50
```

Schema-correct stuck grep (current watcher event schema):

```bash
rg -n '"event_type":"stuck"' "$EVID"
```
