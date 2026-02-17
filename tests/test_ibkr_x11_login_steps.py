"""Smoke tests for the IBKR X11 login step-machine.

Validates:
  - State enum order matches spec
  - Each state handler works with mocked xdotool
  - Fail-closed on X11 failure
  - Evidence output schema (run.json + sha256.txt)
  - Passwords NEVER appear in evidence
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.execution import ibkr_x11_login as mod
from octa.execution.ibkr_x11_login import State, STATE_ORDER


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def evidence_dir(tmp_path: Path) -> str:
    d = tmp_path / "evidence"
    d.mkdir()
    return str(d)


# ---------------------------------------------------------------------------
# State enum + ordering
# ---------------------------------------------------------------------------

def test_state_order_matches_spec():
    """States must execute in the exact order from the spec."""
    expected = [
        "VERIFY_X11",
        "START_TWS",
        "WAIT_LOGIN_WINDOW",
        "INJECT_CREDENTIALS",
        "WAIT_POST_LOGIN",
        "WAIT_DISCLAIMER",
        "ACCEPT_DISCLAIMER",
        "VERIFY_STABLE",
    ]
    assert [s.value for s in STATE_ORDER] == expected


def test_state_enum_has_8_members():
    assert len(State) == 8


# ---------------------------------------------------------------------------
# StepResult
# ---------------------------------------------------------------------------

def test_step_result_pass():
    r = mod.StepResult(State.VERIFY_X11).start()
    r.pass_(foo="bar")
    d = r.to_dict()
    assert d["status"] == "PASS"
    assert d["state"] == "VERIFY_X11"
    assert d["step_name"] == "VERIFY_X11"
    assert d["details"]["foo"] == "bar"
    assert d["start_ts"]
    assert d["end_ts"]


def test_step_result_fail():
    r = mod.StepResult(State.START_TWS).start()
    r.fail(error="boom")
    d = r.to_dict()
    assert d["status"] == "FAIL"
    assert d["state"] == "START_TWS"
    assert d["details"]["error"] == "boom"


# ---------------------------------------------------------------------------
# STATE 1: VERIFY_X11
# ---------------------------------------------------------------------------

def test_verify_x11_pass_xdpyinfo(monkeypatch):
    monkeypatch.setattr(mod, "_run_cmd_ok", lambda argv, **kw: (True, ""))
    r = mod.state_verify_x11(":99")
    assert r.status == "PASS"
    assert r.state == State.VERIFY_X11
    assert r.details["method"] == "xdpyinfo"


def test_verify_x11_fail(monkeypatch):
    monkeypatch.setattr(mod, "_run_cmd_ok", lambda argv, **kw: (False, ""))
    r = mod.state_verify_x11(":57")  # no socket for :57
    assert r.status == "FAIL"


# ---------------------------------------------------------------------------
# STATE 2: START_TWS
# ---------------------------------------------------------------------------

def test_start_tws_already_running(monkeypatch):
    monkeypatch.setattr(mod, "_run_cmd_ok", lambda argv, **kw: (True, "12345 java Jts"))
    r = mod.state_start_tws("/fake/tws", ":99")
    assert r.status == "PASS"
    assert r.details["action"] == "already_running"


def test_start_tws_no_cmd():
    r = mod.state_start_tws("", ":99")
    # pgrep may or may not find Jts on this host — but with empty cmd and no process:
    # If no Jts process: FAIL with no_tws_cmd
    # If Jts process exists: PASS with already_running
    assert r.status in ("PASS", "FAIL")
    if r.status == "FAIL":
        assert r.details["error"] == "no_tws_cmd"


# ---------------------------------------------------------------------------
# STATE 3: WAIT_LOGIN_WINDOW
# ---------------------------------------------------------------------------

def test_wait_login_window_found(monkeypatch):
    monkeypatch.setattr(mod, "_xdotool_search", lambda p, by="--name": ["12345"] if "Login" in p else [])
    r = mod.state_wait_login_window(timeout_sec=2.0, poll_sec=0.1)
    assert r.status == "PASS"
    assert r.state == State.WAIT_LOGIN_WINDOW
    assert "12345" in r.details["window_ids"]


def test_wait_login_window_timeout(monkeypatch):
    monkeypatch.setattr(mod, "_xdotool_search", lambda p, by="--name": [])
    r = mod.state_wait_login_window(timeout_sec=0.3, poll_sec=0.1)
    assert r.status == "FAIL"
    assert "not_found" in r.details.get("error", "")


# ---------------------------------------------------------------------------
# STATE 4: INJECT_CREDENTIALS
# ---------------------------------------------------------------------------

def test_inject_credentials_dry_run(monkeypatch):
    monkeypatch.setenv("OCTA_IBKR_USER", "testuser")
    monkeypatch.setenv("OCTA_IBKR_PASS", "testpass")
    r = mod.state_inject_credentials(dry_run=True)
    assert r.status == "PASS"
    assert r.state == State.INJECT_CREDENTIALS
    assert r.details["dry_run"] is True
    # Password must NEVER appear in details
    d_str = json.dumps(r.to_dict())
    assert "testpass" not in d_str


def test_inject_credentials_no_creds(monkeypatch):
    monkeypatch.delenv("OCTA_IBKR_USER", raising=False)
    monkeypatch.delenv("OCTA_IBKR_PASS", raising=False)
    monkeypatch.delenv("OCTA_IBKR_SECRETS_FILE", raising=False)
    r = mod.state_inject_credentials(dry_run=False)
    assert r.status == "FAIL"
    assert "no_credentials" in r.details.get("error", "")


def test_inject_credentials_secrets_file(monkeypatch, tmp_path):
    sf = tmp_path / "secrets"
    sf.write_text("myuser\nmypass\n")
    monkeypatch.delenv("OCTA_IBKR_USER", raising=False)
    monkeypatch.delenv("OCTA_IBKR_PASS", raising=False)
    monkeypatch.setenv("OCTA_IBKR_SECRETS_FILE", str(sf))
    r = mod.state_inject_credentials(dry_run=True)
    assert r.status == "PASS"
    d_str = json.dumps(r.to_dict())
    assert "mypass" not in d_str


# ---------------------------------------------------------------------------
# STATE 6/7: DISCLAIMER
# ---------------------------------------------------------------------------

def test_accept_disclaimer_dry_run(monkeypatch):
    monkeypatch.setattr(mod, "_xdotool_search", lambda p, by="--name": ["999"] if "Disclaimer" in p else [])
    r = mod.state_accept_disclaimer(timeout_sec=2.0, dry_run=True, poll_sec=0.1)
    assert r.status == "PASS"
    assert r.state == State.ACCEPT_DISCLAIMER
    assert r.details["dry_run"] is True


def test_accept_disclaimer_already_gone(monkeypatch):
    monkeypatch.setattr(mod, "_xdotool_search", lambda p, by="--name": [])
    r = mod.state_accept_disclaimer(timeout_sec=0.3, dry_run=False, poll_sec=0.1)
    assert r.status == "PASS"
    assert r.details["action"] == "disclaimer_already_gone"


# ---------------------------------------------------------------------------
# Full sequence: dry-run with mocked X11
# ---------------------------------------------------------------------------

def test_full_sequence_dry_run_produces_evidence(monkeypatch, evidence_dir):
    """Full step machine in dry-run mode with mocked subprocess."""
    monkeypatch.setenv("OCTA_IBKR_USER", "testuser")
    monkeypatch.setenv("OCTA_IBKR_PASS", "testpass")

    # Mock X11 and pgrep as reachable
    def mock_run_cmd_ok(argv, **kw):
        cmd = argv[0] if argv else ""
        if cmd == "xdpyinfo":
            return True, ""
        if cmd == "pgrep":
            return True, "12345 java Jts"
        if cmd == "which":
            return True, "/fake/tws"
        return True, ""

    monkeypatch.setattr(mod, "_run_cmd_ok", mock_run_cmd_ok)

    def mock_search(pattern, by="--name"):
        if "Login" in pattern:
            return ["100"]
        if "Disclaimer" in pattern:
            return ["200"]
        if "Trader" in pattern or "Trading" in pattern:
            return ["300"]
        return []

    monkeypatch.setattr(mod, "_xdotool_search", mock_search)

    result = mod.run_login_sequence(
        display=":99",
        dry_run=True,
        timeout_sec=10.0,
        evidence_dir=evidence_dir,
        tws_cmd="/fake/tws",
    )

    assert result["ok"] is True

    # Verify evidence artifacts
    run_json_path = Path(evidence_dir) / "run.json"
    assert run_json_path.exists()
    run_data = json.loads(run_json_path.read_text())
    assert run_data["ok"] is True
    assert len(run_data["steps"]) == 8  # all 8 states

    # Verify state order in output
    executed_states = run_data["states_executed"]
    assert executed_states == [s.value for s in STATE_ORDER]

    sha_path = Path(evidence_dir) / "sha256.txt"
    assert sha_path.exists()

    events_path = Path(evidence_dir) / "events.jsonl"
    assert events_path.exists()

    # Password must NEVER appear anywhere in evidence
    all_text = run_json_path.read_text() + events_path.read_text()
    assert "testpass" not in all_text


def test_full_sequence_state_order_on_success(monkeypatch, evidence_dir):
    """Verify state execution order matches STATE_ORDER on a successful run."""
    monkeypatch.setenv("OCTA_IBKR_USER", "u")
    monkeypatch.setenv("OCTA_IBKR_PASS", "p")
    monkeypatch.setattr(mod, "_run_cmd_ok", lambda argv, **kw: (True, "12345 Jts"))
    monkeypatch.setattr(mod, "_xdotool_search", lambda p, by="--name":
                        ["100"] if any(k in p for k in ("Login", "Disclaimer", "Trader")) else [])

    result = mod.run_login_sequence(
        display=":99", dry_run=True, timeout_sec=10.0,
        evidence_dir=evidence_dir, tws_cmd="/fake/tws",
    )
    assert result["ok"] is True
    assert result["states_executed"] == [s.value for s in STATE_ORDER]


def test_full_sequence_fail_closed_on_x11_failure(monkeypatch, evidence_dir):
    """Step machine must fail-closed if X11 is unreachable."""
    monkeypatch.setattr(mod, "_run_cmd_ok", lambda argv, **kw: (False, ""))
    monkeypatch.setattr(mod, "_screenshot", lambda *a, **kw: False)

    result = mod.run_login_sequence(
        display=":57",
        dry_run=False,
        timeout_sec=5.0,
        evidence_dir=evidence_dir,
    )

    assert result["ok"] is False
    run_data = json.loads((Path(evidence_dir) / "run.json").read_text())
    assert run_data["steps"][0]["status"] == "FAIL"
    assert run_data["steps"][0]["state"] == "VERIFY_X11"
    # Only one step should have executed
    assert len(run_data["steps"]) == 1


def test_full_sequence_fail_closed_login_window_not_found(monkeypatch, evidence_dir):
    """Fail-closed when login window never appears."""
    monkeypatch.setenv("OCTA_IBKR_USER", "u")
    monkeypatch.setenv("OCTA_IBKR_PASS", "p")

    def mock_run_cmd_ok(argv, **kw):
        cmd = argv[0] if argv else ""
        if cmd == "xdpyinfo":
            return True, ""
        if cmd == "pgrep":
            return True, "12345 Jts"
        return False, ""

    monkeypatch.setattr(mod, "_run_cmd_ok", mock_run_cmd_ok)
    monkeypatch.setattr(mod, "_xdotool_search", lambda p, by="--name": [])
    monkeypatch.setattr(mod, "_screenshot", lambda *a, **kw: False)

    result = mod.run_login_sequence(
        display=":99", dry_run=False, timeout_sec=0.5,
        evidence_dir=evidence_dir, tws_cmd="/fake/tws",
    )

    assert result["ok"] is False
    run_data = json.loads((Path(evidence_dir) / "run.json").read_text())
    states = [s["state"] for s in run_data["steps"]]
    # Should have VERIFY_X11 (pass), START_TWS (pass), WAIT_LOGIN_WINDOW (fail)
    assert states == ["VERIFY_X11", "START_TWS", "WAIT_LOGIN_WINDOW"]
    assert run_data["steps"][-1]["status"] == "FAIL"
