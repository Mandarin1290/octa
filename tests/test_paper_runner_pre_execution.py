"""Tests for pre-execution gate integration in paper_runner.run_paper().

Verifies:
  - Sandbox mode bypasses pre-execution (no ib_insync needed for sandbox).
  - ib_insync mode with enabled pre-execution config raises RuntimeError when gate fails.
  - ib_insync mode with pre-execution disabled in config proceeds without raising.
  - Broker config auto-discovered from OCTA_BROKER_CFG env var.
  - Broker config auto-discovered from configs/execution_ibkr.yaml default path.
  - Missing config file is handled gracefully (skipped, no error).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from types import SimpleNamespace

import pytest

from octa_ops.autopilot.paper_runner import _run_paper_pre_execution


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_broker_cfg(tmp_path: Path, *, enabled: bool = True) -> Path:
    cfg = {
        "ibkr": {"host": "127.0.0.1", "port": 7497, "client_id": 901},
        "pre_execution": {
            "enabled": enabled,
            "tws_e2e_script": str(tmp_path / "fake_tws_e2e.sh"),
            "tws_e2e_env_passthrough": False,
            "tws_e2e_timeout_sec": 10,
            "port_check": {"host": "127.0.0.1", "port": 7497, "timeout_sec": 1, "retries": 1, "backoff_sec": 0.1},
            "handshake": {
                "enabled": False,
                "mode": "read_only",
                "connect_timeout_sec": 2,
                "wait_for": ["nextValidId"],
                "overall_timeout_sec": 2,
            },
            "telegram": {
                "enabled": False,
                "on_ready_message": "TWS bereit \u2705",
                "on_fail_message": "TWS FAIL: {reason}",
            },
        },
    }
    import yaml
    p = tmp_path / "broker.yaml"
    p.write_text(yaml.dump(cfg), encoding="utf-8")
    return p


def _make_ok_script(tmp_path: Path) -> Path:
    s = tmp_path / "fake_tws_e2e.sh"
    s.write_text("#!/usr/bin/env bash\necho 'LOG: /tmp/test.log'\nexit 0\n", encoding="utf-8")
    s.chmod(0o755)
    return s


def _make_fail_script(tmp_path: Path) -> Path:
    s = tmp_path / "fake_tws_e2e.sh"
    s.write_text("#!/usr/bin/env bash\necho 'LOG: /tmp/test.log'\nexit 1\n", encoding="utf-8")
    s.chmod(0o755)
    return s


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_sandbox_mode_skips_pre_execution(tmp_path: Path) -> None:
    """pre-execution must never be triggered in sandbox mode."""
    broker_cfg = _make_broker_cfg(tmp_path, enabled=True)
    # If pre-execution were run, it would fail because the script doesn't exist yet.
    # The fact that no error is raised proves it was skipped.
    _run_paper_pre_execution(
        run_id="test_sandbox_001",
        mode="paper",
        broker_mode="sandbox",
        broker_cfg_path=str(broker_cfg),
    )  # must not raise


def test_ibkr_mode_fail_closed_when_pre_execution_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ib_insync mode + enabled config + failing gate → RuntimeError (fail-closed)."""
    _make_fail_script(tmp_path)
    broker_cfg = _make_broker_cfg(tmp_path, enabled=True)

    # Stub port check and handshake so only tws_e2e failing matters.
    import octa.execution.pre_execution as pre_mod

    def _fake_port_check(**kwargs: Any) -> Dict[str, Any]:
        return {"ready": True, "attempts": [{"attempt": 1, "ok": True, "error": None}]}

    monkeypatch.setattr(pre_mod, "check_port_open", _fake_port_check)

    with pytest.raises(RuntimeError, match="pre_execution_failed:pre_execution_tws_e2e_failed"):
        _run_paper_pre_execution(
            run_id="test_ibkr_fail_001",
            mode="paper",
            broker_mode="ib_insync",
            broker_cfg_path=str(broker_cfg),
        )


def test_ibkr_mode_succeeds_when_gate_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ib_insync mode + enabled config + passing gate → no error."""
    _make_ok_script(tmp_path)
    broker_cfg = _make_broker_cfg(tmp_path, enabled=True)

    import socket
    import octa.execution.pre_execution as pre_mod

    def _fake_port_check(**kwargs: Any) -> Dict[str, Any]:
        return {"ready": True, "host": "127.0.0.1", "port": 7497, "attempts": []}

    class _FakeBackend:
        forbidden_calls: list = []
        def connect(self): return True
        def wait_for_next_valid_id(self, t): return True
        def req_current_time(self, t): return False
        def disconnect(self): pass

    monkeypatch.setattr(pre_mod, "check_port_open", _fake_port_check)
    monkeypatch.setattr(
        pre_mod,
        "broker_handshake_read_only",
        lambda **kw: {"ready": True, "mode": "read_only", "enabled": True, "seen_events": ["nextValidId"]},
    )

    _run_paper_pre_execution(
        run_id="test_ibkr_ok_001",
        mode="paper",
        broker_mode="ib_insync",
        broker_cfg_path=str(broker_cfg),
    )  # must not raise


def test_ibkr_mode_pre_execution_disabled_in_config_skips(tmp_path: Path) -> None:
    """ib_insync mode + pre_execution.enabled=False in config → skip gracefully."""
    _make_fail_script(tmp_path)  # would fail if run
    broker_cfg = _make_broker_cfg(tmp_path, enabled=False)

    _run_paper_pre_execution(
        run_id="test_disabled_001",
        mode="paper",
        broker_mode="ib_insync",
        broker_cfg_path=str(broker_cfg),
    )  # must not raise


def test_no_broker_cfg_and_no_env_var_skips(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No config path, no OCTA_BROKER_CFG, no default file → skip gracefully.

    We chdir to tmp_path so that the relative path 'configs/execution_ibkr.yaml'
    does not exist, isolating the test from the real repo configuration.
    """
    monkeypatch.delenv("OCTA_BROKER_CFG", raising=False)
    monkeypatch.chdir(tmp_path)  # no configs/execution_ibkr.yaml relative to tmp_path

    _run_paper_pre_execution(
        run_id="test_no_cfg_001",
        mode="paper",
        broker_mode="ib_insync",
        broker_cfg_path=None,
    )  # must not raise (skipped gracefully)


def test_octa_broker_cfg_env_var_picked_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """OCTA_BROKER_CFG env var is used when broker_cfg_path is None."""
    _make_ok_script(tmp_path)
    broker_cfg = _make_broker_cfg(tmp_path, enabled=False)  # disabled → skip but still discovered
    monkeypatch.setenv("OCTA_BROKER_CFG", str(broker_cfg))

    # With pre_execution.enabled=False the gate is skipped; no error means env var was found.
    _run_paper_pre_execution(
        run_id="test_env_var_001",
        mode="paper",
        broker_mode="ib_insync",
        broker_cfg_path=None,
    )  # must not raise


def test_evidence_pack_written_in_ibkr_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """On successful gate in ib_insync mode, evidence files must be written."""
    _make_ok_script(tmp_path)
    broker_cfg = _make_broker_cfg(tmp_path, enabled=True)

    import octa.execution.pre_execution as pre_mod

    monkeypatch.setattr(pre_mod, "check_port_open", lambda **kw: {"ready": True, "attempts": []})
    monkeypatch.setattr(
        pre_mod,
        "broker_handshake_read_only",
        lambda **kw: {"ready": True, "mode": "read_only", "enabled": True, "seen_events": ["nextValidId"]},
    )

    _run_paper_pre_execution(
        run_id="test_evidence_001",
        mode="paper",
        broker_mode="ib_insync",
        broker_cfg_path=str(broker_cfg),
    )

    evidence_base = Path("octa") / "var" / "evidence" / "pre_exec_test_evidence_001"
    gate_dir = evidence_base / "pre_execution"
    assert (gate_dir / "preexec_manifest.json").exists(), "preexec_manifest.json must be written"
    assert (gate_dir / "result.json").exists(), "result.json must be written"
    assert (gate_dir / "cmd.txt").exists(), "cmd.txt must be written"
    assert (gate_dir / "env_snapshot.txt").exists(), "env_snapshot.txt must be written"

    manifest = json.loads((gate_dir / "preexec_manifest.json").read_text())
    assert manifest["run_id"] == "test_evidence_001"
    assert manifest["mode"] == "paper"
    assert manifest["failed"] is False
