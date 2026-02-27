from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from octa.execution.pre_execution import (
    HandshakeConfig,
    PortCheckConfig,
    PreExecutionError,
    PreExecutionSettings,
    TelegramConfig,
    _parse_log_path,
    _redact_env,
    broker_handshake_read_only,
    check_port_open,
    run_pre_execution_gate,
    run_tws_e2e,
)
from octa.execution.runner import ExecutionConfig, run_execution


def _settings(script_path: str = "scripts/tws_e2e.sh") -> PreExecutionSettings:
    return PreExecutionSettings(
        enabled=True,
        tws_e2e_script=script_path,
        tws_e2e_env_passthrough=True,
        tws_e2e_timeout_sec=30,
        port_check=PortCheckConfig(host="127.0.0.1", port=7497, timeout_sec=1, retries=3, backoff_sec=0.1),
        handshake=HandshakeConfig(
            enabled=True,
            mode="read_only",
            connect_timeout_sec=2,
            wait_for=("nextValidId", "currentTime"),
            overall_timeout_sec=2,
        ),
        telegram=TelegramConfig(
            enabled=True,
            on_ready_message="TWS bereit ✅",
            on_fail_message="TWS Pre-Execution FAIL ❌: {reason}",
        ),
        ibkr_client_id=901,
    )


class _DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeNotifier:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def emit(self, event_type: str, payload: dict) -> bool:
        self.events.append((event_type, dict(payload)))
        return True


def test_runner_calls_pre_exec_before_execution(tmp_path: Path, monkeypatch) -> None:
    events: list[str] = []
    cfg_file = tmp_path / "execution_ibkr.yaml"
    cfg_file.write_text("pre_execution:\n  enabled: true\n", encoding="utf-8")

    def _fake_load(_path: Path, *, mode: str):
        events.append("load")
        return _settings(script_path=str(tmp_path / "fake.sh"))

    def _fake_gate(**kwargs):
        events.append("pre_exec")
        raise PreExecutionError("pre_execution_tws_e2e_failed")

    def _fake_snapshot(self):
        events.append("snapshot")
        return {"net_liquidation": 100000.0, "currency": "USD"}

    monkeypatch.setattr("octa.execution.runner.load_pre_execution_settings", _fake_load)
    monkeypatch.setattr("octa.execution.runner.run_pre_execution_gate", _fake_gate)
    monkeypatch.setattr("octa.execution.broker_router.BrokerRouter.account_snapshot", _fake_snapshot)

    with pytest.raises(RuntimeError, match="pre_execution_tws_e2e_failed"):
        run_execution(
            ExecutionConfig(
                mode="paper",
                evidence_dir=tmp_path / "evidence",
                broker_cfg_path=cfg_file,
            )
        )
    assert events == ["load", "pre_exec"]


def test_run_tws_e2e_invocation_and_fail_closed(tmp_path: Path) -> None:
    script = tmp_path / "tws_e2e.sh"
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")

    calls = []

    def _ok_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    out = run_tws_e2e(
        script_path=str(script),
        timeout_sec=12,
        env_passthrough=True,
        evidence_dir=tmp_path / "ev",
        run_cmd=_ok_run,
    )
    assert out["returncode"] == 0
    cmd = calls[0][0][0]
    assert cmd[0] == "bash"
    assert str(script) in cmd
    assert int(calls[0][1]["timeout"]) == 12

    def _bad_run(*args, **kwargs):
        return SimpleNamespace(returncode=3, stdout="", stderr="bad")

    with pytest.raises(PreExecutionError, match="pre_execution_tws_e2e_failed"):
        run_tws_e2e(
            script_path=str(script),
            timeout_sec=12,
            env_passthrough=True,
            evidence_dir=tmp_path / "ev2",
            run_cmd=_bad_run,
        )


def test_port_check_retries_then_succeeds(tmp_path: Path) -> None:
    attempts = {"n": 0}
    sleeps: list[float] = []

    def _connect(addr, timeout):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise OSError("down")
        return _DummyConn()

    out = check_port_open(
        cfg=PortCheckConfig(host="127.0.0.1", port=7497, timeout_sec=1, retries=5, backoff_sec=0.25),
        evidence_dir=tmp_path / "ev",
        connect_fn=_connect,
        sleep_fn=lambda s: sleeps.append(float(s)),
    )
    assert out["ready"] is True
    assert attempts["n"] == 3
    assert sleeps == [0.25, 0.25]


def test_handshake_read_only_no_order_calls(tmp_path: Path) -> None:
    calls: list[str] = []

    class _Backend:
        forbidden_calls: list[str] = []

        def connect(self):
            calls.append("connect")
            return True

        def wait_for_next_valid_id(self, timeout_sec):
            calls.append("wait_next_valid_id")
            return True

        def req_current_time(self, timeout_sec):
            calls.append("req_current_time")
            return True

        def disconnect(self):
            calls.append("disconnect")

        def placeOrder(self, *a, **k):
            self.forbidden_calls.append("placeOrder")
            raise RuntimeError("forbidden")

        def reqOpenOrders(self, *a, **k):
            self.forbidden_calls.append("reqOpenOrders")
            raise RuntimeError("forbidden")

        def reqExecutions(self, *a, **k):
            self.forbidden_calls.append("reqExecutions")
            raise RuntimeError("forbidden")

    out = broker_handshake_read_only(
        cfg=HandshakeConfig(
            enabled=True,
            mode="read_only",
            connect_timeout_sec=2,
            wait_for=("nextValidId", "currentTime"),
            overall_timeout_sec=2,
        ),
        host="127.0.0.1",
        port=7497,
        client_id=901,
        evidence_dir=tmp_path / "ev",
        backend_factory=lambda **kwargs: _Backend(),
    )
    assert out["ready"] is True
    assert calls[0] == "connect"
    assert "wait_next_valid_id" in calls
    assert "disconnect" in calls
    assert "req_current_time" not in calls


def test_pre_execution_success_sends_ready_telegram(tmp_path: Path) -> None:
    script = tmp_path / "ok.sh"
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    notifier = _FakeNotifier()

    class _Backend:
        forbidden_calls: list[str] = []

        def connect(self):
            return True

        def wait_for_next_valid_id(self, timeout_sec):
            return False

        def req_current_time(self, timeout_sec):
            return True

        def disconnect(self):
            return None

    out = run_pre_execution_gate(
        settings=_settings(script_path=str(script)),
        evidence_dir=tmp_path / "ev",
        notifier=notifier,
        run_cmd=lambda *a, **k: SimpleNamespace(returncode=0, stdout="ok", stderr=""),
        connect_fn=lambda *a, **k: _DummyConn(),
        sleep_fn=lambda s: None,
        backend_factory=lambda **kw: _Backend(),
    )
    assert out["ready"] is True
    assert notifier.events and notifier.events[-1][0] == "pre_execution_ready"


def test_handshake_fails_when_forbidden_call_detected(tmp_path: Path) -> None:
    class _Backend:
        forbidden_calls = ["placeOrder"]

        def connect(self):
            return True

        def wait_for_next_valid_id(self, timeout_sec):
            return True

        def req_current_time(self, timeout_sec):
            return False

        def disconnect(self):
            return None

    with pytest.raises(PreExecutionError, match="pre_execution_handshake_not_read_only"):
        broker_handshake_read_only(
            cfg=HandshakeConfig(
                enabled=True,
                mode="read_only",
                connect_timeout_sec=2,
                wait_for=("nextValidId",),
                overall_timeout_sec=2,
            ),
            host="127.0.0.1",
            port=7497,
            client_id=901,
            evidence_dir=tmp_path / "ev_forbidden",
            backend_factory=lambda **kwargs: _Backend(),
        )


def test_pre_execution_fail_closed_and_no_execution_progress(tmp_path: Path, monkeypatch) -> None:
    script = tmp_path / "bad.sh"
    script.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
    notifier = _FakeNotifier()

    with pytest.raises(PreExecutionError, match="pre_execution_tws_e2e_failed"):
        run_pre_execution_gate(
            settings=_settings(script_path=str(script)),
            evidence_dir=tmp_path / "ev",
            notifier=notifier,
            run_cmd=lambda *a, **k: SimpleNamespace(returncode=9, stdout="", stderr="boom"),
            connect_fn=lambda *a, **k: _DummyConn(),
            sleep_fn=lambda s: None,
        )
    assert any(evt == "pre_execution_failed" for evt, _ in notifier.events)

    # Runner must fail-closed before broker path when pre-exec fails.
    cfg_file = tmp_path / "execution_ibkr.yaml"
    cfg_file.write_text("pre_execution:\n  enabled: true\n", encoding="utf-8")
    events: list[str] = []

    def _fake_load(_path: Path, *, mode: str):
        return _settings(script_path=str(script))

    def _fake_gate(**kwargs):
        events.append("pre_exec")
        raise PreExecutionError("pre_execution_port_unreachable")

    def _fake_snapshot(self):
        events.append("snapshot")
        return {"net_liquidation": 100000.0, "currency": "USD"}

    monkeypatch.setattr("octa.execution.runner.load_pre_execution_settings", _fake_load)
    monkeypatch.setattr("octa.execution.runner.run_pre_execution_gate", _fake_gate)
    monkeypatch.setattr("octa.execution.broker_router.BrokerRouter.account_snapshot", _fake_snapshot)
    with pytest.raises(RuntimeError, match="pre_execution_port_unreachable"):
        run_execution(
            ExecutionConfig(
                mode="paper",
                evidence_dir=tmp_path / "exec_ev",
                broker_cfg_path=cfg_file,
            )
        )
    assert events == ["pre_exec"]


# ---------------------------------------------------------------------------
# G1/G2: Evidence pack — cmd.txt, LOG: parsing, env_snapshot, manifest, result.json
# ---------------------------------------------------------------------------


def test_parse_log_path_extracts_log_line() -> None:
    assert _parse_log_path("LOG: /tmp/tws_e2e_20260227T161812Z.log\nsome other line") == "/tmp/tws_e2e_20260227T161812Z.log"
    assert _parse_log_path("no log here") is None
    assert _parse_log_path("  LOG: /path/with/spaces.log") is None  # leading spaces → not BOL
    assert _parse_log_path("LOG: /octa/var/logs/tws_e2e_X.log\n") == "/octa/var/logs/tws_e2e_X.log"


def test_redact_env_masks_secret_keys() -> None:
    env = {
        "HOME": "/home/user",
        "OCTA_IBKR_PASSWORD": "s3cr3t",
        "OCTA_TELEGRAM_BOT_TOKEN": "mytoken",
        "OCTA_IBKR_USERNAME": "trader",
        "PATH": "/usr/bin",
        "MY_SECRET": "hidden",
        "API_KEY": "key123",
    }
    redacted = _redact_env(env)
    assert redacted["HOME"] == "/home/user"
    assert redacted["PATH"] == "/usr/bin"
    assert redacted["OCTA_IBKR_USERNAME"] == "trader"
    assert redacted["OCTA_IBKR_PASSWORD"] == "<redacted>"
    assert redacted["OCTA_TELEGRAM_BOT_TOKEN"] == "<redacted>"
    assert redacted["MY_SECRET"] == "<redacted>"
    assert redacted["API_KEY"] == "<redacted>"


def test_cmd_txt_written_by_run_tws_e2e(tmp_path: Path) -> None:
    script = tmp_path / "ok.sh"
    script.write_text("#!/usr/bin/env bash\necho 'LOG: /tmp/x.log'\nexit 0\n", encoding="utf-8")
    ev = tmp_path / "ev"

    run_tws_e2e(
        script_path=str(script),
        timeout_sec=10,
        env_passthrough=False,
        evidence_dir=ev,
        run_cmd=lambda *a, **k: SimpleNamespace(returncode=0, stdout="LOG: /tmp/x.log\n", stderr=""),
    )

    cmd_txt = ev / "cmd.txt"
    assert cmd_txt.exists(), "cmd.txt must be written"
    assert "bash" in cmd_txt.read_text()
    assert str(script) in cmd_txt.read_text()


def test_run_tws_e2e_log_path_in_result(tmp_path: Path) -> None:
    script = tmp_path / "ok.sh"
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    ev = tmp_path / "ev"

    out = run_tws_e2e(
        script_path=str(script),
        timeout_sec=10,
        env_passthrough=False,
        evidence_dir=ev,
        run_cmd=lambda *a, **k: SimpleNamespace(
            returncode=0,
            stdout="starting...\nLOG: /octa/var/logs/tws_e2e_20260227T161812Z.log\nready\n",
            stderr="",
        ),
    )

    assert out["log_path"] == "/octa/var/logs/tws_e2e_20260227T161812Z.log"
    import json
    result_json = json.loads((ev / "tws_e2e.result.json").read_text())
    assert result_json["log_path"] == "/octa/var/logs/tws_e2e_20260227T161812Z.log"


def test_preexec_manifest_and_result_json_written_on_success(tmp_path: Path) -> None:
    import json
    script = tmp_path / "ok.sh"
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    notifier = _FakeNotifier()

    class _Backend:
        forbidden_calls: list = []

        def connect(self):
            return True

        def wait_for_next_valid_id(self, timeout_sec):
            return True

        def req_current_time(self, timeout_sec):
            return False

        def disconnect(self):
            pass

    ev = tmp_path / "evidence"
    out = run_pre_execution_gate(
        settings=_settings(script_path=str(script)),
        evidence_dir=ev,
        notifier=notifier,
        run_cmd=lambda *a, **k: SimpleNamespace(
            returncode=0,
            stdout="LOG: /octa/var/logs/tws_e2e_XYZ.log\n",
            stderr="",
        ),
        connect_fn=lambda *a, **k: _DummyConn(),
        sleep_fn=lambda s: None,
        backend_factory=lambda **kw: _Backend(),
        mode="paper",
        run_id="test_run_001",
    )

    gate_dir = ev / "pre_execution"
    manifest = json.loads((gate_dir / "preexec_manifest.json").read_text())
    assert manifest["run_id"] == "test_run_001"
    assert manifest["mode"] == "paper"
    assert "started_at" in manifest
    assert "completed_at" in manifest
    assert "config_hash" in manifest
    assert manifest["failed"] is False

    result = json.loads((gate_dir / "result.json").read_text())
    assert result["rc"] == 0
    assert result["handshake_ok"] is True
    assert result["api_port"] == 7497
    assert result["script_log_path"] == "/octa/var/logs/tws_e2e_XYZ.log"
    assert result["mode"] == "paper"
    assert result["run_id"] == "test_run_001"

    env_snap = (gate_dir / "env_snapshot.txt").read_text()
    assert "=" in env_snap  # at least one KEY=value line


def test_telegram_success_message_includes_metadata(tmp_path: Path) -> None:
    script = tmp_path / "ok.sh"
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    notifier = _FakeNotifier()

    class _Backend:
        forbidden_calls: list = []

        def connect(self):
            return True

        def wait_for_next_valid_id(self, timeout_sec):
            return True

        def req_current_time(self, timeout_sec):
            return False

        def disconnect(self):
            pass

    run_pre_execution_gate(
        settings=_settings(script_path=str(script)),
        evidence_dir=tmp_path / "ev",
        notifier=notifier,
        run_cmd=lambda *a, **k: SimpleNamespace(
            returncode=0,
            stdout="LOG: /octa/var/logs/tws_e2e_METACHECK.log\n",
            stderr="",
        ),
        connect_fn=lambda *a, **k: _DummyConn(),
        sleep_fn=lambda s: None,
        backend_factory=lambda **kw: _Backend(),
        mode="live",
        run_id="meta_run_007",
    )

    assert notifier.events, "at least one event expected"
    evt_type, payload = notifier.events[-1]
    assert evt_type == "pre_execution_ready"
    msg = payload["message"]
    assert "mode=live" in msg
    assert "meta_run_007" in msg
    assert "7497" in msg
    assert "/octa/var/logs/tws_e2e_METACHECK.log" in msg


def test_preexec_manifest_written_on_failure(tmp_path: Path) -> None:
    import json
    script = tmp_path / "bad.sh"
    script.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
    notifier = _FakeNotifier()

    with pytest.raises(PreExecutionError):
        run_pre_execution_gate(
            settings=_settings(script_path=str(script)),
            evidence_dir=tmp_path / "ev",
            notifier=notifier,
            run_cmd=lambda *a, **k: SimpleNamespace(returncode=7, stdout="", stderr="boom"),
            connect_fn=lambda *a, **k: _DummyConn(),
            sleep_fn=lambda s: None,
            mode="shadow",
            run_id="fail_run_001",
        )

    gate_dir = tmp_path / "ev" / "pre_execution"
    manifest = json.loads((gate_dir / "preexec_manifest.json").read_text())
    assert manifest["failed"] is True
    assert manifest["failure_reason"] == "pre_execution_tws_e2e_failed"
    assert manifest["run_id"] == "fail_run_001"
    assert manifest["mode"] == "shadow"
