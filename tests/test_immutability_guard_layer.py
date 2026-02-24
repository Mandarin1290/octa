from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from octa.core.governance.drift_monitor import evaluate_drift
from octa.core.governance.immutability_guard import assert_write_allowed
from octa.os import OSBrain, OSBrainConfig
from octa.os.state_store import OSStateStore
from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore
from octa_ops.autopilot.registry import ArtifactRegistry


def _seed_navs(ledger_dir: Path) -> None:
    ledger = LedgerStore(str(ledger_dir))
    start = datetime.now(timezone.utc) - timedelta(days=30)
    nav = 100.0
    for i in range(25):
        nav *= 0.999
        ts = (start + timedelta(days=i)).isoformat()
        ledger.append(
            AuditEvent.create(
                actor="test",
                action="performance.nav",
                payload={"date": ts, "nav": nav},
                severity="INFO",
            )
        )


def _write_os_policy(path: Path, *, training_enabled: bool, execution_enabled: bool) -> None:
    payload = {
        "mode": {"default": "shadow", "allowed": ["shadow", "paper", "live"]},
        "cadence": {
            "tick_seconds": 1,
            "eligibility_check_seconds": 1,
            "execution_check_seconds": 1,
            "training_check_seconds": 1,
        },
        "training_windows": {
            "tz": "Europe/Berlin",
            "allowed_weekdays": [0, 1, 2, 3, 4, 5, 6],
            "start_hour": 0,
            "end_hour": 23,
        },
        "backoff": {"seconds": [1, 2, 3], "max_errors_before_degrade": 2},
        "safety": {
            "default_execution_enabled": False,
            "fail_closed_on_unknown_sensor": True,
            "require_blessed_1d_1h": True,
            "default_mode_shadow": True,
        },
        "services": {
            "dashboard": {"enabled": False},
            "alerts": {"enabled": False},
            "broker": {"enabled": False, "required_for_execution": False},
            "training": {"enabled": bool(training_enabled), "command": ""},
            "execution": {"enabled": bool(execution_enabled)},
        },
        "capabilities": {
            "brain": ["READ_STATE", "WRITE_STATE", "WRITE_EVIDENCE", "WRITE_BLESSED_MODEL"],
            "dashboard_service": ["DASHBOARD_START", "WRITE_EVIDENCE"],
            "alerts_service": ["ALERT_SEND", "WRITE_EVIDENCE"],
            "broker_service": ["BROKER_CONNECT", "READ_STATE", "WRITE_EVIDENCE"],
            "training_service": ["READ_STATE", "WRITE_CANDIDATE_MODEL", "WRITE_EVIDENCE"],
            "execution_service": ["READ_STATE", "ISSUE_ORDER_INTENT", "SEND_ORDER", "WRITE_EVIDENCE"],
            "risk_service": ["READ_STATE", "APPROVE_ORDER", "WRITE_EVIDENCE"],
        },
        "live_arming": {"token_path": "octa/var/state/live_armed.json", "ttl_seconds": 900},
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def test_registry_write_blocked_in_production_context(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    reg = ArtifactRegistry(
        root=str(tmp_path / "registry"),
        ctx={
            "mode": "paper",
            "service": "autopilot",
            "execution_active": True,
            "run_id": "exec_run",
            "entrypoint": "execution_service",
        },
    )
    try:
        reg.record_run_start("run-1", {"k": "v"})
        raise AssertionError("expected IMMUTABLE_PROD_BLOCK")
    except RuntimeError as exc:
        assert "IMMUTABLE_PROD_BLOCK" in str(exc)


def test_registry_write_allowed_in_research_context(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    reg = ArtifactRegistry(
        root=str(tmp_path / "registry"),
        ctx={
            "mode": "research",
            "service": "autopilot",
            "execution_active": False,
            "run_id": "research_run",
            "entrypoint": "autopilot",
        },
    )
    cfg_sha = reg.record_run_start("run-1", {"k": "v"})
    assert isinstance(cfg_sha, str) and len(cfg_sha) > 0


def test_drift_state_write_blocked_in_production_context(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    ledger_dir = tmp_path / "ledger"
    _seed_navs(ledger_dir)

    decision = evaluate_drift(
        ledger_dir=str(ledger_dir),
        model_key="ABC_1D",
        gate="global_1d",
        timeframe="1D",
        bucket="default",
        cfg={"kpi_threshold": 0.0, "window_days": 20, "breach_days": 2},
        ctx={
            "mode": "paper",
            "service": "autopilot",
            "execution_active": True,
            "run_id": "exec_run",
            "entrypoint": "execution_service",
        },
    )
    assert decision.diagnostics.get("state_write_blocked") is True
    assert not (Path("octa") / "var" / "registry" / "models" / "drift" / "ABC_1D.json").exists()


def test_drift_state_write_allowed_in_research_context(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    ledger_dir = tmp_path / "ledger"
    _seed_navs(ledger_dir)

    decision = evaluate_drift(
        ledger_dir=str(ledger_dir),
        model_key="ABC_1D",
        gate="global_1d",
        timeframe="1D",
        bucket="default",
        cfg={"kpi_threshold": 0.0, "window_days": 20, "breach_days": 2},
        ctx={
            "mode": "research",
            "service": "autopilot",
            "execution_active": False,
            "run_id": "research_run",
            "entrypoint": "autopilot",
        },
    )
    assert isinstance(decision.kpi, float)
    assert (Path("octa") / "var" / "registry" / "models" / "drift" / "ABC_1D.json").exists()


def test_os_brain_training_tick_blocked_when_execution_active(tmp_path) -> None:
    var_root = tmp_path / "var"
    state = OSStateStore(var_root)
    policy = tmp_path / "policy.yaml"
    _write_os_policy(policy, training_enabled=True, execution_enabled=True)
    brain = OSBrain(
        OSBrainConfig(
            config_path=tmp_path / "dev.yaml",
            policy_path=policy,
            mode="shadow",
            arm_live_flag=False,
        ),
        state_store=state,
    )
    out = brain.tick()
    assert out["state"] == "WAIT_FOR_ELIGIBLE"
    assert out["action_result"].get("triggered") is False
    assert out["action_result"].get("reason") == "training_tick_blocked_immutable_prod"

    incident_path = Path(out["evidence"]).parent / "training_tick_block.json"
    assert incident_path.exists()
    payload = json.loads(incident_path.read_text(encoding="utf-8"))
    assert payload.get("reason") == "IMMUTABLE_PROD_BLOCK"


def test_training_write_allowed_in_research_context() -> None:
    assert_write_allowed(
        {
            "mode": "research",
            "service": "autopilot",
            "execution_active": False,
            "run_id": "research_run",
            "entrypoint": "autopilot",
        },
        operation="training_tick",
        target="autopilot_cascade_training",
        details={},
    )
