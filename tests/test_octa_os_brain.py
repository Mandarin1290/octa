from __future__ import annotations

import json
from pathlib import Path

import yaml

from octa.os import OSBrain, OSBrainConfig
from octa.os.state_store import OSStateStore


def _write_policy(path: Path, *, with_send: bool = True, live_token_path: str = "") -> None:
    caps_exec = ["READ_STATE", "ISSUE_ORDER_INTENT", "WRITE_EVIDENCE"]
    if with_send:
        caps_exec.append("SEND_ORDER")

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
            "dashboard": {"enabled": True},
            "alerts": {"enabled": True},
            "broker": {"enabled": False, "required_for_execution": False},
            "training": {"enabled": False},
            "execution": {"enabled": True},
        },
        "capabilities": {
            "brain": ["READ_STATE", "WRITE_STATE", "WRITE_EVIDENCE", "WRITE_BLESSED_MODEL"],
            "dashboard_service": ["DASHBOARD_START", "WRITE_EVIDENCE"],
            "alerts_service": ["ALERT_SEND", "WRITE_EVIDENCE"],
            "broker_service": ["BROKER_CONNECT", "READ_STATE", "WRITE_EVIDENCE"],
            "training_service": ["READ_STATE", "WRITE_CANDIDATE_MODEL", "WRITE_EVIDENCE"],
            "execution_service": caps_exec,
            "risk_service": ["READ_STATE", "APPROVE_ORDER", "WRITE_EVIDENCE"],
        },
        "live_arming": {"token_path": live_token_path, "ttl_seconds": 900},
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def _new_brain(
    tmp_path: Path, *, mode: str = "shadow", arm_live_flag: bool = False, with_send: bool = True
) -> OSBrain:
    var_root = tmp_path / "var"
    state = OSStateStore(var_root)
    policy = tmp_path / "policy.yaml"
    token_path = str(var_root / "state" / "live_armed.json")
    _write_policy(policy, with_send=with_send, live_token_path=token_path)
    return OSBrain(
        OSBrainConfig(
            config_path=tmp_path / "dev.yaml",
            policy_path=policy,
            mode=mode,
            arm_live_flag=arm_live_flag,
        ),
        state_store=state,
    )


def _append_blessed(
    state: OSStateStore, symbol: str = "AAPL", p1d: str = "PASS", p1h: str = "PASS"
) -> None:
    state.paths.blessed_registry.parent.mkdir(parents=True, exist_ok=True)
    with state.paths.blessed_registry.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {"symbol": symbol, "performance_1d": p1d, "performance_1h": p1h}, sort_keys=True
            )
            + "\n"
        )


def test_no_blessed_wait_for_eligible(tmp_path: Path) -> None:
    brain = _new_brain(tmp_path, mode="shadow")
    out = brain.tick()
    assert out["state"] == "WAIT_FOR_ELIGIBLE"
    assert out["runbook"] == "EligibilityRunbook"
    assert out["action_result"].get("sent") is not True
    assert not any(brain.state_store.paths.intents_dir.glob("*.json"))
    identity = Path(out["evidence"]).parent / "run_identity.json"
    assert identity.exists()


def test_live_requires_arm_token(tmp_path: Path) -> None:
    brain = _new_brain(tmp_path, mode="live", arm_live_flag=False)
    _append_blessed(brain.state_store, "AAPL")
    out = brain.tick()
    assert out["state"] in {"WAIT", "WAIT_FOR_ELIGIBLE"}
    assert out["action_result"].get("sent") is not True


def test_2pc_reject_no_send(tmp_path: Path) -> None:
    brain = _new_brain(tmp_path, mode="paper", arm_live_flag=False)
    _append_blessed(brain.state_store, "AAPL")

    class Reject:
        allow = False
        reason = "reject"
        final_size = 0.0
        risk_snapshot = {"k": "v"}

    brain.risk_engine.decide_ml = lambda **_k: Reject()  # type: ignore[assignment]

    out = brain.tick()
    assert out["state"] == "EXECUTION_TICK"
    assert out["action_result"]["approved"] is False
    assert out["action_result"]["sent"] is False


def test_capability_violation_fail_closed(tmp_path: Path) -> None:
    brain = _new_brain(tmp_path, mode="paper", with_send=False)
    _append_blessed(brain.state_store, "AAPL")
    out = brain.tick()
    assert out["state"] == "WAIT"
    assert out["runbook"] == "CapabilityViolationRunbook"


def test_policy_invalid_disables_execution(tmp_path: Path) -> None:
    var_root = tmp_path / "var"
    state = OSStateStore(var_root)
    policy = tmp_path / "policy_invalid.yaml"
    policy.write_text("mode: []\n", encoding="utf-8")

    brain = OSBrain(
        OSBrainConfig(
            config_path=tmp_path / "dev.yaml", policy_path=policy, mode="paper", arm_live_flag=False
        ),
        state_store=state,
    )
    _append_blessed(state, "AAPL")
    out = brain.tick()
    assert out["runbook"] == "PolicyInvalidRunbook"
    assert out["state"] == "WAIT"
    assert out["action_result"].get("sent") is not True


def test_invariant_guard_blocks_wait_with_sent(tmp_path: Path) -> None:
    brain = _new_brain(tmp_path, mode="shadow")
    payload = {
        "state": "WAIT",
        "next_action": "execute_2pc",
        "reason": "bad",
        "runbook_details": {},
        "action_result": {"sent": True, "reason": "bad"},
        "2pc_status": {"sent": True, "reason": "bad"},
        "errors_and_backoff": {},
    }
    patched, violated = brain._validate_tick_invariants(payload)
    assert violated is True
    assert patched["state"] == "RECOVER_BACKOFF"
    assert patched["next_action"] == "sleep"
    assert patched["action_result"]["sent"] is False
    assert patched["2pc_status"]["sent"] is False
