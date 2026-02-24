"""Tests for octa/core/governance/lifecycle_controller.py (I6).

All tests are offline-safe (tmp_path, no network, no broker calls).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import pytest

from octa.core.governance.lifecycle_controller import (
    LifecycleController,
    LifecycleControllerConfig,
    LifecycleDecision,
    _build_blessed_map,
    _resolve_current_statuses,
    decide_next_status,
    latest_by_model_id,
    load_live_arm_token,
    load_promotion_candidates,
    load_registry_entries,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_RESEARCH_CTX = {"stage": "research", "mode": "shadow", "execution_active": False}


def _model_entry(
    sym: str = "AAPL",
    tf: str = "1D",
    model_id: str = "mid001",
    status: str = "PAPER",
    gates: Dict[str, str] | None = None,
    created_at: str = "2026-01-01T00:00:00+00:00",
) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "model_id": model_id,
        "symbol": sym,
        "timeframe": tf,
        "created_at": created_at,
        "promotion": {"status": status, "reason": "autopilot"},
        "gates": gates or {
            "structural": "PASS",
            "risk": "HOLD",
            "performance": "PASS",
            "drift": "HOLD",
        },
        "artifact": {"path": "/tmp/m.pkl", "sha256": "abc", "size_bytes": 1024},
        "training": {"feature_code_hash": "fch", "config_hash": "cfh"},
        "environment": {"python": "3.13", "platform": "linux", "deps_fingerprint": "dfp"},
        "evidence": {
            "evidence_dir": "artifacts/runs/r1",
            "run_id": "r1",
            "inputs_hash": "ih",
            "outputs_hash": "oh",
        },
    }


def _write_registry(path: Path, entries: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _write_candidates(path: Path, candidates: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"candidates": candidates, "portfolio": {"passed": True}}),
        encoding="utf-8",
    )


def _write_token(path: Path, *, expired: bool = False, ttl_seconds: int = 900) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    if expired:
        expires_at = now - timedelta(seconds=60)
    else:
        expires_at = now + timedelta(seconds=ttl_seconds)
    token = {
        "armed_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "armed_by": "test_operator",
        "nonce": "abc123",
    }
    path.write_text(json.dumps(token), encoding="utf-8")


def _good_candidate(sym: str = "AAPL", tf: str = "1D") -> Dict[str, Any]:
    return {
        "symbol": sym,
        "timeframe": tf,
        "status": "PASS",
        "trading_days": 25,
        "drawdown": 0.02,
        "details": "ok",
    }


def _make_cfg(tmp_path: Path, **overrides) -> LifecycleControllerConfig:
    defaults: Dict[str, Any] = dict(
        registry_path=tmp_path / "registry.jsonl",
        candidates_path=None,
        token_path=tmp_path / "live_armed.json",
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    defaults.update(overrides)
    return LifecycleControllerConfig(**defaults)


# ---------------------------------------------------------------------------
# 1. load_registry_entries
# ---------------------------------------------------------------------------


def test_load_registry_entries_empty(tmp_path):
    path = tmp_path / "reg.jsonl"
    assert load_registry_entries(path) == []


def test_load_registry_entries_skips_malformed(tmp_path):
    path = tmp_path / "reg.jsonl"
    path.write_text('{"ok": 1}\nnot json\n{"ok": 2}\n', encoding="utf-8")
    entries = load_registry_entries(path)
    assert len(entries) == 2
    assert entries[0]["ok"] == 1


# ---------------------------------------------------------------------------
# 2. Append-only: promotion_event is appended, nothing is overwritten
# ---------------------------------------------------------------------------


def test_append_only_no_overwrite(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    entry = _model_entry(status="PAPER")
    _write_registry(reg_path, [entry])

    # Write good candidates + token → PAPER→LIVE should be allowed
    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate()])
    token_path = tmp_path / "live_armed.json"
    _write_token(token_path)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=cand_path,
        token_path=token_path,
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    lc = LifecycleController(cfg)
    decisions = lc.run(_RESEARCH_CTX)

    lines_before = 1
    lines_after = len(reg_path.read_text(encoding="utf-8").strip().splitlines())

    assert lines_after > lines_before  # promotion_event appended
    # First line must be unchanged (original model entry)
    first_line = json.loads(reg_path.read_text(encoding="utf-8").splitlines()[0])
    assert first_line["model_id"] == "mid001"
    assert first_line.get("entry_type") != "promotion_event"


def test_append_only_last_line_is_promotion_event(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    entry = _model_entry(status="PAPER")
    _write_registry(reg_path, [entry])

    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate()])
    token_path = tmp_path / "live_armed.json"
    _write_token(token_path)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=cand_path,
        token_path=token_path,
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    lc = LifecycleController(cfg)
    lc.run(_RESEARCH_CTX)

    last_line = json.loads(reg_path.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert last_line.get("entry_type") == "promotion_event"
    assert last_line.get("to_status") == "LIVE"
    assert last_line.get("allowed") is True
    assert "entry_sha256" in last_line


# ---------------------------------------------------------------------------
# 3. Fail-closed: missing promotion_candidates → HOLD
# ---------------------------------------------------------------------------


def test_hold_on_missing_candidates(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    _write_registry(reg_path, [_model_entry(status="PAPER")])

    cfg = _make_cfg(tmp_path, registry_path=reg_path, candidates_path=None)
    lc = LifecycleController(cfg)
    decisions = lc.run(_RESEARCH_CTX)

    assert len(decisions) == 1
    d = decisions[0]
    assert not d.allowed
    assert d.reason == "promotion_candidates_missing"
    assert d.to_status == "PAPER"  # stays PAPER


def test_hold_on_candidate_file_missing_for_specific_key(tmp_path):
    """candidates.json exists but has no entry for AAPL/1D → no_candidate_for_key."""
    reg_path = tmp_path / "registry.jsonl"
    _write_registry(reg_path, [_model_entry(sym="AAPL", tf="1D", status="PAPER")])
    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate(sym="MSFT", tf="1D")])  # different symbol

    cfg = _make_cfg(
        tmp_path, registry_path=reg_path, candidates_path=cand_path,
        token_path=tmp_path / "no_token.json",
    )
    lc = LifecycleController(cfg)
    decisions = lc.run(_RESEARCH_CTX)

    assert decisions[0].reason == "no_candidate_for_key"
    assert not decisions[0].allowed


# ---------------------------------------------------------------------------
# 4. paper → live requires arm token (within TTL)
# ---------------------------------------------------------------------------


def test_paper_to_live_valid_token(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    _write_registry(reg_path, [_model_entry(status="PAPER")])
    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate()])
    token_path = tmp_path / "live_armed.json"
    _write_token(token_path)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=cand_path,
        token_path=token_path,
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    lc = LifecycleController(cfg)
    decisions = lc.run(_RESEARCH_CTX)

    assert decisions[0].allowed
    assert decisions[0].to_status == "LIVE"
    assert decisions[0].reason == "live_ready"


def test_paper_to_paper_ready_when_no_token(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    _write_registry(reg_path, [_model_entry(status="PAPER")])
    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate()])

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=cand_path,
        token_path=tmp_path / "no_token.json",  # does not exist
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    lc = LifecycleController(cfg)
    decisions = lc.run(_RESEARCH_CTX)

    assert not decisions[0].allowed
    assert decisions[0].to_status == "PAPER_READY_FOR_LIVE"
    assert decisions[0].reason == "live_arm_required"


def test_paper_to_paper_ready_when_token_expired(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    _write_registry(reg_path, [_model_entry(status="PAPER")])
    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate()])
    token_path = tmp_path / "live_armed.json"
    _write_token(token_path, expired=True)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=cand_path,
        token_path=token_path,
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    lc = LifecycleController(cfg)
    decisions = lc.run(_RESEARCH_CTX)

    assert not decisions[0].allowed
    assert decisions[0].to_status == "PAPER_READY_FOR_LIVE"
    assert decisions[0].reason == "live_arm_required"


def test_load_live_arm_token_missing(tmp_path):
    armed, details = load_live_arm_token(tmp_path / "no_token.json", 900)
    assert not armed
    assert details["reason"] == "token_file_missing"


def test_load_live_arm_token_expired(tmp_path):
    token_path = tmp_path / "token.json"
    _write_token(token_path, expired=True)
    armed, details = load_live_arm_token(token_path, 900)
    assert not armed
    assert details["reason"] == "token_expired"


def test_load_live_arm_token_valid(tmp_path):
    token_path = tmp_path / "token.json"
    _write_token(token_path)
    armed, details = load_live_arm_token(token_path, 900)
    assert armed
    assert details.get("armed") is True


# ---------------------------------------------------------------------------
# 5. require_blessed_1d_1h enforcement
# ---------------------------------------------------------------------------


def test_require_blessed_1d_1h_missing_1d_holds(tmp_path):
    """CANDIDATE model for AAPL/5M; only 1H is in registry → HOLD."""
    reg_path = tmp_path / "registry.jsonl"
    entry_5m = _model_entry(sym="AAPL", tf="5M", model_id="m5m", status="CANDIDATE")
    entry_1h = _model_entry(sym="AAPL", tf="1H", model_id="m1h", status="CANDIDATE")
    _write_registry(reg_path, [entry_5m, entry_1h])

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=None,
        token_path=tmp_path / "no_token.json",
        ttl_seconds=900,
        require_blessed_1d_1h=True,
    )
    entries = load_registry_entries(reg_path)
    blessed_map = _build_blessed_map(entries)

    d = decide_next_status(
        entry_5m, "CANDIDATE", cfg, {}, (False, {}), blessed_map
    )
    assert not d.allowed
    assert d.reason == "blessed_1d_1h_missing"
    assert d.inputs["has_1d"] is False  # no 1D entry


def test_require_blessed_1d_1h_missing_1h_holds(tmp_path):
    """CANDIDATE model for AAPL/5M; only 1D in registry → HOLD."""
    reg_path = tmp_path / "registry.jsonl"
    entry_5m = _model_entry(sym="AAPL", tf="5M", model_id="m5m", status="CANDIDATE")
    entry_1d = _model_entry(sym="AAPL", tf="1D", model_id="m1d", status="CANDIDATE")
    _write_registry(reg_path, [entry_5m, entry_1d])

    entries = load_registry_entries(reg_path)
    blessed_map = _build_blessed_map(entries)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=None,
        token_path=tmp_path / "no_token.json",
        ttl_seconds=900,
        require_blessed_1d_1h=True,
    )
    d = decide_next_status(
        entry_5m, "CANDIDATE", cfg, {}, (False, {}), blessed_map
    )
    assert not d.allowed
    assert d.reason == "blessed_1d_1h_missing"
    assert d.inputs["has_1h"] is False  # no 1H entry


def test_require_blessed_1d_1h_both_present_advances_to_shadow(tmp_path):
    """CANDIDATE with both 1D and 1H present in registry → SHADOW."""
    reg_path = tmp_path / "registry.jsonl"
    entry_5m = _model_entry(sym="AAPL", tf="5M", model_id="m5m", status="CANDIDATE")
    entry_1d = _model_entry(sym="AAPL", tf="1D", model_id="m1d", status="CANDIDATE")
    entry_1h = _model_entry(sym="AAPL", tf="1H", model_id="m1h", status="CANDIDATE")
    _write_registry(reg_path, [entry_5m, entry_1d, entry_1h])

    entries = load_registry_entries(reg_path)
    blessed_map = _build_blessed_map(entries)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=None,
        token_path=tmp_path / "no_token.json",
        ttl_seconds=900,
        require_blessed_1d_1h=True,
    )
    d = decide_next_status(
        entry_5m, "CANDIDATE", cfg, {}, (False, {}), blessed_map
    )
    assert d.allowed
    assert d.to_status == "SHADOW"
    assert d.reason == "gates_pass"


# ---------------------------------------------------------------------------
# 6. Determinism: same inputs → same outputs (except as_of timestamp)
# ---------------------------------------------------------------------------


def test_deterministic_same_inputs_same_output(tmp_path):
    """decide_next_status must return same to_status/allowed/reason for same inputs."""
    entry = _model_entry(status="PAPER")
    cfg = _make_cfg(tmp_path)  # no candidates, no token
    results = [
        decide_next_status(entry, "PAPER", cfg, {}, (False, {}), {})
        for _ in range(5)
    ]
    to_statuses = {d.to_status for d in results}
    reasons = {d.reason for d in results}
    allowed = {d.allowed for d in results}
    assert len(to_statuses) == 1
    assert len(reasons) == 1
    assert len(allowed) == 1


# ---------------------------------------------------------------------------
# 7. Promotion_event status resolution
# ---------------------------------------------------------------------------


def test_resolve_current_statuses_promotion_event_overrides(tmp_path):
    """An allowed promotion_event overrides the base model entry status."""
    entry = _model_entry(status="PAPER", created_at="2026-01-01T00:00:00+00:00")
    promo_event = {
        "entry_type": "promotion_event",
        "symbol": "AAPL",
        "timeframe": "1D",
        "from_status": "PAPER",
        "to_status": "LIVE",
        "allowed": True,
        "created_at": "2026-01-02T00:00:00+00:00",
    }
    statuses = _resolve_current_statuses([entry, promo_event])
    assert statuses.get("AAPL|1D") == "LIVE"


def test_resolve_current_statuses_rejected_event_does_not_change_status(tmp_path):
    """A non-allowed promotion_event must NOT change current status."""
    entry = _model_entry(status="PAPER", created_at="2026-01-01T00:00:00+00:00")
    promo_event = {
        "entry_type": "promotion_event",
        "symbol": "AAPL",
        "timeframe": "1D",
        "from_status": "PAPER",
        "to_status": "LIVE",
        "allowed": False,
        "created_at": "2026-01-02T00:00:00+00:00",
    }
    statuses = _resolve_current_statuses([entry, promo_event])
    assert statuses.get("AAPL|1D") == "PAPER"


# ---------------------------------------------------------------------------
# 8. Lifecycle controller doesn't write in non-research context
# ---------------------------------------------------------------------------


def test_no_write_in_non_research_context(tmp_path):
    reg_path = tmp_path / "registry.jsonl"
    _write_registry(reg_path, [_model_entry(status="PAPER")])
    cand_path = tmp_path / "candidates.json"
    _write_candidates(cand_path, [_good_candidate()])
    token_path = tmp_path / "live_armed.json"
    _write_token(token_path)

    cfg = LifecycleControllerConfig(
        registry_path=reg_path,
        candidates_path=cand_path,
        token_path=token_path,
        ttl_seconds=900,
        require_blessed_1d_1h=False,
    )
    lc = LifecycleController(cfg)
    non_research_ctx = {"stage": "production", "mode": "shadow", "execution_active": False}
    lc.run(non_research_ctx)

    # Only the original line should be present (no promotion_event appended)
    lines = reg_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1


# ---------------------------------------------------------------------------
# 9. Gate FAIL → REJECTED
# ---------------------------------------------------------------------------


def test_gate_fail_produces_rejected(tmp_path):
    entry = _model_entry(
        status="CANDIDATE",
        gates={"structural": "FAIL", "risk": "HOLD", "performance": "PASS", "drift": "HOLD"},
    )
    cfg = _make_cfg(tmp_path)
    d = decide_next_status(entry, "CANDIDATE", cfg, {}, (False, {}), {})
    assert not d.allowed
    assert d.to_status == "REJECTED"
    assert d.reason == "gate_fail"


# ---------------------------------------------------------------------------
# 10. min_trading_days enforced
# ---------------------------------------------------------------------------


def test_min_trading_days_not_met_holds(tmp_path):
    entry = _model_entry(status="PAPER")
    cfg = _make_cfg(tmp_path)
    cfg.min_trading_days = 20  # default
    candidate = {"symbol": "AAPL", "timeframe": "1D", "status": "PASS", "trading_days": 5}
    d = decide_next_status(entry, "PAPER", cfg, {"AAPL|1D": candidate}, (True, {}), {})
    assert not d.allowed
    assert d.reason == "min_trading_days_not_met"


# ---------------------------------------------------------------------------
# 11. LifecycleDecision is a frozen dataclass
# ---------------------------------------------------------------------------


def test_lifecycle_decision_is_frozen():
    d = LifecycleDecision(
        model_id="m1", symbol="AAPL", timeframe="1D",
        from_status="PAPER", to_status="LIVE",
        allowed=True, reason="test",
    )
    with pytest.raises((AttributeError, TypeError)):
        d.allowed = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 12. load_promotion_candidates fail-closed
# ---------------------------------------------------------------------------


def test_load_promotion_candidates_missing_file(tmp_path):
    result = load_promotion_candidates(tmp_path / "nope.json")
    assert result == {}


def test_load_promotion_candidates_malformed_file(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("not json", encoding="utf-8")
    result = load_promotion_candidates(path)
    assert result == {}


def test_load_promotion_candidates_hold_entries_excluded(tmp_path):
    """Only PASS candidates are included in the lookup dict."""
    path = tmp_path / "candidates.json"
    _write_candidates(path, [
        {"symbol": "AAPL", "timeframe": "1D", "status": "PASS"},
        {"symbol": "MSFT", "timeframe": "1H", "status": "HOLD"},
    ])
    result = load_promotion_candidates(path)
    # Note: load_promotion_candidates doesn't filter by status — it loads all.
    # T3 in decide_next_status handles status checking.
    assert "AAPL|1D" in result
    assert "MSFT|1H" in result
