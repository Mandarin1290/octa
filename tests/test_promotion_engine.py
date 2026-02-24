"""Tests for octa/core/governance/promotion_engine.py (I3)."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from octa.core.governance.governance_audit import GovernanceAudit
from octa.core.governance.promotion_engine import PromotionDecision, PromotionEngine
from octa_ops.autopilot.registry import ArtifactRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_registry(tmp_path) -> ArtifactRegistry:
    return ArtifactRegistry(root=str(tmp_path / "reg"))


def _make_gov(tmp_path, run_id: str = "test_run") -> GovernanceAudit:
    return GovernanceAudit(run_id=run_id, root=tmp_path / "audit")


def _add_artifact(reg: ArtifactRegistry, symbol: str = "AAPL", timeframe: str = "1D") -> int:
    return reg.add_artifact(
        run_id="r1",
        symbol=symbol,
        timeframe=timeframe,
        artifact_kind="model",
        path="/tmp/fake_model.cbm",
        sha256="abc123",
        schema_version=1,
    )


def _good_reports() -> Dict[str, Any]:
    return {
        "validation_report": {
            "ok": True,
            "aggregate_metrics": {"sharpe_cv": 0.2},
        },
        "scoring_report": {
            "ok": True,
            "metrics": {"sharpe": 1.5, "max_drawdown": 0.05, "trade_count": 100},
        },
        "mc_report": {"ok": True, "prob_dd_breach": 0.05},
        "thresholds": {
            "min_sharpe": 1.0,
            "max_drawdown": 0.12,
            "min_trades": 50,
            "max_split_cv": 0.5,
            "mc_dd_prob_max": 0.1,
            "min_improvement": 0.05,
        },
    }


def _good_champion_artifacts() -> Dict[str, Any]:
    return {"score": 1.5, "stability_ok": True}


def _read_events(gov: GovernanceAudit) -> List[str]:
    return [ev["payload"]["event_type"] for ev in gov.read_events()]


def _get_lifecycle(reg: ArtifactRegistry, artifact_id: int):
    return reg.get_lifecycle_status(artifact_id)


# ---------------------------------------------------------------------------
# Lifecycle transition tests (no gate)
# ---------------------------------------------------------------------------

def test_research_to_shadow(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    # New artifact has lifecycle_status = RESEARCH (default)
    dec = eng.promote(aid, "SHADOW")
    assert dec.ok
    assert dec.from_status == "RESEARCH"
    assert dec.to_status == "SHADOW"
    assert _get_lifecycle(reg, aid) == "SHADOW"


def test_live_to_retired(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "LIVE")
    dec = eng.retire(aid)
    assert dec.ok
    assert dec.from_status == "LIVE"
    assert dec.to_status == "RETIRED"
    assert _get_lifecycle(reg, aid) == "RETIRED"


def test_quarantined_to_retired(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "QUARANTINED")
    dec = eng.retire(aid)
    assert dec.ok
    assert dec.from_status == "QUARANTINED"
    assert dec.to_status == "RETIRED"
    assert _get_lifecycle(reg, aid) == "RETIRED"


def test_shadow_has_no_eligibility_gate(tmp_path):
    """RESEARCH → SHADOW must succeed without any reports."""
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    dec = eng.promote(aid, "SHADOW")
    assert dec.ok
    assert _get_lifecycle(reg, aid) == "SHADOW"


# ---------------------------------------------------------------------------
# Gated transition: PAPER
# ---------------------------------------------------------------------------

def test_shadow_to_paper_with_good_reports(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "SHADOW")
    rpts = _good_reports()
    dec = eng.promote(
        aid, "PAPER",
        validation_report=rpts["validation_report"],
        scoring_report=rpts["scoring_report"],
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
    )
    assert dec.ok
    assert dec.from_status == "SHADOW"
    assert dec.to_status == "PAPER"
    assert _get_lifecycle(reg, aid) == "PAPER"


def test_paper_gate_blocks_on_low_sharpe(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "SHADOW")
    rpts = _good_reports()
    bad_scoring = dict(rpts["scoring_report"])
    bad_scoring["metrics"] = dict(bad_scoring["metrics"])
    bad_scoring["metrics"]["sharpe"] = 0.3  # below min_sharpe=1.0
    dec = eng.promote(
        aid, "PAPER",
        validation_report=rpts["validation_report"],
        scoring_report=bad_scoring,
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
    )
    assert not dec.ok
    assert dec.reason == "min_sharpe"
    assert _get_lifecycle(reg, aid) == "SHADOW"  # unchanged


def test_paper_gate_passes_on_good_metrics(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "SHADOW")
    rpts = _good_reports()
    dec = eng.promote(
        aid, "PAPER",
        validation_report=rpts["validation_report"],
        scoring_report=rpts["scoring_report"],
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
    )
    assert dec.ok
    assert _get_lifecycle(reg, aid) == "PAPER"


# ---------------------------------------------------------------------------
# Gated transition: LIVE
# ---------------------------------------------------------------------------

def test_paper_to_live_with_good_reports_and_champion(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "PAPER")
    rpts = _good_reports()
    dec = eng.promote(
        aid, "LIVE",
        validation_report=rpts["validation_report"],
        scoring_report=rpts["scoring_report"],
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
        model_artifacts=_good_champion_artifacts(),
        champion_score=1.0,  # challenger=1.5 → delta=0.5 ≥ 0.05
    )
    assert dec.ok
    assert dec.from_status == "PAPER"
    assert dec.to_status == "LIVE"
    assert _get_lifecycle(reg, aid) == "LIVE"


def test_live_gate_blocks_when_release_fails(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "PAPER")
    rpts = _good_reports()
    bad_validation = {"ok": False, "errors": ["schema_mismatch"]}
    dec = eng.promote(
        aid, "LIVE",
        validation_report=bad_validation,
        scoring_report=rpts["scoring_report"],
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
        model_artifacts=_good_champion_artifacts(),
    )
    assert not dec.ok
    assert dec.reason == "validation_failed"
    assert _get_lifecycle(reg, aid) == "PAPER"


def test_live_gate_blocks_when_champion_fails(tmp_path):
    """stability_ok=False → champion gate rejects."""
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "PAPER")
    rpts = _good_reports()
    unstable_artifacts = {"score": 1.5, "stability_ok": False}
    dec = eng.promote(
        aid, "LIVE",
        validation_report=rpts["validation_report"],
        scoring_report=rpts["scoring_report"],
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
        model_artifacts=unstable_artifacts,
        champion_score=1.0,
    )
    assert not dec.ok
    assert dec.reason == "stability_failed"
    assert _get_lifecycle(reg, aid) == "PAPER"


def test_live_gate_blocks_when_improvement_insufficient(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "PAPER")
    rpts = _good_reports()
    dec = eng.promote(
        aid, "LIVE",
        validation_report=rpts["validation_report"],
        scoring_report=rpts["scoring_report"],
        mc_report=rpts["mc_report"],
        thresholds=rpts["thresholds"],
        model_artifacts={"score": 1.02, "stability_ok": True},
        champion_score=1.0,  # delta=0.02 < min_improvement=0.05
    )
    assert not dec.ok
    assert dec.reason == "improvement_insufficient"
    assert _get_lifecycle(reg, aid) == "PAPER"


# ---------------------------------------------------------------------------
# Invalid transitions
# ---------------------------------------------------------------------------

def test_invalid_transition_research_to_live_is_rejected(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    dec = eng.promote(aid, "LIVE")
    assert not dec.ok
    assert dec.reason == "invalid_transition"
    events = _read_events(gov)
    assert "PROMOTION_REJECTED" in events


def test_invalid_transition_retired_to_paper_is_rejected(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "RETIRED")
    dec = eng.promote(aid, "PAPER")
    assert not dec.ok
    assert dec.reason == "invalid_transition"


def test_invalid_transition_shadow_to_live_is_rejected(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "SHADOW")
    dec = eng.promote(aid, "LIVE")
    assert not dec.ok
    assert dec.reason == "invalid_transition"
    assert _get_lifecycle(reg, aid) == "SHADOW"


# ---------------------------------------------------------------------------
# NULL lifecycle (pre-I1 rows) treated as RESEARCH
# ---------------------------------------------------------------------------

def test_null_lifecycle_treated_as_research(tmp_path):
    """An artifact with NULL lifecycle_status should behave like RESEARCH."""
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    # Force NULL via direct SQL (simulates pre-I1 row)
    reg._conn.execute("UPDATE artifacts SET lifecycle_status=NULL WHERE id=?", (aid,))
    assert reg.get_lifecycle_status(aid) is None
    dec = eng.promote(aid, "SHADOW")
    assert dec.ok
    assert dec.from_status == "RESEARCH"
    assert _get_lifecycle(reg, aid) == "SHADOW"


def test_null_lifecycle_invalid_transition_to_live_rejected(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg._conn.execute("UPDATE artifacts SET lifecycle_status=NULL WHERE id=?", (aid,))
    dec = eng.promote(aid, "LIVE")
    assert not dec.ok
    assert dec.reason == "invalid_transition"


# ---------------------------------------------------------------------------
# get_promoted_artifacts() safety gap fixes
# ---------------------------------------------------------------------------

def _promote_artifact_at_status(reg, status, level="paper"):
    """Insert an artifact with the given lifecycle_status and a promotion row."""
    aid = _add_artifact(reg)
    if status is None:
        reg._conn.execute("UPDATE artifacts SET lifecycle_status=NULL WHERE id=?", (aid,))
    else:
        reg.set_lifecycle_status(aid, status)
    reg.promote("AAPL", "1D", aid, level)
    return aid


def test_get_promoted_artifacts_excludes_quarantined_for_paper(tmp_path):
    reg = _make_registry(tmp_path)
    _promote_artifact_at_status(reg, "QUARANTINED", "paper")
    results = reg.get_promoted_artifacts("paper")
    assert results == []


def test_get_promoted_artifacts_excludes_retired_for_paper(tmp_path):
    reg = _make_registry(tmp_path)
    _promote_artifact_at_status(reg, "RETIRED", "paper")
    results = reg.get_promoted_artifacts("paper")
    assert results == []


def test_get_promoted_artifacts_allows_paper_and_live_for_level_paper(tmp_path):
    reg = _make_registry(tmp_path)
    # Need two different (symbol, timeframe) combos because of UNIQUE(symbol, timeframe, level)
    aid1 = reg.add_artifact("r1", "AAPL", "1D", "model", "/tmp/m1.cbm", "sha1", 1)
    aid2 = reg.add_artifact("r1", "MSFT", "1H", "model", "/tmp/m2.cbm", "sha2", 1)
    reg.set_lifecycle_status(aid1, "PAPER")
    reg.set_lifecycle_status(aid2, "LIVE")
    reg.promote("AAPL", "1D", aid1, "paper")
    reg.promote("MSFT", "1H", aid2, "paper")
    results = reg.get_promoted_artifacts("paper")
    assert len(results) == 2


def test_get_promoted_artifacts_allows_null_for_level_paper(tmp_path):
    """NULL lifecycle (pre-I1 rows) should still be visible for level=paper."""
    reg = _make_registry(tmp_path)
    _promote_artifact_at_status(reg, None, "paper")
    results = reg.get_promoted_artifacts("paper")
    assert len(results) == 1


def test_get_promoted_artifacts_live_only_allows_live(tmp_path):
    """For level=live, only LIVE status passes; PAPER and NULL are excluded."""
    reg = _make_registry(tmp_path)
    # PAPER → should not appear for live
    aid_paper = reg.add_artifact("r1", "AAPL", "1D", "model", "/tmp/m1.cbm", "sha1", 1)
    reg.set_lifecycle_status(aid_paper, "PAPER")
    reg.promote("AAPL", "1D", aid_paper, "live")

    # NULL → should not appear for live
    aid_null = reg.add_artifact("r1", "MSFT", "1H", "model", "/tmp/m2.cbm", "sha2", 1)
    reg._conn.execute("UPDATE artifacts SET lifecycle_status=NULL WHERE id=?", (aid_null,))
    reg.promote("MSFT", "1H", aid_null, "live")

    # LIVE → should appear
    aid_live = reg.add_artifact("r1", "GOOG", "30M", "model", "/tmp/m3.cbm", "sha3", 1)
    reg.set_lifecycle_status(aid_live, "LIVE")
    reg.promote("GOOG", "30M", aid_live, "live")

    results = reg.get_promoted_artifacts("live")
    assert len(results) == 1
    assert results[0]["symbol"] == "GOOG"


# ---------------------------------------------------------------------------
# retire() convenience wrapper
# ---------------------------------------------------------------------------

def test_retire_convenience_wrapper(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "PAPER")
    dec = eng.retire(aid, reason="model_degraded")
    assert dec.ok
    assert dec.to_status == "RETIRED"
    assert dec.reason == "model_degraded"
    assert _get_lifecycle(reg, aid) == "RETIRED"


def test_retire_from_quarantined(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    reg.set_lifecycle_status(aid, "QUARANTINED")
    dec = eng.retire(aid)
    assert dec.ok
    assert dec.from_status == "QUARANTINED"
    assert _get_lifecycle(reg, aid) == "RETIRED"


# ---------------------------------------------------------------------------
# Event chain tests
# ---------------------------------------------------------------------------

def test_successful_promotion_emits_model_promoted_event(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    eng.promote(aid, "SHADOW")
    events = _read_events(gov)
    assert "MODEL_PROMOTED" in events
    assert "PROMOTION_REJECTED" not in events


def test_rejected_promotion_emits_promotion_rejected_event(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    # Invalid transition: RESEARCH → LIVE
    eng.promote(aid, "LIVE")
    events = _read_events(gov)
    assert "PROMOTION_REJECTED" in events
    assert "MODEL_PROMOTED" not in events


# ---------------------------------------------------------------------------
# Dataclass immutability
# ---------------------------------------------------------------------------

def test_promotion_decision_is_frozen_dataclass(tmp_path):
    reg = _make_registry(tmp_path)
    gov = _make_gov(tmp_path)
    eng = PromotionEngine(reg, gov)
    aid = _add_artifact(reg)
    dec = eng.promote(aid, "SHADOW")
    with pytest.raises((AttributeError, TypeError)):
        dec.ok = False  # type: ignore[misc]
