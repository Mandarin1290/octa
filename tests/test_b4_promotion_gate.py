"""Tests for Phase 4 B4: Paper → Live Promotion Gate.

Verifies:
1) below_threshold            — sharpe < min_sharpe → eligible=False
2) above_threshold            — all metrics pass → eligible=True
3) missing_metric             — missing 'sharpe' → eligible=False, MISSING_METRIC
4) drift_breach_file          — drift registry file with disabled=False → eligible=False
5) risk_incident_present      — risk_incident_count > 0 → eligible=False, RISK_INCIDENTS_PRESENT
6) registry_mismatch          — SQLite vs JSONL disagree → eligible=False, REGISTRY_MISMATCH
7) determinism                — same inputs → sha256 self-consistent across calls
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from octa.core.promotion.promotion_criteria import PromotionCriteria, evaluate
from octa.core.promotion.promotion_engine import (
    evaluate_promotion,
    load_drift_state,
    load_registry_state,
    write_decision,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_GOOD_METRICS = {
    "1D": {"sharpe": 1.5, "max_drawdown": 0.04, "n_trades": 120},
}

_BAD_METRICS = {
    "1D": {"sharpe": 0.3, "max_drawdown": 0.04, "n_trades": 120},
}

_CRITERIA = PromotionCriteria(
    min_sharpe=0.8,
    max_drawdown=0.15,
    min_trades=30,
    min_survivors=1,
    drift_guard_required=True,
    risk_incidents_max=0,
)


def _make_sqlite(path: Path, symbol: str, timeframe: str, lifecycle_status: str) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE artifacts ("
        "id INTEGER PRIMARY KEY, run_id TEXT, symbol TEXT, timeframe TEXT, "
        "artifact_kind TEXT, path TEXT, sha256 TEXT, size_bytes INTEGER, "
        "schema_version INTEGER, created_at TEXT, status TEXT, meta_json TEXT, "
        "lifecycle_status TEXT DEFAULT 'RESEARCH')"
    )
    conn.execute(
        "INSERT INTO artifacts (run_id, symbol, timeframe, lifecycle_status) VALUES (?,?,?,?)",
        ("run1", symbol, timeframe, lifecycle_status),
    )
    conn.commit()
    conn.close()


def _make_jsonl(path: Path, symbol: str, timeframe: str, status: str) -> None:
    entry = {
        "model_id": "abc123",
        "symbol": symbol,
        "timeframe": timeframe,
        "promotion": {"status": status, "reason": "test"},
        "gates": {"structural": "PASS", "performance": "PASS", "risk": "PASS", "drift": "PASS"},
        "entry_sha256": "dummy",
    }
    path.write_text(json.dumps(entry) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# 1 — below threshold → not eligible
# ---------------------------------------------------------------------------

def test_below_threshold_not_eligible() -> None:
    """sharpe=0.3 < min_sharpe=0.8 → eligible=False, SHARPE_TOO_LOW in TF reasons."""
    eligible, reasons, details = evaluate(
        _BAD_METRICS, criteria=_CRITERIA, drift_breaches={}, risk_incident_count=0
    )
    assert not eligible
    # SHARPE_TOO_LOW is captured per-timeframe in timeframe_results
    tf_reasons = details["timeframe_results"]["1D"]["reasons"]
    assert any("SHARPE_TOO_LOW" in r for r in tf_reasons), (
        f"Expected SHARPE_TOO_LOW in tf_reasons={tf_reasons}"
    )
    assert details["survivors"] == 0
    # Symbol-level reason: INSUFFICIENT_SURVIVORS (derived from 0 passing timeframes)
    assert any("INSUFFICIENT_SURVIVORS" in r for r in reasons)


# ---------------------------------------------------------------------------
# 2 — above threshold → eligible
# ---------------------------------------------------------------------------

def test_above_threshold_eligible() -> None:
    """sharpe=1.5, dd=0.04, trades=120 — all pass → eligible=True."""
    eligible, reasons, details = evaluate(
        _GOOD_METRICS, criteria=_CRITERIA, drift_breaches={}, risk_incident_count=0
    )
    assert eligible, f"Expected eligible=True, got reasons={reasons}"
    assert reasons == []
    assert details["survivors"] == 1
    assert details["timeframe_results"]["1D"]["pass"] is True


# ---------------------------------------------------------------------------
# 3 — missing metric → fail-closed
# ---------------------------------------------------------------------------

def test_missing_metric_fails_closed() -> None:
    """Missing 'sharpe' key → eligible=False, MISSING_METRIC:sharpe."""
    incomplete = {"1D": {"max_drawdown": 0.04, "n_trades": 100}}  # sharpe absent
    eligible, reasons, details = evaluate(
        incomplete, criteria=_CRITERIA, drift_breaches={}, risk_incident_count=0
    )
    assert not eligible
    tf_reasons = details["timeframe_results"]["1D"]["reasons"]
    assert any("MISSING_METRIC:sharpe" in r for r in tf_reasons), (
        f"Expected MISSING_METRIC:sharpe in tf_reasons={tf_reasons}"
    )
    assert any("INSUFFICIENT_SURVIVORS" in r for r in reasons)


# ---------------------------------------------------------------------------
# 4 — drift breach → fail-closed
# ---------------------------------------------------------------------------

def test_drift_breach_from_registry_file_blocks(tmp_path: Path) -> None:
    """Drift registry file with disabled=False → active breach → eligible=False."""
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    # disabled=False means active breach per drift_monitor.py semantics
    (drift_dir / "AAPL_1D.json").write_text(
        json.dumps({"streak": 10, "disabled": False, "kpi": -999.0}),
        encoding="utf-8",
    )
    breached = load_drift_state("AAPL", "1D", drift_registry_dir=drift_dir)
    assert breached is True, "disabled=False must be a breach"

    # Evaluate with drift check enabled
    result = evaluate_promotion(
        "AAPL",
        metrics_by_tf=_GOOD_METRICS,
        criteria=_CRITERIA,
        drift_registry_dir=drift_dir,
        evidence_dir=tmp_path / "ev",
    )
    assert not result.eligible_for_live
    assert any("DRIFT_BREACH" in r for r in result.reasons)


def test_no_drift_file_not_breached(tmp_path: Path) -> None:
    """No drift file for model → not breached (first run, never evaluated)."""
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    assert load_drift_state("NEWSTOCK", "1D", drift_registry_dir=drift_dir) is False


def test_drift_admin_suppressed_not_blocked(tmp_path: Path) -> None:
    """disabled=True → admin-suppressed → not a breach."""
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    (drift_dir / "SPY_1D.json").write_text(
        json.dumps({"streak": 0, "disabled": True, "kpi": 0.0}),
        encoding="utf-8",
    )
    assert load_drift_state("SPY", "1D", drift_registry_dir=drift_dir) is False


# ---------------------------------------------------------------------------
# 5 — risk incident present
# ---------------------------------------------------------------------------

def test_risk_incident_blocks_eligibility() -> None:
    """risk_incident_count=1 (> risk_incidents_max=0) → RISK_INCIDENTS_PRESENT."""
    eligible, reasons, details = evaluate(
        _GOOD_METRICS, criteria=_CRITERIA, drift_breaches={}, risk_incident_count=1
    )
    assert not eligible
    assert any("RISK_INCIDENTS_PRESENT:1" in r for r in reasons), (
        f"Expected RISK_INCIDENTS_PRESENT:1, got {reasons}"
    )


def test_zero_risk_incidents_allowed() -> None:
    """risk_incident_count=0 → no block."""
    eligible, reasons, _ = evaluate(
        _GOOD_METRICS, criteria=_CRITERIA, drift_breaches={}, risk_incident_count=0
    )
    assert eligible, f"Expected eligible, got {reasons}"


# ---------------------------------------------------------------------------
# 6 — registry mismatch → fail-closed
# ---------------------------------------------------------------------------

def test_registry_mismatch_raises(tmp_path: Path) -> None:
    """SQLite=RESEARCH vs JSONL=PAPER → ValueError(REGISTRY_MISMATCH)."""
    db_path = tmp_path / "registry.sqlite3"
    jsonl_path = tmp_path / "registry.jsonl"
    _make_sqlite(db_path, "AAPL", "1D", "RESEARCH")
    _make_jsonl(jsonl_path, "AAPL", "1D", "PAPER")

    with pytest.raises(ValueError, match="REGISTRY_MISMATCH"):
        load_registry_state("AAPL", "1D", sqlite_path=db_path, jsonl_path=jsonl_path)


def test_registry_mismatch_in_evaluate_promotion(tmp_path: Path) -> None:
    """evaluate_promotion → eligible=False + REGISTRY_MISMATCH reason."""
    db_path = tmp_path / "registry.sqlite3"
    jsonl_path = tmp_path / "registry.jsonl"
    _make_sqlite(db_path, "MSFT", "1D", "RESEARCH")
    _make_jsonl(jsonl_path, "MSFT", "1D", "LIVE")

    result = evaluate_promotion(
        "MSFT",
        metrics_by_tf=_GOOD_METRICS,
        criteria=_CRITERIA,
        sqlite_path=db_path,
        jsonl_path=jsonl_path,
        evidence_dir=tmp_path / "ev",
    )
    assert not result.eligible_for_live
    assert any("REGISTRY_MISMATCH" in r for r in result.reasons)
    # Decision file must be written even on mismatch
    assert (tmp_path / "ev" / "promotion_decision.json").exists()


def test_registry_agreement_passes(tmp_path: Path) -> None:
    """SQLite=PAPER and JSONL=PAPER → no mismatch, status=PAPER."""
    db_path = tmp_path / "registry.sqlite3"
    jsonl_path = tmp_path / "registry.jsonl"
    _make_sqlite(db_path, "IBM", "1D", "PAPER")
    _make_jsonl(jsonl_path, "IBM", "1D", "PAPER")

    status, details = load_registry_state("IBM", "1D", sqlite_path=db_path, jsonl_path=jsonl_path)
    assert status == "PAPER"
    assert details["sqlite_status"] == "PAPER"
    assert details["jsonl_status"] == "PAPER"


# ---------------------------------------------------------------------------
# 7 — determinism: same inputs → sha256 self-consistent
# ---------------------------------------------------------------------------

def test_determinism_sha256_self_consistent(tmp_path: Path) -> None:
    """write_decision sha256 is the sha256 of the canonical JSON (excluding sha256 field)."""
    criteria = PromotionCriteria(
        min_sharpe=1.0, max_drawdown=0.10, min_trades=50,
        min_survivors=1, drift_guard_required=False, risk_incidents_max=0,
    )
    metrics = {"1D": {"sharpe": 1.2, "max_drawdown": 0.06, "n_trades": 80}}

    ev = tmp_path / "ev"
    result = evaluate_promotion("TSLA", metrics_by_tf=metrics, criteria=criteria, evidence_dir=ev)

    dec = json.loads((ev / "promotion_decision.json").read_text(encoding="utf-8"))
    stored_sha = dec.pop("decision_sha256")

    # Recompute
    expected = hashlib.sha256(
        json.dumps(dec, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    assert stored_sha == expected, (
        f"Stored sha256 {stored_sha!r} != recomputed {expected!r}"
    )
    # .sha256 sidecar must match
    sidecar = (ev / "promotion_decision.sha256").read_text(encoding="utf-8").strip()
    assert sidecar == stored_sha


def test_determinism_stable_eligible_field(tmp_path: Path) -> None:
    """Two evaluate_promotion calls with same metrics produce same eligible_for_live."""
    criteria = PromotionCriteria(
        min_sharpe=1.0, max_drawdown=0.10, min_trades=50,
        min_survivors=1, drift_guard_required=False, risk_incidents_max=0,
    )
    metrics = {"1D": {"sharpe": 1.2, "max_drawdown": 0.06, "n_trades": 80}}

    r1 = evaluate_promotion("GOOG", metrics_by_tf=metrics, criteria=criteria, evidence_dir=tmp_path / "e1")
    r2 = evaluate_promotion("GOOG", metrics_by_tf=metrics, criteria=criteria, evidence_dir=tmp_path / "e2")

    assert r1.eligible_for_live == r2.eligible_for_live
    assert r1.reasons == r2.reasons
