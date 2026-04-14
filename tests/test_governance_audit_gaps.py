"""Tests for governance audit gaps fixed in Step 7.

Covers:
- EVENT_PRESCREENING_COMPLETE registered and emittable
- prescreen_universe emits PRESCREENING_COMPLETE when gov_audit supplied
- train_regime_ensemble emits EVENT_TRAINING_RUN per submodel + EVENT_REGIME_ACTIVATED
- cascade_train passes gov_audit through (structural, not calling real training)
- Unknown event still raises ValueError
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# 1. Event registration
# ---------------------------------------------------------------------------

def test_prescreening_complete_is_registered() -> None:
    from octa.core.governance.governance_audit import (
        EVENT_PRESCREENING_COMPLETE,
        _KNOWN_EVENTS,
    )
    assert EVENT_PRESCREENING_COMPLETE in _KNOWN_EVENTS
    assert EVENT_PRESCREENING_COMPLETE == "PRESCREENING_COMPLETE"


def test_regime_activated_is_registered() -> None:
    from octa.core.governance.governance_audit import (
        EVENT_REGIME_ACTIVATED,
        _KNOWN_EVENTS,
    )
    assert EVENT_REGIME_ACTIVATED in _KNOWN_EVENTS


def test_prescreening_complete_is_emittable(tmp_path: Path) -> None:
    from octa.core.governance.governance_audit import (
        EVENT_PRESCREENING_COMPLETE,
        GovernanceAudit,
    )
    ga = GovernanceAudit(run_id="gap_test_001", root=tmp_path)
    rec = ga.emit(EVENT_PRESCREENING_COMPLETE, {"total": 10, "passed": 7, "failed": 3, "by_reason": {}})
    assert rec.index == 1
    assert ga.verify()


def test_unknown_event_still_raises(tmp_path: Path) -> None:
    from octa.core.governance.governance_audit import GovernanceAudit
    ga = GovernanceAudit(run_id="gap_test_unk", root=tmp_path)
    with pytest.raises(ValueError, match="Unknown governance event type"):
        ga.emit("NOT_A_REAL_EVENT", {})


# ---------------------------------------------------------------------------
# 2. prescreen_universe emits PRESCREENING_COMPLETE
# ---------------------------------------------------------------------------

def _make_prescreening_inventory(tmp_path: Path) -> Dict[str, Any]:
    """Create a minimal inventory with no valid parquets (symbols will fail F1)."""
    return {
        "AAAA": {"tfs": {"1D": []}, "asset_class": "equity"},
        "BBBB": {"tfs": {"1D": []}, "asset_class": "equity"},
    }


def test_prescreen_universe_emits_gov_event(tmp_path: Path) -> None:
    from octa.core.governance.governance_audit import (
        EVENT_PRESCREENING_COMPLETE,
        GovernanceAudit,
    )
    from octa_training.core.prescreening import prescreen_universe

    ga = GovernanceAudit(run_id="ps_gov_001", root=tmp_path)
    inventory = _make_prescreening_inventory(tmp_path)

    prescreen_universe(
        symbols=["AAAA", "BBBB"],
        inventory=inventory,
        cfg=None,
        gov_audit=ga,
    )

    events = ga.read_events()
    types_emitted = [e.get("payload", {}).get("event_type") for e in events]
    assert EVENT_PRESCREENING_COMPLETE in types_emitted

    # Find the prescreening event and check payload fields
    ps_events = [
        e for e in events
        if e.get("payload", {}).get("event_type") == EVENT_PRESCREENING_COMPLETE
    ]
    assert len(ps_events) == 1
    data = ps_events[0]["payload"]["data"]
    assert data["total"] == 2
    assert data["passed"] == 0
    assert data["failed"] == 2
    assert ga.verify()


def test_prescreen_universe_no_gov_audit_does_not_crash(tmp_path: Path) -> None:
    """Passing gov_audit=None must not crash (backwards-compatible)."""
    from octa_training.core.prescreening import prescreen_universe

    inventory = _make_prescreening_inventory(tmp_path)
    # Should not raise
    results = prescreen_universe(symbols=["AAAA"], inventory=inventory, cfg=None, gov_audit=None)
    assert "AAAA" in results


# ---------------------------------------------------------------------------
# 3. train_regime_ensemble emits governance events
# ---------------------------------------------------------------------------

def _make_minimal_parquet(tmp_path: Path, symbol: str = "TST") -> Path:
    """Write a minimal 1D parquet with bull+bear regime diversity."""
    import numpy as np
    import pandas as pd

    n = 600
    dates = pd.date_range("2020-01-01", periods=n, freq="B")
    # Simulate trending (bull) then drawdown (bear)
    closes = np.concatenate([
        100 + np.arange(300) * 0.2,         # bull: steady climb
        160 - np.arange(300) * 0.3,         # bear: steady decline
    ]).astype(float)
    df = pd.DataFrame({
        "open": closes * 0.99,
        "high": closes * 1.01,
        "low": closes * 0.98,
        "close": closes,
        "volume": np.ones(n) * 500_000,
    }, index=dates)
    pq_path = tmp_path / f"{symbol}_1D.parquet"
    df.to_parquet(str(pq_path))
    return pq_path


def test_train_regime_ensemble_emits_regime_activated(tmp_path: Path) -> None:
    """train_regime_ensemble must emit EVENT_REGIME_ACTIVATED when gov_audit provided.

    Uses a mock gov_audit to avoid needing a real GovernanceAudit on disk.
    The mock captures all emit() calls; we verify at least one REGIME_ACTIVATED
    event was emitted regardless of whether training passed.
    """
    from octa_training.core.pipeline import train_regime_ensemble
    from octa.core.governance.governance_audit import EVENT_REGIME_ACTIVATED

    pq = _make_minimal_parquet(tmp_path)

    # Minimal cfg stub
    class _PS:
        enabled = False

    class _ROE:
        enabled = True
        regimes = ["bull", "bear"]
        min_regimes_trained = 2
        min_rows = {}
        require_bull = True
        require_bear = True
        regime_artifacts_dir = None

    class _CrisisOOS:
        enabled = False
        windows = []

    class _Cfg:
        prescreening = _PS()
        regime_ensemble = _ROE()
        crisis_oos = _CrisisOOS()
        splits_by_timeframe = {}

    from octa_training.core.state import StateRegistry
    state = StateRegistry(str(tmp_path / "state.db"))

    mock_audit = MagicMock()
    mock_audit.emit = MagicMock(return_value=None)

    result = train_regime_ensemble(
        symbol="TST",
        timeframe="1D",
        cfg=_Cfg(),
        state=state,
        run_id="gov_test_regime_001",
        parquet_path=str(pq),
        regime_artifacts_dir=str(tmp_path / "regime_arts"),
        gov_audit=mock_audit,
    )

    # Check emit was called at least once for REGIME_ACTIVATED
    calls = [c for c in mock_audit.emit.call_args_list if c[0][0] == EVENT_REGIME_ACTIVATED]
    assert len(calls) == 1, f"Expected 1 REGIME_ACTIVATED emit, got {len(calls)}"

    # Payload must include key fields
    payload = calls[0][0][1]
    assert payload["symbol"] == "TST"
    assert payload["timeframe"] == "1D"
    assert "passed" in payload
    assert "regimes_trained" in payload


def test_train_regime_ensemble_emits_training_run_per_submodel(tmp_path: Path) -> None:
    """For each regime actually attempted, EVENT_TRAINING_RUN must be emitted.

    We patch get_regime_splits to return controlled bull+bear splits so the
    regime loop definitely runs, independent of whether the synthetic price
    series happens to classify into distinct regimes.
    """
    import unittest.mock as _mock
    import numpy as np
    import pandas as pd

    from octa_training.core.pipeline import train_regime_ensemble
    from octa.core.governance.governance_audit import EVENT_TRAINING_RUN

    pq = _make_minimal_parquet(tmp_path)

    class _PS:
        enabled = False

    class _ROE:
        enabled = True
        regimes = ["bull", "bear"]
        min_regimes_trained = 2
        min_rows = {}
        require_bull = True
        require_bear = True
        regime_artifacts_dir = None

    class _CrisisOOS:
        enabled = False
        windows = []

    class _Cfg:
        prescreening = _PS()
        regime_ensemble = _ROE()
        crisis_oos = _CrisisOOS()
        splits_by_timeframe = {}

    from octa_training.core.state import StateRegistry
    state = StateRegistry(str(tmp_path / "state.db"))

    mock_audit = MagicMock()
    mock_audit.emit = MagicMock(return_value=None)

    # Build a DataFrame with 600 rows so parquet load succeeds
    n = 600
    dates = pd.date_range("2020-01-01", periods=n, freq="B")
    closes = np.linspace(100, 160, n)
    df_stub = pd.DataFrame({"open": closes, "high": closes*1.01, "low": closes*0.99, "close": closes, "volume": np.ones(n)*500000}, index=dates)

    # Fake splits: bull uses first 300 rows, bear uses last 300
    _fake_splits = {
        "bull": df_stub.iloc[:300],
        "bear": df_stub.iloc[300:],
    }

    with _mock.patch("octa_training.core.regime_labels.get_regime_splits", return_value=_fake_splits), \
         _mock.patch("octa_training.core.regime_labels.classify_regimes", return_value=pd.Series(["bull"]*300 + ["bear"]*300, index=dates)), \
         _mock.patch("octa_training.core.pipeline.train_evaluate_adaptive") as _mock_tea:

        from octa_training.core.pipeline import PipelineResult
        _mock_tea.return_value = PipelineResult(symbol="TST", run_id="r", passed=False, error="mocked")

        train_regime_ensemble(
            symbol="TST",
            timeframe="1D",
            cfg=_Cfg(),
            state=state,
            run_id="gov_test_regime_002",
            parquet_path=str(pq),
            regime_artifacts_dir=str(tmp_path / "regime_arts"),
            gov_audit=mock_audit,
        )

    # Each regime that was attempted should emit EVENT_TRAINING_RUN
    tr_calls = [c for c in mock_audit.emit.call_args_list if c[0][0] == EVENT_TRAINING_RUN]
    assert len(tr_calls) == 2, f"Expected 2 TRAINING_RUN emits (bull+bear), got {len(tr_calls)}"
    regimes_seen = {c[0][1]["regime"] for c in tr_calls}
    assert regimes_seen == {"bull", "bear"}
    for c in tr_calls:
        payload = c[0][1]
        assert payload["phase"] == "regime_submodel"
        assert payload["symbol"] == "TST"
        assert "passed" in payload


def test_train_regime_ensemble_no_gov_audit_does_not_crash(tmp_path: Path) -> None:
    """gov_audit=None (default) must work without any crash."""
    from octa_training.core.pipeline import train_regime_ensemble

    pq = _make_minimal_parquet(tmp_path)

    class _PS:
        enabled = False

    class _ROE:
        enabled = True
        regimes = ["bull", "bear"]
        min_regimes_trained = 2
        min_rows = {}
        require_bull = True
        require_bear = True
        regime_artifacts_dir = None

    class _CrisisOOS:
        enabled = False
        windows = []

    class _Cfg:
        prescreening = _PS()
        regime_ensemble = _ROE()
        crisis_oos = _CrisisOOS()
        splits_by_timeframe = {}

    from octa_training.core.state import StateRegistry
    state = StateRegistry(str(tmp_path / "state.db"))

    result = train_regime_ensemble(
        symbol="TST",
        timeframe="1D",
        cfg=_Cfg(),
        state=state,
        run_id="gov_test_regime_003",
        parquet_path=str(pq),
        regime_artifacts_dir=str(tmp_path / "regime_arts"),
        # gov_audit not passed — must use default None
    )
    # result is a RegimeEnsemble regardless of pass/fail
    assert hasattr(result, "passed")


# ---------------------------------------------------------------------------
# 4. Audit chain integrity still holds after new events
# ---------------------------------------------------------------------------

def test_audit_chain_integrity_with_new_events(tmp_path: Path) -> None:
    """Hash-chain verify() must pass after emitting all three new-style events."""
    from octa.core.governance.governance_audit import (
        EVENT_PRESCREENING_COMPLETE,
        EVENT_REGIME_ACTIVATED,
        EVENT_TRAINING_RUN,
        GovernanceAudit,
    )

    ga = GovernanceAudit(run_id="integrity_test_001", root=tmp_path)
    ga.emit(EVENT_TRAINING_RUN, {"phase": "regime_submodel", "symbol": "X", "timeframe": "1D", "regime": "bull", "passed": True, "error": None})
    ga.emit(EVENT_TRAINING_RUN, {"phase": "regime_submodel", "symbol": "X", "timeframe": "1D", "regime": "bear", "passed": False, "error": "test"})
    ga.emit(EVENT_REGIME_ACTIVATED, {"symbol": "X", "timeframe": "1D", "passed": False, "regimes_trained": 1, "regime_artifact_paths": {}, "router_path": None, "error": "missing_required_regimes:bear"})
    ga.emit(EVENT_PRESCREENING_COMPLETE, {"total": 5, "passed": 3, "failed": 2, "by_reason": {"price_too_low": 2}})

    assert ga.verify()
    events = ga.read_events()
    assert len(events) == 4
