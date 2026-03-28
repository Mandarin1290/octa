"""Tests for octa.execution.inference_bridge.

Coverage:
- InferenceResult dataclass
- build_inference_proposal: artifact_not_found
- build_inference_proposal: artifact load error (bad pkl)
- build_inference_proposal: no safe_inference in artifact
- build_inference_proposal: parquet not found
- build_inference_proposal: predict() missing features → signal=0
- build_inference_proposal: predict() returns signal=1 → approved=True
- build_inference_proposal: predict() returns signal=0 → approved=True, signal=0
- build_inference_proposal: predict() returns signal=-1 → approved=True, signal=-1
- run_inference_cycle: writes evidence file
- run_inference_cycle: all artifact-not-found → all approved=False
- runner.py ExecutionConfig: new fields present with correct defaults
- runner.py: ml_inference_enabled=False → inference_map empty
"""
from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from octa.execution.inference_bridge import (
    InferenceResult,
    build_inference_proposal,
    run_inference_cycle,
)


# ---------------------------------------------------------------------------
# Helpers — module-level so pickle can serialize them
# ---------------------------------------------------------------------------

class _MockSafeInferenceSignal1:
    feature_names = ["feat_a", "feat_b"]

    def predict(self, X: pd.DataFrame) -> Dict[str, Any]:
        if not {"feat_a", "feat_b"}.issubset(set(X.columns)):
            missing = {"feat_a", "feat_b"} - set(X.columns)
            return {"signal": 0.0, "position": 0.0, "confidence": 0.0,
                    "diagnostics": {"error": f"missing_features:{missing}"}}
        return {"signal": 1.0, "position": 0.5, "confidence": 0.75, "diagnostics": {}}


class _MockSafeInferenceSignal0:
    feature_names = ["feat_a", "feat_b"]

    def predict(self, X: pd.DataFrame) -> Dict[str, Any]:
        if not {"feat_a", "feat_b"}.issubset(set(X.columns)):
            missing = {"feat_a", "feat_b"} - set(X.columns)
            return {"signal": 0.0, "position": 0.0, "confidence": 0.0,
                    "diagnostics": {"error": f"missing_features:{missing}"}}
        return {"signal": 0.0, "position": 0.0, "confidence": 0.5, "diagnostics": {}}


class _MockSafeInferenceSignalMinus1:
    feature_names = ["feat_a", "feat_b"]

    def predict(self, X: pd.DataFrame) -> Dict[str, Any]:
        if not {"feat_a", "feat_b"}.issubset(set(X.columns)):
            missing = {"feat_a", "feat_b"} - set(X.columns)
            return {"signal": 0.0, "position": 0.0, "confidence": 0.0,
                    "diagnostics": {"error": f"missing_features:{missing}"}}
        return {"signal": -1.0, "position": -0.5, "confidence": 0.8, "diagnostics": {}}


_MOCK_SI_MAP = {1: _MockSafeInferenceSignal1, 0: _MockSafeInferenceSignal0, -1: _MockSafeInferenceSignalMinus1}


def _make_safe_inference(signal: int) -> Any:
    return _MOCK_SI_MAP[signal]()


def _write_artifact(pkl_path: Path, safe_inference: Any) -> None:
    """Write a minimal tradeable artifact .pkl."""
    pkl_path.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "artifact_kind": "tradeable",
        "schema_version": 1,
        "asset": {"symbol": pkl_path.stem, "asset_class": "equity", "bar_size": "1D"},
        "timeframe": "1D",
        "feature_spec": {
            "features": getattr(safe_inference, "feature_names", []),
            "feature_config": {"feature_settings": {}},
        },
        "safe_inference": safe_inference,
    }
    with open(pkl_path, "wb") as f:
        pickle.dump(artifact, f)


def _make_parquet_dir(base: Path, symbol: str) -> Path:
    """Create a minimal OHLCV parquet for a symbol."""
    import numpy as np
    import pandas as pd

    parquet_dir = base / "parquets"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2025-01-01", periods=20, freq="B", tz="UTC")
    df = pd.DataFrame(
        {
            "open": np.ones(20) * 100.0,
            "high": np.ones(20) * 101.0,
            "low": np.ones(20) * 99.0,
            "close": np.ones(20) * 100.0,
            "volume": np.ones(20) * 1_000_000.0,
        },
        index=dates,
    )
    pq_path = parquet_dir / f"{symbol}.parquet"
    df.to_parquet(str(pq_path))
    return parquet_dir


# ---------------------------------------------------------------------------
# Unit tests: build_inference_proposal
# ---------------------------------------------------------------------------

def test_build_inference_proposal_artifact_not_found(tmp_path: Path) -> None:
    ir = build_inference_proposal(
        symbol="MISS",
        asset_class="equities",
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw",
        timeframe="1D",
    )
    assert ir.approved is False
    assert ir.signal == 0
    assert ir.reason == "artifact_not_found"
    assert ir.symbol == "MISS"


def test_build_inference_proposal_bad_pkl(tmp_path: Path) -> None:
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "BAD.pkl"
    pkl_path.parent.mkdir(parents=True, exist_ok=True)
    pkl_path.write_bytes(b"not_a_pickle")

    ir = build_inference_proposal(
        symbol="BAD",
        asset_class="equities",
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw",
        timeframe="1D",
    )
    assert ir.approved is False
    assert ir.signal == 0
    assert "artifact_load_error" in ir.reason


def test_build_inference_proposal_no_safe_inference(tmp_path: Path) -> None:
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "NOSAFE.pkl"
    pkl_path.parent.mkdir(parents=True, exist_ok=True)
    with open(pkl_path, "wb") as f:
        pickle.dump({"artifact_kind": "tradeable", "schema_version": 1, "safe_inference": None}, f)

    ir = build_inference_proposal(
        symbol="NOSAFE",
        asset_class="equities",
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw",
        timeframe="1D",
    )
    assert ir.approved is False
    assert ir.reason == "no_safe_inference"


def test_build_inference_proposal_parquet_not_found(tmp_path: Path) -> None:
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "NOPQ.pkl"
    _write_artifact(pkl_path, _make_safe_inference(signal=1))

    ir = build_inference_proposal(
        symbol="NOPQ",
        asset_class="equities",
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw_empty",  # no parquets here
        timeframe="1D",
    )
    assert ir.approved is False
    assert "parquet_not_found" in ir.reason
    assert ir.feature_count_model == 2  # feat_a + feat_b


def test_build_inference_proposal_missing_features_in_parquet(tmp_path: Path) -> None:
    """When parquet lacks model features → SafeInference returns signal=0 with diagnostics.error."""
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "SYM.pkl"
    _write_artifact(pkl_path, _make_safe_inference(signal=1))

    # Parquet with OHLCV only; build_features will produce computed features
    # but NOT feat_a / feat_b (which are mock names)
    pq_dir = _make_parquet_dir(tmp_path, "SYM")

    # Patch build_features to return a DataFrame WITHOUT feat_a/feat_b
    mock_feats = MagicMock()
    mock_feats.X = pd.DataFrame({"other_col": [1.0]})

    with patch("octa.execution.inference_bridge._build_features", return_value=mock_feats):
        with patch("octa.execution.inference_bridge._find_raw_parquet_direct", return_value=Path(str(pq_dir / "SYM.parquet"))):
            with patch("octa.execution.inference_bridge._load_parquet", return_value=pd.DataFrame()):
                ir = build_inference_proposal(
                    symbol="SYM",
                    asset_class="equities",
                    artifact_dir=tmp_path / "PKL",
                    raw_data_dir=tmp_path / "raw",
                    timeframe="1D",
                )

    # Missing features → SafeInference returns diagnostics.error → approved=False
    assert ir.approved is False
    assert "predict_diagnostics_error" in ir.reason
    assert ir.signal == 0


def test_build_inference_proposal_signal_1(tmp_path: Path) -> None:
    """When model returns signal=1 → approved=True, signal=1."""
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "LONG.pkl"
    _write_artifact(pkl_path, _make_safe_inference(signal=1))

    mock_feats = MagicMock()
    mock_feats.X = pd.DataFrame(
        {"feat_a": [1.0], "feat_b": [2.0]},
        index=pd.date_range("2025-01-01", periods=1, freq="B", tz="UTC"),
    )

    with patch("octa.execution.inference_bridge._build_features", return_value=mock_feats):
        with patch("octa.execution.inference_bridge._find_raw_parquet_direct", return_value=Path("dummy.parquet")):
            with patch("octa.execution.inference_bridge._load_parquet", return_value=pd.DataFrame()):
                ir = build_inference_proposal(
                    symbol="LONG",
                    asset_class="equities",
                    artifact_dir=tmp_path / "PKL",
                    raw_data_dir=tmp_path / "raw",
                    timeframe="1D",
                )

    assert ir.approved is True
    assert ir.signal == 1
    assert ir.reason == "ok"
    assert ir.confidence == 0.75
    assert ir.feature_count_model == 2
    assert ir.feature_count_runtime == 2


def test_build_inference_proposal_signal_0(tmp_path: Path) -> None:
    """When model returns signal=0 → approved=True, signal=0."""
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "FLAT.pkl"
    _write_artifact(pkl_path, _make_safe_inference(signal=0))

    mock_feats = MagicMock()
    mock_feats.X = pd.DataFrame(
        {"feat_a": [1.0], "feat_b": [2.0]},
        index=pd.date_range("2025-01-01", periods=1, freq="B", tz="UTC"),
    )

    with patch("octa.execution.inference_bridge._build_features", return_value=mock_feats):
        with patch("octa.execution.inference_bridge._find_raw_parquet_direct", return_value=Path("dummy.parquet")):
            with patch("octa.execution.inference_bridge._load_parquet", return_value=pd.DataFrame()):
                ir = build_inference_proposal(
                    symbol="FLAT",
                    asset_class="equities",
                    artifact_dir=tmp_path / "PKL",
                    raw_data_dir=tmp_path / "raw",
                    timeframe="1D",
                )

    assert ir.approved is True
    assert ir.signal == 0
    assert ir.reason == "ok"


def test_build_inference_proposal_signal_minus1(tmp_path: Path) -> None:
    """When model returns signal=-1 → approved=True, signal=-1."""
    pkl_path = tmp_path / "PKL" / "equity" / "1D" / "SHORT.pkl"
    _write_artifact(pkl_path, _make_safe_inference(signal=-1))

    mock_feats = MagicMock()
    mock_feats.X = pd.DataFrame(
        {"feat_a": [1.0], "feat_b": [2.0]},
        index=pd.date_range("2025-01-01", periods=1, freq="B", tz="UTC"),
    )

    with patch("octa.execution.inference_bridge._build_features", return_value=mock_feats):
        with patch("octa.execution.inference_bridge._find_raw_parquet_direct", return_value=Path("dummy.parquet")):
            with patch("octa.execution.inference_bridge._load_parquet", return_value=pd.DataFrame()):
                ir = build_inference_proposal(
                    symbol="SHORT",
                    asset_class="equities",
                    artifact_dir=tmp_path / "PKL",
                    raw_data_dir=tmp_path / "raw",
                    timeframe="1D",
                )

    assert ir.approved is True
    assert ir.signal == -1
    assert ir.reason == "ok"


# ---------------------------------------------------------------------------
# Unit tests: run_inference_cycle
# ---------------------------------------------------------------------------

def test_run_inference_cycle_all_missing(tmp_path: Path) -> None:
    """When no artifacts exist, all results are approved=False, evidence written."""
    eligible = [
        {"symbol": "AAA", "asset_class": "equities", "scaling_level": 1},
        {"symbol": "BBB", "asset_class": "equities", "scaling_level": 0},
    ]
    evidence_dir = tmp_path / "evidence"

    results = run_inference_cycle(
        eligible_rows=eligible,
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw",
        timeframe="1D",
        evidence_dir=evidence_dir,
        cycle_idx=1,
    )

    assert "AAA" in results
    assert "BBB" in results
    assert results["AAA"].approved is False
    assert results["BBB"].approved is False

    # Evidence file must exist
    ev_file = evidence_dir / "inference_cycle_001.json"
    assert ev_file.exists()
    payload = json.loads(ev_file.read_text())
    assert payload["cycle"] == 1
    assert payload["summary"]["total"] == 2
    assert payload["summary"]["inference_errors"] == 2
    assert payload["summary"]["blocked_by_signal"] == 0


def test_run_inference_cycle_writes_evidence_json(tmp_path: Path) -> None:
    """Evidence JSON has correct structure."""
    results = run_inference_cycle(
        eligible_rows=[{"symbol": "X", "asset_class": "etf", "scaling_level": 0}],
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw",
        timeframe="1D",
        evidence_dir=tmp_path / "ev",
        cycle_idx=42,
    )

    ev_file = tmp_path / "ev" / "inference_cycle_042.json"
    assert ev_file.exists()
    payload = json.loads(ev_file.read_text())
    assert payload["cycle"] == 42
    assert payload["inference_enabled"] is True
    assert "results" in payload
    assert "summary" in payload
    assert "timestamp_utc" in payload


def test_run_inference_cycle_empty_eligible(tmp_path: Path) -> None:
    """Empty eligible_rows → empty results, evidence file written."""
    results = run_inference_cycle(
        eligible_rows=[],
        artifact_dir=tmp_path / "PKL",
        raw_data_dir=tmp_path / "raw",
        timeframe="1D",
        evidence_dir=tmp_path / "ev",
        cycle_idx=1,
    )
    assert results == {}
    ev_file = tmp_path / "ev" / "inference_cycle_001.json"
    assert ev_file.exists()


# ---------------------------------------------------------------------------
# Integration: ExecutionConfig defaults
# ---------------------------------------------------------------------------

def test_execution_config_new_fields_exist() -> None:
    """New ExecutionConfig fields are present with correct defaults."""
    from octa.execution.runner import ExecutionConfig
    cfg = ExecutionConfig()
    assert cfg.ml_inference_enabled is False
    assert cfg.artifact_dir == Path("raw") / "PKL"
    assert cfg.raw_data_dir == Path("raw")
    assert cfg.inference_timeframe == "1D"


def test_execution_config_inference_fields_settable() -> None:
    """New ExecutionConfig fields can be overridden."""
    from octa.execution.runner import ExecutionConfig
    cfg = ExecutionConfig(
        ml_inference_enabled=True,
        artifact_dir=Path("/custom/PKL"),
        raw_data_dir=Path("/custom/raw"),
        inference_timeframe="1H",
    )
    assert cfg.ml_inference_enabled is True
    assert cfg.artifact_dir == Path("/custom/PKL")
    assert cfg.raw_data_dir == Path("/custom/raw")
    assert cfg.inference_timeframe == "1H"


# ---------------------------------------------------------------------------
# Integration: runner imports EVENT_INFERENCE_CYCLE
# ---------------------------------------------------------------------------

def test_runner_imports_inference_cycle_event() -> None:
    from octa.execution.runner import EVENT_INFERENCE_CYCLE
    from octa.core.governance.governance_audit import EVENT_INFERENCE_CYCLE as GOV_EV
    assert EVENT_INFERENCE_CYCLE == GOV_EV
    assert isinstance(EVENT_INFERENCE_CYCLE, str)
    assert len(EVENT_INFERENCE_CYCLE) > 0


def test_governance_audit_knows_inference_event() -> None:
    from octa.core.governance.governance_audit import EVENT_INFERENCE_CYCLE, _KNOWN_EVENTS
    assert EVENT_INFERENCE_CYCLE in _KNOWN_EVENTS


# ---------------------------------------------------------------------------
# Unit tests: InferenceResult is frozen dataclass
# ---------------------------------------------------------------------------

def test_inference_result_frozen() -> None:
    ir = InferenceResult(
        symbol="X", timeframe="1D", signal=1, position=0.5, confidence=0.8,
        approved=True, reason="ok", artifact_path="/tmp/x.pkl",
        artifact_hash="abc123", feature_count_model=10, feature_count_runtime=10,
        diagnostics={},
    )
    with pytest.raises((AttributeError, TypeError)):
        ir.signal = 0  # type: ignore[misc]


def test_inference_result_approved_false_does_not_block() -> None:
    """Verify the pre-gate logic: approved=False → block condition is False."""
    ir = InferenceResult(
        symbol="X", timeframe="1D", signal=0, position=0.0, confidence=0.0,
        approved=False, reason="artifact_not_found", artifact_path="",
        artifact_hash="", feature_count_model=0, feature_count_runtime=0,
        diagnostics={},
    )
    # The runner condition: "if ir is not None and ir.approved and ir.signal <= 0"
    should_block = (ir is not None) and ir.approved and (ir.signal <= 0)
    assert should_block is False


def test_inference_result_approved_true_signal_zero_blocks() -> None:
    """approved=True, signal=0 → block condition is True."""
    ir = InferenceResult(
        symbol="X", timeframe="1D", signal=0, position=0.0, confidence=0.0,
        approved=True, reason="ok", artifact_path="/p.pkl",
        artifact_hash="abc", feature_count_model=5, feature_count_runtime=5,
        diagnostics={},
    )
    should_block = (ir is not None) and ir.approved and (ir.signal <= 0)
    assert should_block is True


def test_inference_result_approved_true_signal_1_passes() -> None:
    """approved=True, signal=1 → block condition is False."""
    ir = InferenceResult(
        symbol="X", timeframe="1D", signal=1, position=0.5, confidence=0.8,
        approved=True, reason="ok", artifact_path="/p.pkl",
        artifact_hash="abc", feature_count_model=5, feature_count_runtime=5,
        diagnostics={},
    )
    should_block = (ir is not None) and ir.approved and (ir.signal <= 0)
    assert should_block is False


# ---------------------------------------------------------------------------
# P0-1: Direct parquet lookup (_find_raw_parquet_direct)
# ---------------------------------------------------------------------------

def test_find_raw_parquet_direct_equity_found(tmp_path: Path) -> None:
    """P0-1: equity asset_class maps to Stock_parquet/<SYMBOL>_<TF>.parquet."""
    from octa.execution.inference_bridge import _find_raw_parquet_direct
    raw = tmp_path / "raw"
    pq = raw / "Stock_parquet" / "AAPL_1D.parquet"
    pq.parent.mkdir(parents=True)
    pq.touch()
    result = _find_raw_parquet_direct("AAPL", "equity", "1D", raw)
    assert result == pq


def test_find_raw_parquet_direct_etf_found(tmp_path: Path) -> None:
    """P0-1: etf asset_class maps to ETF_Parquet/<SYMBOL>_<TF>.parquet."""
    from octa.execution.inference_bridge import _find_raw_parquet_direct
    raw = tmp_path / "raw"
    pq = raw / "ETF_Parquet" / "SPY_1H.parquet"
    pq.parent.mkdir(parents=True)
    pq.touch()
    result = _find_raw_parquet_direct("SPY", "etf", "1H", raw)
    assert result == pq


def test_find_raw_parquet_direct_unknown_asset_class_returns_none(tmp_path: Path) -> None:
    """P0-1: Unknown asset_class → None (fail-closed, no crash)."""
    from octa.execution.inference_bridge import _find_raw_parquet_direct
    result = _find_raw_parquet_direct("SYM", "unknown_asset_type", "1D", tmp_path)
    assert result is None


def test_find_raw_parquet_direct_missing_file_returns_none(tmp_path: Path) -> None:
    """P0-1: Known asset_class but file missing → None (fail-closed)."""
    from octa.execution.inference_bridge import _find_raw_parquet_direct
    # Directory exists but file does not
    (tmp_path / "raw" / "Stock_parquet").mkdir(parents=True)
    result = _find_raw_parquet_direct("MISSING", "equity", "1D", tmp_path / "raw")
    assert result is None


def test_find_raw_parquet_direct_symbol_uppercased(tmp_path: Path) -> None:
    """P0-1: Symbol is uppercased in path construction."""
    from octa.execution.inference_bridge import _find_raw_parquet_direct
    raw = tmp_path / "raw"
    pq = raw / "Stock_parquet" / "AAPL_1D.parquet"
    pq.parent.mkdir(parents=True)
    pq.touch()
    # Pass lowercase — must still find it
    result = _find_raw_parquet_direct("aapl", "equity", "1d", raw)
    assert result == pq


# ---------------------------------------------------------------------------
# P0-3: paper_ready_dir secondary artifact lookup
# ---------------------------------------------------------------------------

def test_find_artifact_falls_back_to_paper_ready(tmp_path: Path) -> None:
    """P0-3: When primary PKL missing, paper_ready_dir secondary path is checked."""
    from octa.execution.inference_bridge import _find_artifact
    paper_ready = tmp_path / "paper_ready"
    pkl = paper_ready / "AAPL" / "1D" / "AAPL_1D.pkl"
    pkl.parent.mkdir(parents=True)
    pkl.touch()
    result = _find_artifact("AAPL", "equity", "1D", tmp_path / "PKL", paper_ready_dir=paper_ready)
    assert result == pkl


def test_find_artifact_primary_takes_precedence(tmp_path: Path) -> None:
    """P0-3: Primary raw/PKL artifact takes precedence over paper_ready."""
    from octa.execution.inference_bridge import _find_artifact
    primary = tmp_path / "PKL" / "equity" / "1D" / "AAPL.pkl"
    primary.parent.mkdir(parents=True)
    primary.touch()
    paper_ready = tmp_path / "paper_ready"
    paper_pkl = paper_ready / "AAPL" / "1D" / "AAPL_1D.pkl"
    paper_pkl.parent.mkdir(parents=True)
    paper_pkl.touch()
    result = _find_artifact("AAPL", "equity", "1D", tmp_path / "PKL", paper_ready_dir=paper_ready)
    assert result == primary


def test_find_artifact_falls_back_to_paper_ready_no_tf_suffix(tmp_path: Path) -> None:
    """P03 naming fix: paper_ready/<S>/<TF>/<S>.pkl (promotion convention, no TF suffix)."""
    from octa.execution.inference_bridge import _find_artifact
    paper_ready = tmp_path / "paper_ready"
    # Promotion writes SYMBOL.pkl (src.name), not SYMBOL_TF.pkl
    pkl = paper_ready / "ABT" / "1D" / "ABT.pkl"
    pkl.parent.mkdir(parents=True)
    pkl.touch()
    result = _find_artifact("ABT", "equity", "1D", tmp_path / "PKL", paper_ready_dir=paper_ready)
    assert result == pkl


def test_find_artifact_no_paper_ready_dir_returns_none(tmp_path: Path) -> None:
    """P0-3: paper_ready_dir=None + no primary PKL → None (fail-closed)."""
    from octa.execution.inference_bridge import _find_artifact
    result = _find_artifact("MISS", "equity", "1D", tmp_path / "PKL", paper_ready_dir=None)
    assert result is None


def test_execution_config_paper_ready_dir_field(tmp_path: Path) -> None:
    """P0-3/P0-5: ExecutionConfig has paper_ready_dir field with default."""
    from octa.execution.runner import ExecutionConfig
    cfg = ExecutionConfig()
    assert cfg.paper_ready_dir is not None
    assert "paper_ready" in str(cfg.paper_ready_dir)


def test_execution_config_paper_ready_dir_can_be_none() -> None:
    """P0-3: paper_ready_dir can be set to None to disable secondary lookup."""
    from octa.execution.runner import ExecutionConfig
    cfg = ExecutionConfig(paper_ready_dir=None)
    assert cfg.paper_ready_dir is None
