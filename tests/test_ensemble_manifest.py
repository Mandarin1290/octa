"""Tests for v0.1.0 ensemble_manifest.json writing in _promote_to_paper_ready."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

# Import the function under test
# _promote_to_paper_ready is a module-level function, not exported via __all__,
# so we import it from the module directly.
from octa.support.ops.run_full_cascade_training_from_parquets import (
    _promote_to_paper_ready,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_stage(tmp_path: Path, symbol: str, tf: str) -> dict:
    """Build a minimal stage dict with a real model file for testing."""
    model_dir = tmp_path / "models" / symbol / tf
    model_dir.mkdir(parents=True, exist_ok=True)
    model_file = model_dir / "model.cbm"
    model_file.write_bytes(b"fake_model_bytes")
    return {
        "timeframe": tf,
        "model_artifacts": [str(model_file)],
        "features_used": ["feat_a", "feat_b"],
        "metrics_summary": {"sharpe": 1.5, "profit_factor": 1.3},
        "training_window": {"start": "2020-01-01", "end": "2024-12-31"},
    }


# ---------------------------------------------------------------------------
# Test 1: ensemble_manifest.json is created at symbol level
# ---------------------------------------------------------------------------

def test_manifest_created(tmp_path):
    symbol = "AAPL"
    stage_1d = _make_fake_stage(tmp_path / "source", symbol, "1D")
    stage_1h = _make_fake_stage(tmp_path / "source", symbol, "1H")

    paper_root = tmp_path / "paper_ready"
    _promote_to_paper_ready(symbol, [stage_1d, stage_1h], paper_root, run_id="test_run_001")

    manifest_path = paper_root / symbol / "ensemble_manifest.json"
    assert manifest_path.exists(), "ensemble_manifest.json not found at symbol level"


# ---------------------------------------------------------------------------
# Test 2: manifest schema_version is v0.1.0
# ---------------------------------------------------------------------------

def test_manifest_schema_version(tmp_path):
    symbol = "MSFT"
    stage = _make_fake_stage(tmp_path / "source", symbol, "1D")
    paper_root = tmp_path / "paper_ready"
    _promote_to_paper_ready(symbol, [stage], paper_root, run_id="test_run_002")

    manifest = json.loads((paper_root / symbol / "ensemble_manifest.json").read_text())
    assert manifest["schema_version"] == "v0.1.0"
    assert manifest["architecture"] == "regime_ensemble"


# ---------------------------------------------------------------------------
# Test 3: manifest contains correct symbol, timeframes, run_id
# ---------------------------------------------------------------------------

def test_manifest_contents(tmp_path):
    symbol = "GOOG"
    stage_1d = _make_fake_stage(tmp_path / "source", symbol, "1D")
    stage_1h = _make_fake_stage(tmp_path / "source", symbol, "1H")
    paper_root = tmp_path / "paper_ready"
    run_id = "test_run_003"

    _promote_to_paper_ready(symbol, [stage_1d, stage_1h], paper_root, run_id=run_id)

    manifest = json.loads((paper_root / symbol / "ensemble_manifest.json").read_text())
    assert manifest["symbol"] == symbol
    assert set(manifest["timeframes"]) == {"1D", "1H"}
    assert manifest["run_id"] == run_id
    assert "created_at" in manifest


# ---------------------------------------------------------------------------
# Test 4: manifest path included in returned out_paths
# ---------------------------------------------------------------------------

def test_manifest_in_out_paths(tmp_path):
    symbol = "TSLA"
    stage = _make_fake_stage(tmp_path / "source", symbol, "1D")
    paper_root = tmp_path / "paper_ready"

    out_paths = _promote_to_paper_ready(symbol, [stage], paper_root, run_id="test_run_004")

    manifest_path = str(paper_root / symbol / "ensemble_manifest.json")
    assert manifest_path in out_paths, f"ensemble_manifest.json not in out_paths: {out_paths}"


# ---------------------------------------------------------------------------
# Test 5: per_tf_metrics populated in manifest
# ---------------------------------------------------------------------------

def test_manifest_per_tf_metrics(tmp_path):
    symbol = "NVDA"
    stage_1d = _make_fake_stage(tmp_path / "source", symbol, "1D")
    paper_root = tmp_path / "paper_ready"
    _promote_to_paper_ready(symbol, [stage_1d], paper_root, run_id="test_run_005")

    manifest = json.loads((paper_root / symbol / "ensemble_manifest.json").read_text())
    assert "per_tf_metrics" in manifest
    assert "1D" in manifest["per_tf_metrics"]
    tf_metrics = manifest["per_tf_metrics"]["1D"]
    assert tf_metrics.get("sharpe") == 1.5
