"""Tests for deterministic training fingerprint and smart checkpoints."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.core.orchestration.training_fingerprint import (
    CheckpointDecision,
    DataFingerprint,
    TrainingFingerprint,
    check_checkpoint,
    compute_data_fingerprint,
    compute_training_fingerprint,
    save_fingerprint,
)


def _make_fingerprint(**overrides: str) -> TrainingFingerprint:
    defaults = dict(
        config={"model_type": "catboost", "depth": 6},
        symbol="AAPL",
        timeframe="1D",
        window_start="2020-01-01",
        window_end="2025-12-31",
        global_end="2025-12-31",
        data_fingerprint=compute_data_fingerprint(
            row_count=1500,
            columns=["open", "high", "low", "close", "volume"],
            first_date="2020-01-02",
            last_date="2025-12-30",
        ),
        code_version="abc123",
    )
    defaults.update(overrides)
    return compute_training_fingerprint(**defaults)


def test_fingerprint_deterministic() -> None:
    fp1 = _make_fingerprint()
    fp2 = _make_fingerprint()
    assert fp1.fingerprint_hash == fp2.fingerprint_hash


def test_fingerprint_changes_on_symbol() -> None:
    fp1 = _make_fingerprint(symbol="AAPL")
    fp2 = _make_fingerprint(symbol="MSFT")
    assert fp1.fingerprint_hash != fp2.fingerprint_hash


def test_fingerprint_changes_on_timeframe() -> None:
    fp1 = _make_fingerprint(timeframe="1D")
    fp2 = _make_fingerprint(timeframe="1H")
    assert fp1.fingerprint_hash != fp2.fingerprint_hash


def test_fingerprint_changes_on_window() -> None:
    fp1 = _make_fingerprint(window_end="2025-12-31")
    fp2 = _make_fingerprint(window_end="2026-01-31")
    assert fp1.fingerprint_hash != fp2.fingerprint_hash


def test_fingerprint_changes_on_global_end() -> None:
    fp1 = _make_fingerprint(global_end="2025-12-31")
    fp2 = _make_fingerprint(global_end="2026-01-15")
    assert fp1.fingerprint_hash != fp2.fingerprint_hash


def test_fingerprint_changes_on_code_version() -> None:
    fp1 = _make_fingerprint(code_version="abc123")
    fp2 = _make_fingerprint(code_version="def456")
    assert fp1.fingerprint_hash != fp2.fingerprint_hash


def test_data_fingerprint_deterministic() -> None:
    dfp1 = compute_data_fingerprint(row_count=100, columns=["a", "b", "c"])
    dfp2 = compute_data_fingerprint(row_count=100, columns=["c", "a", "b"])
    # Sorted columns => same hash
    assert dfp1.column_hash == dfp2.column_hash


def test_data_fingerprint_changes_on_row_count() -> None:
    dfp1 = compute_data_fingerprint(row_count=100, columns=["a"])
    dfp2 = compute_data_fingerprint(row_count=200, columns=["a"])
    assert dfp1.row_count != dfp2.row_count


def test_checkpoint_skip_on_match(tmp_path: Path) -> None:
    fp = _make_fingerprint()
    fp_path = tmp_path / "fingerprint.json"
    save_fingerprint(fp, fp_path)

    decision = check_checkpoint(fp, fp_path)
    assert decision.action == "SKIP_CHECKPOINT_HIT"
    assert decision.reason == "fingerprint_match"


def test_checkpoint_retrain_on_mismatch(tmp_path: Path) -> None:
    fp1 = _make_fingerprint(symbol="AAPL")
    fp_path = tmp_path / "fingerprint.json"
    save_fingerprint(fp1, fp_path)

    fp2 = _make_fingerprint(symbol="MSFT")
    decision = check_checkpoint(fp2, fp_path)
    assert decision.action == "RETRAIN"
    assert decision.reason == "fingerprint_mismatch"


def test_checkpoint_retrain_no_stored(tmp_path: Path) -> None:
    fp = _make_fingerprint()
    decision = check_checkpoint(fp, tmp_path / "nonexistent.json")
    assert decision.action == "RETRAIN"
    assert decision.reason == "no_stored_fingerprint"


def test_checkpoint_retrain_none_path() -> None:
    fp = _make_fingerprint()
    decision = check_checkpoint(fp, None)
    assert decision.action == "RETRAIN"


def test_save_and_load_fingerprint(tmp_path: Path) -> None:
    fp = _make_fingerprint()
    path = tmp_path / "fp.json"
    save_fingerprint(fp, path)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["fingerprint_hash"] == fp.fingerprint_hash
    assert data["symbol"] == "AAPL"
    assert data["timeframe"] == "1D"


def test_checkpoint_retrain_corrupt_stored(tmp_path: Path) -> None:
    fp_path = tmp_path / "bad.json"
    fp_path.write_text("NOT VALID JSON", encoding="utf-8")
    fp = _make_fingerprint()
    decision = check_checkpoint(fp, fp_path)
    assert decision.action == "RETRAIN"
    assert decision.reason == "stored_fingerprint_unreadable"
