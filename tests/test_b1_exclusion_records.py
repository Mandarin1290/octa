"""B1 — Exclusion-Record tests.

(a) FEATURE_MATRIX_EMPTY path in pipeline.py → cascade_train.py writes Exclusion-Record
    with reason_code containing "FEATURE_MATRIX_EMPTY".
(b) TRAINING_EXCEPTION path → cascade_train.py exception handler writes Record,
    exception does not propagate out of run_cascade_training.
"""
from __future__ import annotations

import json
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_parquet(tmp_path: Path, n: int = 500) -> str:
    """Write a minimal parquet the walkforward eligibility check accepts."""
    idx = pd.date_range("2015-01-01", periods=n, freq="B", tz="UTC")
    df = pd.DataFrame(
        {
            "open": np.random.rand(n) + 10,
            "high": np.random.rand(n) + 11,
            "low": np.random.rand(n) + 9,
            "close": np.random.rand(n) + 10,
            "volume": np.ones(n) * 1000,
        },
        index=idx,
    )
    p = tmp_path / "ADC_1D.parquet"
    df.to_parquet(p)
    return str(p)


def _make_cfg():
    """Load the real sweep config — sufficient for cascade_train._walkforward_resolver."""
    from octa_training.core.config import load_config
    repo = Path(__file__).resolve().parents[1]
    return load_config(str(repo / "configs" / "sweep_catboost_1d.yaml"))


# ── test (a): FEATURE_MATRIX_EMPTY → Exclusion-Record ────────────────────────

def test_feature_matrix_empty_writes_exclusion_record(tmp_path):
    """When pipeline returns FEATURE_MATRIX_EMPTY, cascade_train writes an exclusion record."""
    from octa_ops.autopilot.cascade_train import run_cascade_training, CascadePolicy
    from octa_ops.autopilot.autopilot_types import normalize_timeframe

    pq_path = _make_parquet(tmp_path, n=600)

    # Make train_evaluate_adaptive return passed=False, error='FEATURE_MATRIX_EMPTY'
    fake_result = types.SimpleNamespace(
        passed=False,
        error="FEATURE_MATRIX_EMPTY",
        metrics=None,
        gate_result=None,
        pack_result=None,
    )

    reports_dir = str(tmp_path / "reports")
    excl_dir = tmp_path / "octa" / "var" / "evidence" / "exclusions"

    # v0.0.0: when regime_ensemble.enabled=True, cascade_train dispatches to
    # train_regime_ensemble instead of train_evaluate_adaptive.  Patch both so
    # the test works regardless of which dispatch path is active.
    from octa_training.core.pipeline import RegimeEnsemble as _RE
    fake_ensemble = _RE(
        symbol="ADC", timeframe="1D", run_id="test_run_empty",
        submodels={"neutral": fake_result},
        regimes_trained=0, passed=False, error="FEATURE_MATRIX_EMPTY",
    )

    with (
        patch("octa_ops.autopilot.cascade_train.train_evaluate_adaptive", return_value=fake_result),
        patch("octa_ops.autopilot.cascade_train.train_regime_ensemble", return_value=fake_ensemble),
        patch("octa_ops.autopilot.cascade_train._write_exclusion_record") as mock_excl,
        # Make _write_exclusion_record also do the real write for the assertion
    ):
        # Re-patch to both call through and capture
        mock_excl.side_effect = None  # let it pass

        decisions, metrics = run_cascade_training(
            run_id="test_run_empty",
            config_path=str(Path(__file__).resolve().parents[1] / "configs" / "sweep_catboost_1d.yaml"),
            symbol="ADC",
            asset_class="stock",
            parquet_paths={"1D": pq_path},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=False,
            reports_dir=reports_dir,
        )

    # _write_exclusion_record must have been called with reason_code derived from error
    assert mock_excl.called, "Exclusion record must be written when metrics_dump is None"
    call_kwargs = mock_excl.call_args.kwargs
    assert call_kwargs["symbol"] == "ADC"
    assert call_kwargs["tf"] == "1D"
    assert "FEATURE_MATRIX_EMPTY" in call_kwargs["reason_code"] or "feature_matrix_empty" in call_kwargs["reason_code"].lower()


def test_feature_matrix_empty_exclusion_record_real_write(tmp_path):
    """Integration variant: the real _write_exclusion_record writes a JSON file."""
    from octa_ops.autopilot.cascade_train import run_cascade_training, CascadePolicy, _write_exclusion_record

    pq_path = _make_parquet(tmp_path, n=600)

    fake_result = types.SimpleNamespace(
        passed=False,
        error="FEATURE_MATRIX_EMPTY",
        metrics=None,
        gate_result=None,
        pack_result=None,
    )

    reports_dir = str(tmp_path / "reports")
    # Redirect evidence root by monkeypatching _write_exclusion_record to use tmp_path
    written: list = []

    def _real_write(**kwargs):
        kwargs["evidence_root"] = str(tmp_path / "octa" / "var" / "evidence")
        path = _write_exclusion_record(**kwargs)
        if path:
            written.append(path)
        return path

    from octa_training.core.pipeline import RegimeEnsemble as _RE2
    fake_ensemble2 = _RE2(
        symbol="ADC", timeframe="1D", run_id="test_run_empty2",
        submodels={"neutral": fake_result},
        regimes_trained=0, passed=False, error="FEATURE_MATRIX_EMPTY",
    )

    with (
        patch("octa_ops.autopilot.cascade_train.train_evaluate_adaptive", return_value=fake_result),
        patch("octa_ops.autopilot.cascade_train.train_regime_ensemble", return_value=fake_ensemble2),
        patch("octa_ops.autopilot.cascade_train._write_exclusion_record", side_effect=_real_write),
    ):
        run_cascade_training(
            run_id="test_run_empty2",
            config_path=str(Path(__file__).resolve().parents[1] / "configs" / "sweep_catboost_1d.yaml"),
            symbol="ADC",
            asset_class="stock",
            parquet_paths={"1D": pq_path},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=False,
            reports_dir=reports_dir,
        )

    assert written, "No exclusion file was written"
    rec = json.loads(Path(written[0]).read_text())
    assert rec["symbol"] == "ADC"
    assert rec["tf"] == "1D"
    assert "FEATURE_MATRIX_EMPTY" in rec["reason_code"] or "feature_matrix_empty" in rec["reason_code"].lower()
    assert "run_id" in rec
    assert "timestamp" in rec


# ── test (b): TRAINING_EXCEPTION → Record written, exception not propagated ──

def test_training_exception_writes_record_and_does_not_propagate(tmp_path):
    """When train_evaluate_adaptive raises, cascade_train writes an exclusion record
    and returns normally — the exception must not propagate out."""
    from octa_ops.autopilot.cascade_train import run_cascade_training, CascadePolicy, _write_exclusion_record

    pq_path = _make_parquet(tmp_path, n=600)
    reports_dir = str(tmp_path / "reports")

    written: list = []

    def _real_write(**kwargs):
        kwargs["evidence_root"] = str(tmp_path / "octa" / "var" / "evidence")
        path = _write_exclusion_record(**kwargs)
        if path:
            written.append(path)
        return path

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic training crash")

    with (
        patch("octa_ops.autopilot.cascade_train.train_evaluate_adaptive", side_effect=_boom),
        # v0.0.0: also patch train_regime_ensemble to raise so the cascade
        # exception handler fires and records TRAIN_ERROR (not GATE_FAIL).
        patch("octa_ops.autopilot.cascade_train.train_regime_ensemble", side_effect=_boom),
        patch("octa_ops.autopilot.cascade_train._write_exclusion_record", side_effect=_real_write),
    ):
        # Must NOT raise
        decisions, metrics = run_cascade_training(
            run_id="test_run_exc",
            config_path=str(Path(__file__).resolve().parents[1] / "configs" / "sweep_catboost_1d.yaml"),
            symbol="AUR",
            asset_class="stock",
            parquet_paths={"1D": pq_path},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=False,
            reports_dir=reports_dir,
        )

    # Exception must not propagate — we got here
    assert decisions, "run_cascade_training must return decisions even on exception"
    assert decisions[0].status == "TRAIN_ERROR"
    assert decisions[0].reason == "train_exception"

    assert written, "Exclusion record must be written on TRAINING_EXCEPTION"
    rec = json.loads(Path(written[0]).read_text())
    assert rec["symbol"] == "AUR"
    assert rec["reason_code"] == "TRAINING_EXCEPTION"
    assert "synthetic training crash" in rec["detail"]

    # metrics_by_tf must have a structured entry (not missing)
    assert "1D" in metrics
    assert metrics["1D"]["reason_code"] == "TRAINING_EXCEPTION"
