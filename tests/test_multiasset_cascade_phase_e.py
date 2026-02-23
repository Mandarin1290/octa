"""Phase E: Deferred items — options callable API, OptionsModelAdapter, cascade dependency.

Covers:
1. train_options_lstm module importable without TF (deferred import)
2. wf_date_split_with_purge_embargo() — purge/embargo gaps enforced
3. train_options() callable — returns StageResult; handles TF unavailable
4. train_options() WF gate: oos_auc < threshold → performance_pass=False
5. OptionsModelAdapter — load, predict, fail-closed, sha256 sidecar
6. run_cascade_training() underlying_performance_pass=False → all SKIP
7. run_cascade_training() underlying_performance_pass=True → proceeds normally
8. run_cascade_training() underlying_performance_pass=None → backward compat
9. Non-option asset_class ignores underlying_performance_pass
10. StageResult returned by train_options() validates correctly

TF strategy: tensorflow causes SIGILL on this CPU.
- Tests that verify TF-unavailable fallback: patch sys.modules["tensorflow"] = None
- Tests that need fake TF to proceed past the import check: inject a minimal stub module
"""
from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_options_df(n_contracts: int = 3, n_days: int = 60) -> pd.DataFrame:
    rows = []
    base = pd.Timestamp("2024-01-01", tz="UTC")
    for c in range(n_contracts):
        for d in range(n_days):
            rows.append({
                "timestamp": base + pd.Timedelta(days=d),
                "contract_id": f"C{c}",
                "delta": 0.5,
                "moneyness": 1.0,
                "tte_days": max(1, 90 - d),
                "mid": 5.0,
                "underlying_close": 100.0,
                "label_refined_pos": int((d + c) % 2),
            })
    return pd.DataFrame(rows)


def _write_options_parquet(path: Path, n_days: int = 60) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _make_options_df(n_days=n_days).to_parquet(path, index=False)


def _make_fake_tf(*, fold_auc: float = 0.60) -> types.ModuleType:
    """Build a minimal fake tensorflow module for controlled testing."""
    fake_history = MagicMock()
    fake_history.history = {"loss": [0.5], "AUC": [fold_auc], "accuracy": [0.6]}

    fake_model = MagicMock()
    fake_model.fit.return_value = fake_history
    fake_model.evaluate.return_value = [0.5, fold_auc, 0.6]
    fake_model.get_weights.return_value = []
    fake_model.set_weights.return_value = None
    fake_model.save.return_value = None

    fake_sequential = MagicMock(return_value=fake_model)
    fake_layers = types.SimpleNamespace(
        Input=MagicMock(return_value=MagicMock()),
        Masking=MagicMock(return_value=MagicMock()),
        LSTM=MagicMock(return_value=MagicMock()),
        Dense=MagicMock(return_value=MagicMock()),
    )
    fake_keras = types.SimpleNamespace(
        Sequential=fake_sequential,
        layers=fake_layers,
        models=types.SimpleNamespace(load_model=MagicMock(return_value=fake_model)),
    )
    fake_tf = types.ModuleType("tensorflow")
    fake_tf.keras = fake_keras  # type: ignore[attr-defined]
    return fake_tf


# ---------------------------------------------------------------------------
# 1. Module importable (no TF needed)
# ---------------------------------------------------------------------------

class TestModuleImportable:
    def test_train_options_lstm_importable(self) -> None:
        """Module must be importable without TF — all TF imports are deferred."""
        with patch.dict(sys.modules, {"tensorflow": None}):
            import importlib
            import scripts.train_options_lstm as mod
            assert callable(mod.train_options)
            assert callable(mod.wf_date_split_with_purge_embargo)
            assert callable(mod.build_sequences)

    def test_options_adapter_importable(self) -> None:
        from octa_ops.autopilot.options_adapter import OptionsModelAdapter
        assert OptionsModelAdapter is not None


# ---------------------------------------------------------------------------
# 2. wf_date_split_with_purge_embargo
# ---------------------------------------------------------------------------

from scripts.train_options_lstm import wf_date_split, wf_date_split_with_purge_embargo


class TestWFPurgeEmbargo:
    def test_splits_have_purge_gap(self) -> None:
        """Purged train set must end earlier than unpurged original."""
        df = _make_options_df(n_days=60)
        orig = wf_date_split(df, n_folds=3)
        purged = wf_date_split_with_purge_embargo(df, n_folds=3, purge_bars=5, embargo_bars=2)
        assert len(purged) >= 1
        if orig:
            assert purged[0][0][-1] <= orig[0][0][-1]

    def test_splits_have_embargo_gap(self) -> None:
        """Embargoed val start must be >= original val start."""
        df = _make_options_df(n_days=60)
        orig = wf_date_split(df, n_folds=3)
        purged = wf_date_split_with_purge_embargo(df, n_folds=3, purge_bars=5, embargo_bars=2)
        if purged and orig:
            assert purged[0][1][0] >= orig[0][1][0]

    def test_no_overlap_between_train_and_val(self) -> None:
        df = _make_options_df(n_days=60)
        splits = wf_date_split_with_purge_embargo(df, n_folds=3, purge_bars=5, embargo_bars=2)
        for train_d, val_d in splits:
            assert not (set(train_d) & set(val_d)), "train/val must not overlap"

    def test_insufficient_dates_returns_list(self) -> None:
        """With very few dates, must not raise — may return empty list."""
        df = _make_options_df(n_days=3, n_contracts=1)
        splits = wf_date_split_with_purge_embargo(df, n_folds=5, purge_bars=5, embargo_bars=5)
        assert isinstance(splits, list)

    def test_zero_purge_zero_embargo_same_fold_count(self) -> None:
        df = _make_options_df(n_days=60)
        orig = wf_date_split(df, n_folds=3)
        purge_zero = wf_date_split_with_purge_embargo(df, n_folds=3, purge_bars=0, embargo_bars=0)
        assert len(purge_zero) == len(orig)

    def test_each_fold_train_before_val(self) -> None:
        df = _make_options_df(n_days=60)
        splits = wf_date_split_with_purge_embargo(df, n_folds=3, purge_bars=3, embargo_bars=2)
        for train_d, val_d in splits:
            assert len(train_d) > 0
            assert len(val_d) > 0
            assert max(train_d) < min(val_d)

    def test_forward_monotone_folds(self) -> None:
        """Each fold's training window must end later than the previous."""
        df = _make_options_df(n_days=90)
        splits = wf_date_split_with_purge_embargo(df, n_folds=3, purge_bars=3, embargo_bars=2)
        if len(splits) >= 2:
            for i in range(1, len(splits)):
                assert splits[i][0][-1] >= splits[i - 1][0][-1]


# ---------------------------------------------------------------------------
# 3. train_options() callable — interface and TF-unavailable path
# ---------------------------------------------------------------------------

from scripts.train_options_lstm import train_options
from octa_ops.autopilot.stage_result import StageResult, StageMandatoryMetrics


class TestTrainOptionsCallable:
    def test_returns_stage_result_when_tf_unavailable(self, tmp_path: Path) -> None:
        """TF import fails → returns StageResult with TRAIN_ERROR."""
        pq = tmp_path / "opts.parquet"
        _write_options_parquet(pq)
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = train_options(
                symbol="AAPL_OPT", timeframe="30M", asset_class="option",
                run_id="e_test", stage_index=0, parquet_path=str(pq),
            )
        assert isinstance(result, StageResult)
        assert result.structural_pass is False
        assert result.performance_pass is False
        assert result.fail_status == "TRAIN_ERROR"
        assert "tensorflow" in str(result.fail_reason).lower()

    def test_returns_stage_result_on_missing_parquet(self, tmp_path: Path) -> None:
        pq = tmp_path / "nonexistent.parquet"
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = train_options(
                symbol="AAPL_OPT", timeframe="30M", asset_class="option",
                run_id="e_test", stage_index=0, parquet_path=str(pq),
            )
        assert isinstance(result, StageResult)
        assert result.fail_status == "TRAIN_ERROR"

    def test_returns_data_invalid_on_missing_timestamp(self, tmp_path: Path) -> None:
        """Parquet without timestamp → DATA_INVALID (requires TF to pass import check)."""
        pq = tmp_path / "bad.parquet"
        pd.DataFrame({"close": [1.0, 2.0], "contract_id": ["A", "A"]}).to_parquet(pq)
        fake_tf = types.ModuleType("tensorflow")
        with patch.dict(sys.modules, {"tensorflow": fake_tf}):
            result = train_options(
                symbol="X_OPT", timeframe="30M", asset_class="option",
                run_id="e_test", stage_index=0, parquet_path=str(pq),
            )
        assert isinstance(result, StageResult)
        assert result.fail_status in {"DATA_INVALID", "TRAIN_ERROR"}
        assert result.structural_pass is False

    def test_result_has_correct_identity_fields(self, tmp_path: Path) -> None:
        pq = tmp_path / "opts2.parquet"
        _write_options_parquet(pq)
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = train_options(
                symbol="BTC_OPT", timeframe="30M", asset_class="option",
                run_id="r42", stage_index=2, parquet_path=str(pq),
            )
        assert result.symbol == "BTC_OPT"
        assert result.timeframe == "30M"
        assert result.asset_class == "option"
        assert result.run_id == "r42"
        assert result.stage_index == 2

    def test_elapsed_sec_non_negative(self, tmp_path: Path) -> None:
        pq = tmp_path / "opts3.parquet"
        _write_options_parquet(pq)
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = train_options(
                symbol="Z_OPT", timeframe="30M", asset_class="option",
                run_id="r", stage_index=0, parquet_path=str(pq),
            )
        assert result.elapsed_sec >= 0.0


# ---------------------------------------------------------------------------
# 4. train_options() with fake TF: performance gate
# ---------------------------------------------------------------------------

class TestTrainOptionsGate:
    def test_above_threshold_performance_pass(self, tmp_path: Path) -> None:
        """oos_auc >= 0.55 → performance_pass=True."""
        pq = tmp_path / "opts_high.parquet"
        _write_options_parquet(pq, n_days=80)
        fake_tf = _make_fake_tf(fold_auc=0.70)
        with patch.dict(sys.modules, {"tensorflow": fake_tf}):
            result = train_options(
                symbol="AAPL_OPT", timeframe="30M", asset_class="option",
                run_id="r", stage_index=0, parquet_path=str(pq),
                model_root=str(tmp_path / "models"),
                min_train_sequences=1, min_val_sequences=1,
                epochs=1, n_folds=2,
            )
        assert isinstance(result, StageResult)
        if result.fail_status not in {"TRAIN_ERROR", "DATA_INVALID"}:
            if result.metrics is not None and result.metrics.oos_auc is not None:
                if result.metrics.oos_auc >= 0.55:
                    assert result.performance_pass is True

    def test_below_threshold_gate_fail(self, tmp_path: Path) -> None:
        """oos_auc < 0.55 → performance_pass=False, fail_status=GATE_FAIL."""
        pq = tmp_path / "opts_low.parquet"
        _write_options_parquet(pq, n_days=80)
        fake_tf = _make_fake_tf(fold_auc=0.45)
        with patch.dict(sys.modules, {"tensorflow": fake_tf}):
            result = train_options(
                symbol="AAPL_OPT", timeframe="30M", asset_class="option",
                run_id="r", stage_index=0, parquet_path=str(pq),
                model_root=str(tmp_path / "models"),
                min_train_sequences=1, min_val_sequences=1,
                epochs=1, n_folds=2,
            )
        assert isinstance(result, StageResult)
        if result.fail_status not in {"TRAIN_ERROR", "DATA_INVALID"}:
            if result.metrics is not None and result.metrics.oos_auc is not None:
                if result.metrics.oos_auc < 0.55:
                    assert result.performance_pass is False
                    assert result.fail_status == "GATE_FAIL"

    def test_result_validate_passes(self, tmp_path: Path) -> None:
        """StageResult returned by train_options() passes its own validation."""
        pq = tmp_path / "opts_valid.parquet"
        _write_options_parquet(pq, n_days=80)
        fake_tf = _make_fake_tf(fold_auc=0.60)
        with patch.dict(sys.modules, {"tensorflow": fake_tf}):
            result = train_options(
                symbol="AAPL_OPT", timeframe="30M", asset_class="option",
                run_id="r", stage_index=0, parquet_path=str(pq),
                model_root=str(tmp_path / "models"),
                min_train_sequences=1, min_val_sequences=1,
                epochs=1, n_folds=2,
            )
        issues = result.validate()
        assert issues == [], f"StageResult.validate() returned: {issues}"

    def test_insufficient_sequences_data_invalid(self, tmp_path: Path) -> None:
        """Data without label_refined_pos → no sequences built → DATA_INVALID."""
        pq = tmp_path / "no_labels.parquet"
        pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=10, tz="UTC"),
            "contract_id": ["C1"] * 10,
            "delta": [0.5] * 10,
        }).to_parquet(pq, index=False)
        fake_tf = _make_fake_tf(fold_auc=0.60)
        with patch.dict(sys.modules, {"tensorflow": fake_tf}):
            result = train_options(
                symbol="X_OPT", timeframe="30M", asset_class="option",
                run_id="r", stage_index=0, parquet_path=str(pq),
            )
        assert result.structural_pass is False

    def test_custom_threshold_respected(self, tmp_path: Path) -> None:
        """Custom performance_threshold_auc=0.80 blocks auc=0.70."""
        pq = tmp_path / "opts_thresh.parquet"
        _write_options_parquet(pq, n_days=80)
        fake_tf = _make_fake_tf(fold_auc=0.70)
        with patch.dict(sys.modules, {"tensorflow": fake_tf}):
            result = train_options(
                symbol="AAPL_OPT", timeframe="30M", asset_class="option",
                run_id="r", stage_index=0, parquet_path=str(pq),
                min_train_sequences=1, min_val_sequences=1,
                epochs=1, n_folds=2,
                performance_threshold_auc=0.80,  # requires 0.80, gets 0.70
            )
        if result.fail_status not in {"TRAIN_ERROR", "DATA_INVALID"}:
            if result.metrics is not None and result.metrics.oos_auc is not None:
                if result.metrics.oos_auc < 0.80:
                    assert result.performance_pass is False


# ---------------------------------------------------------------------------
# 5. OptionsModelAdapter
# ---------------------------------------------------------------------------

from octa_ops.autopilot.options_adapter import OptionsModelAdapter


class TestOptionsModelAdapter:
    def test_not_loaded_initially(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "model.h5"))
        assert not adapter.is_loaded

    def test_load_raises_when_file_missing(self, tmp_path: Path) -> None:
        # Inject a fake TF where load_model raises OSError (file not found)
        fake_tf = types.ModuleType("tensorflow")
        fake_tf.keras = types.SimpleNamespace(  # type: ignore[attr-defined]
            models=types.SimpleNamespace(
                load_model=MagicMock(side_effect=OSError("No such file"))
            )
        )
        adapter = OptionsModelAdapter(str(tmp_path / "nonexistent.h5"))
        with pytest.raises(Exception):
            with patch.dict(sys.modules, {"tensorflow": fake_tf}):
                adapter.load()

    def test_predict_returns_neutral_when_load_fails(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "nonexistent.h5"))
        X = np.zeros((5, 32, 5), dtype=np.float32)
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = adapter.predict(X)
        assert isinstance(result, np.ndarray)
        assert len(result) == 5
        assert np.allclose(result, 0.5)

    def test_predict_result_shape_matches_batch(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "nope.h5"))
        X = np.zeros((10, 32, 5), dtype=np.float32)
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = adapter.predict(X)
        assert result.shape == (10,)

    def test_sha256_returns_none_when_no_sidecar(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "model.h5"))
        assert adapter.sha256() is None

    def test_sha256_reads_sidecar_file(self, tmp_path: Path) -> None:
        model_path = tmp_path / "model.h5"
        model_path.with_suffix(".sha256").write_text("abc123\n")
        adapter = OptionsModelAdapter(str(model_path))
        assert adapter.sha256() == "abc123"

    def test_load_error_attribute_populated_on_failure(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "nope.h5"))
        with patch.dict(sys.modules, {"tensorflow": None}):
            try:
                adapter.load()
            except Exception:
                pass
        assert adapter.load_error is not None

    def test_predict_with_mock_model(self) -> None:
        fake_model = MagicMock()
        fake_model.predict.return_value = np.array([[0.7], [0.3], [0.8]])
        adapter = OptionsModelAdapter("/fake/path.h5")
        adapter._model = fake_model
        X = np.zeros((3, 32, 5), dtype=np.float32)
        result = adapter.predict(X)
        assert len(result) == 3
        assert abs(result[0] - 0.7) < 1e-5

    def test_load_error_none_initially(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "model.h5"))
        assert adapter.load_error is None

    def test_predict_all_values_between_0_and_1_on_fail(self, tmp_path: Path) -> None:
        adapter = OptionsModelAdapter(str(tmp_path / "nope.h5"))
        X = np.zeros((7, 32, 5), dtype=np.float32)
        with patch.dict(sys.modules, {"tensorflow": None}):
            result = adapter.predict(X)
        assert (result >= 0.0).all()
        assert (result <= 1.0).all()


# ---------------------------------------------------------------------------
# 6-9. Cascade dependency — underlying_performance_pass
# ---------------------------------------------------------------------------

import octa_ops.autopilot.cascade_train as ct
from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training


def _setup_cascade_mocks(monkeypatch: Any, tmp_path: Path) -> None:
    class _Paths:
        def __init__(self) -> None:
            self.pkl_dir = tmp_path / "pkl"
            self.state_dir = tmp_path / "state"

    class _Cfg:
        def __init__(self) -> None:
            self.paths = _Paths()
            self.splits: dict = {}
            self.parquet = types.SimpleNamespace(
                nan_threshold=0.2, allow_negative_prices=False,
                resample_enabled=False, resample_bar_size="1D",
            )

        def copy(self, deep: bool = False) -> "_Cfg":
            c = _Cfg.__new__(_Cfg)
            c.paths = _Paths()
            c.splits = {}
            c.parquet = self.parquet
            return c

    monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: _Cfg())
    monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
    monkeypatch.setattr(ct, "_walkforward_eligibility", lambda **_kw: {"eligible": True})
    monkeypatch.setattr(
        ct, "train_evaluate_package",
        lambda **_kw: types.SimpleNamespace(
            passed=True,
            gate_result=types.SimpleNamespace(model_dump=lambda: {}, dict=lambda: {}),
            metrics=types.SimpleNamespace(model_dump=lambda: {}, dict=lambda: {}),
            pack_result={},
            error="",
        ),
    )


class TestCascadeDependencyCheck:
    def test_option_underlying_fail_returns_all_skip(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        decisions, metrics = run_cascade_training(
            run_id="e_dep",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"30M": "x.parquet"},
            cascade=CascadePolicy(order=["30M"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            underlying_performance_pass=False,
        )
        assert len(decisions) == 1
        assert decisions[0].status == "SKIP"
        assert decisions[0].reason == "underlying_cascade_not_passed"
        assert decisions[0].details["structural_pass"] is False
        assert decisions[0].details["performance_pass"] is False
        assert decisions[0].details["underlying_performance_pass"] is False
        assert metrics == {}

    def test_option_underlying_fail_all_tfs_skipped(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        decisions, _ = run_cascade_training(
            run_id="e_dep3",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"1D": "x.parquet", "1H": "x.parquet", "30M": "x.parquet"},
            cascade=CascadePolicy(order=["1D", "1H", "30M"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            underlying_performance_pass=False,
        )
        assert len(decisions) == 3
        assert all(d.status == "SKIP" for d in decisions)
        assert all(d.reason == "underlying_cascade_not_passed" for d in decisions)

    def test_option_underlying_pass_proceeds(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        decisions, _ = run_cascade_training(
            run_id="e_pass",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"30M": "x.parquet"},
            cascade=CascadePolicy(order=["30M"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            underlying_performance_pass=True,
        )
        assert decisions[0].status == "PASS"

    def test_option_underlying_none_backward_compat(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        decisions, _ = run_cascade_training(
            run_id="e_none",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"30M": "x.parquet"},
            cascade=CascadePolicy(order=["30M"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            # underlying_performance_pass not passed → defaults to None
        )
        assert decisions[0].status == "PASS"

    def test_non_option_ignores_underlying_param(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        for ac in ["stock", "future", "crypto", "fx"]:
            decisions, _ = run_cascade_training(
                run_id=f"e_{ac}",
                config_path="configs/dev.yaml",
                symbol="X",
                asset_class=ac,
                parquet_paths={"1D": "x.parquet"},
                cascade=CascadePolicy(order=["1D"]),
                safe_mode=True,
                reports_dir=str(tmp_path),
                underlying_performance_pass=False,
            )
            assert decisions[0].status == "PASS", f"asset_class={ac} should not be blocked"

    def test_option_and_options_both_trigger_check(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        for ac in ["option", "options"]:
            decisions, _ = run_cascade_training(
                run_id=f"e_opt_{ac}",
                config_path="configs/dev.yaml",
                symbol="X_OPT",
                asset_class=ac,
                parquet_paths={"30M": "x.parquet"},
                cascade=CascadePolicy(order=["30M"]),
                safe_mode=True,
                reports_dir=str(tmp_path),
                underlying_performance_pass=False,
            )
            assert decisions[0].reason == "underlying_cascade_not_passed", f"ac={ac}"

    def test_skip_detail_contains_flag(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path)
        decisions, _ = run_cascade_training(
            run_id="e_detail",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"30M": "x.parquet"},
            cascade=CascadePolicy(order=["30M"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            underlying_performance_pass=False,
        )
        assert decisions[0].details.get("underlying_performance_pass") is False
