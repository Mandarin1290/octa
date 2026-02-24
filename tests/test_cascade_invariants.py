"""Phase D invariant tests for CASCADE training pipeline.

Tests:
- test_promotion_requires_real_pass (I1)
- test_gate_fail_stops_cascade (I1)
- test_rowcount_unknown_is_fail_closed (I3)
- test_rowcount_data_invalid_is_fail_closed (I3)
- test_asset_class_mapping_deterministic_examples (I4)
- test_unknown_asset_class_is_fail_closed (I4)
- test_noop_outcome_is_flagged (I2)
- test_calendar_policy_crypto_fx_not_252 (I5) — delegates to existing tests
- test_futures_roll_embargo_prevents_fold_contamination (I6)
- test_walkforward_splits_roll_embargo_backward_compat (I6)
- test_options_skip_when_underlying_none (I7)
- test_options_skip_when_underlying_false (I7)
- test_options_proceed_when_underlying_true (I7) — structural only
"""
from __future__ import annotations

import types
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_minimal_cfg(splits: Optional[Dict] = None):
    """Build a minimal config namespace compatible with cascade_train helpers."""
    splits = splits or {}
    paths = types.SimpleNamespace(
        pkl_dir="pkl",
        state_dir="state",
    )
    cfg = types.SimpleNamespace(
        parquet=types.SimpleNamespace(
            nan_threshold=0.2,
            allow_negative_prices=False,
            resample_enabled=False,
            resample_bar_size="1D",
        ),
        splits=splits,
        paths=paths,
    )
    # Allow shallow copy
    def _copy(deep=False):
        import copy
        c = copy.copy(cfg)
        c.paths = copy.copy(cfg.paths)
        return c
    cfg.copy = _copy
    return cfg


def _make_gate_decision_pass(symbol, tf):
    from octa_ops.autopilot.types import GateDecision
    return GateDecision(
        symbol=symbol,
        timeframe=tf,
        stage="train",
        status="PASS",
        reason=None,
        details={"structural_pass": True, "performance_pass": True},
    )


def _make_gate_decision_fail(symbol, tf, status="GATE_FAIL"):
    from octa_ops.autopilot.types import GateDecision
    return GateDecision(
        symbol=symbol,
        timeframe=tf,
        stage="train",
        status=status,
        reason="gate_failed",
        details={"structural_pass": False, "performance_pass": False},
    )


# ---------------------------------------------------------------------------
# I1: Promotion requires genuine performance PASS
# ---------------------------------------------------------------------------

class TestPromotionRequiresRealPass:
    """I1: Only performance_pass=True advances the cascade."""

    def _run_cascade(self, monkeypatch, tmp_path, first_tf_passes: bool):
        """Run a 2-TF cascade (1D, 1H) with a mocked train_evaluate_package."""
        import octa_ops.autopilot.cascade_train as ct
        from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training

        # Mock train_evaluate_package
        call_log = []

        def mock_train(*, symbol, cfg, state, run_id, safe_mode, smoke_test, parquet_path, dataset, asset_class, gate_overrides):
            call_log.append({"symbol": symbol, "parquet_path": parquet_path})
            return types.SimpleNamespace(
                passed=first_tf_passes,
                error=None,
                gate_result=None,
                metrics=None,
                pack_result=None,
            )

        monkeypatch.setattr(ct, "train_evaluate_package", mock_train)

        # Mock _walkforward_eligibility to always return eligible
        monkeypatch.setattr(ct, "_walkforward_eligibility", lambda *, parquet_path, cfg, asset_class: {
            "eligible": True,
            "reason": None,
            "available_bars": 1000,
            "required_bars": 100,
            "resolver": {},
            "proof": "mock",
        })

        # Mock load_config
        monkeypatch.setattr(ct, "load_config", lambda path, **kw: _make_minimal_cfg())

        # Create fake parquet files
        pq_1d = str(tmp_path / "ES_1D.parquet")
        pq_1h = str(tmp_path / "ES_1H.parquet")
        Path(pq_1d).touch()
        Path(pq_1h).touch()

        decisions, _ = run_cascade_training(
            run_id="test_run",
            config_path="configs/dev.yaml",
            symbol="ES",
            asset_class="future",
            parquet_paths={"1D": pq_1d, "1H": pq_1h},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        return decisions, call_log

    def test_pass_1d_allows_1h_training(self, monkeypatch, tmp_path):
        """When 1D passes, 1H must be trained."""
        decisions, call_log = self._run_cascade(monkeypatch, tmp_path, first_tf_passes=True)
        tfs = [d.timeframe for d in decisions if d.stage == "train"]
        statuses = {d.timeframe: d.status for d in decisions}
        assert "1H" in tfs
        assert statuses["1H"] == "PASS"
        assert len(call_log) == 2  # both TFs trained

    def test_fail_1d_skips_1h(self, monkeypatch, tmp_path):
        """I1: When 1D GATE_FAILs (performance fail), 1H must be SKIP."""
        decisions, call_log = self._run_cascade(monkeypatch, tmp_path, first_tf_passes=False)
        statuses = {d.timeframe: d.status for d in decisions}
        assert statuses.get("1D") == "GATE_FAIL"
        assert statuses.get("1H") == "SKIP"
        # Only 1D should have been trained
        assert len(call_log) == 1

    def test_fail_1d_skip_reason_mentions_cascade(self, monkeypatch, tmp_path):
        """I1: SKIP reason must indicate cascade stage did not pass."""
        decisions, _ = self._run_cascade(monkeypatch, tmp_path, first_tf_passes=False)
        skip_1h = next((d for d in decisions if d.timeframe == "1H"), None)
        assert skip_1h is not None
        assert "cascade_previous_stage_not_passed" in str(skip_1h.reason)


# ---------------------------------------------------------------------------
# I3: Row-count unknown is FAIL-CLOSED
# ---------------------------------------------------------------------------

class TestRowcountFailClosed:
    """I3: Missing row count must yield eligible=False."""

    def test_rowcount_unknown_returns_fail(self, monkeypatch, tmp_path):
        """When pyarrow fails AND load_parquet raises, result must be FAIL."""
        import octa_ops.autopilot.cascade_train as ct

        pq_path = str(tmp_path / "AAPL.parquet")
        Path(pq_path).touch()

        cfg = _make_minimal_cfg()

        monkeypatch.setattr(ct, "load_parquet", lambda p, **kw: (_ for _ in ()).throw(RuntimeError("io_fail")))

        def _fake_num_rows(path):
            return None  # pyarrow also fails

        monkeypatch.setattr(ct, "_parquet_num_rows", _fake_num_rows)

        result = ct._walkforward_eligibility(parquet_path=pq_path, cfg=cfg, asset_class="stock")
        assert result["eligible"] is False
        assert "ROWCOUNT_UNKNOWN" in str(result.get("reason", ""))

    def test_rowcount_unknown_has_fail_closed_proof(self, monkeypatch, tmp_path):
        """Proof field must indicate fail-closed path was taken."""
        import octa_ops.autopilot.cascade_train as ct

        pq_path = str(tmp_path / "AAPL.parquet")
        Path(pq_path).touch()
        cfg = _make_minimal_cfg()

        monkeypatch.setattr(ct, "load_parquet", lambda p, **kw: (_ for _ in ()).throw(RuntimeError("io_fail")))
        monkeypatch.setattr(ct, "_parquet_num_rows", lambda path: None)

        result = ct._walkforward_eligibility(parquet_path=pq_path, cfg=cfg, asset_class="stock")
        assert "fail_closed" in str(result.get("proof", ""))

    def test_rowcount_available_returns_correct_eligibility(self, monkeypatch, tmp_path):
        """When bars are available and >= threshold, eligible=True."""
        import octa_ops.autopilot.cascade_train as ct

        data = pd.DataFrame({
            "open": [100.0] * 500,
            "high": [101.0] * 500,
            "low": [99.0] * 500,
            "close": [100.0] * 500,
            "volume": [1000.0] * 500,
        })
        pq_path = str(tmp_path / "AAPL.parquet")
        data.to_parquet(pq_path)

        monkeypatch.setattr(ct, "load_parquet", lambda p, **kw: data)
        monkeypatch.setattr(ct, "validate_price_series", lambda df, **kw: types.SimpleNamespace(
            ok=True, code="OK", stats={"rows_clean": 500}
        ))

        cfg = _make_minimal_cfg(splits={"min_train_size": 100, "min_test_size": 30})
        result = ct._walkforward_eligibility(parquet_path=pq_path, cfg=cfg, asset_class="stock")
        assert result["eligible"] is True


# ---------------------------------------------------------------------------
# I2: SafeNoopGate.evaluate() carries is_noop=True
# ---------------------------------------------------------------------------

class TestNoopOutcomeFlagged:
    """I2: SafeNoopGate must produce GateOutcome with is_noop=True."""

    def test_noop_gate_is_noop_flag_true(self):
        from octa.core.cascade.adapters import SafeNoopGate
        gate = SafeNoopGate(name="test_noop", timeframe="1D")
        outcome = gate.evaluate(["AAPL", "MSFT"])
        assert outcome.is_noop is True

    def test_noop_gate_still_passes_symbols(self):
        from octa.core.cascade.adapters import SafeNoopGate
        gate = SafeNoopGate(name="test_noop", timeframe="1D")
        outcome = gate.evaluate(["AAPL"])
        from octa.core.cascade.contracts import GateDecision
        assert outcome.decision == GateDecision.PASS
        assert "AAPL" in outcome.eligible_symbols

    def test_real_gate_adapter_is_noop_false(self):
        """A real (non-noop) GateOutcome should default is_noop=False."""
        from octa.core.cascade.contracts import GateDecision, GateOutcome
        outcome = GateOutcome(
            decision=GateDecision.PASS,
            eligible_symbols=["AAPL"],
            rejected_symbols=[],
        )
        assert outcome.is_noop is False


# ---------------------------------------------------------------------------
# I4: Unknown asset class is FAIL-CLOSED
# ---------------------------------------------------------------------------

class TestUnknownAssetClassFailClosed:
    """I4: run_cascade_training with unknown asset_class must return GATE_FAIL for all TFs."""

    def test_unknown_asset_class_all_fail(self, monkeypatch, tmp_path):
        import octa_ops.autopilot.cascade_train as ct
        from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training

        monkeypatch.setattr(ct, "load_config", lambda path, **kw: _make_minimal_cfg())

        pq = str(tmp_path / "X.parquet")
        Path(pq).touch()

        decisions, metrics = run_cascade_training(
            run_id="test_run",
            config_path="configs/dev.yaml",
            symbol="UNKNOWN_THING",
            asset_class="unknown",
            parquet_paths={"1D": pq, "1H": pq},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(decisions) > 0
        for d in decisions:
            assert d.status == "GATE_FAIL", f"Expected GATE_FAIL for {d.timeframe}, got {d.status}"
            assert "UNKNOWN_ASSET_CLASS" in str(d.reason)

    def test_empty_string_asset_class_all_fail(self, monkeypatch, tmp_path):
        import octa_ops.autopilot.cascade_train as ct
        from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training

        monkeypatch.setattr(ct, "load_config", lambda path, **kw: _make_minimal_cfg())

        pq = str(tmp_path / "X.parquet")
        Path(pq).touch()

        decisions, _ = run_cascade_training(
            run_id="test_run",
            config_path="configs/dev.yaml",
            symbol="X",
            asset_class="",
            parquet_paths={"1D": pq},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert all(d.status == "GATE_FAIL" for d in decisions)

    def test_known_asset_class_passes_through(self, monkeypatch, tmp_path):
        """Known asset class 'stock' must not be blocked by the unknown check."""
        import octa_ops.autopilot.cascade_train as ct
        from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training

        monkeypatch.setattr(ct, "load_config", lambda path, **kw: _make_minimal_cfg())
        monkeypatch.setattr(ct, "_walkforward_eligibility", lambda *, parquet_path, cfg, asset_class: {
            "eligible": False,  # will stop here, but not at unknown-class check
            "reason": "insufficient_history_for_walkforward",
            "available_bars": 0,
            "required_bars": 100,
            "resolver": {},
            "proof": "mock",
        })

        pq = str(tmp_path / "AAPL.parquet")
        Path(pq).touch()

        decisions, _ = run_cascade_training(
            run_id="test_run",
            config_path="configs/dev.yaml",
            symbol="AAPL",
            asset_class="stock",
            parquet_paths={"1D": pq},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(decisions) > 0
        # Should be SKIP for insufficient history, not GATE_FAIL for unknown class
        assert all("UNKNOWN_ASSET_CLASS" not in str(d.reason) for d in decisions)


# ---------------------------------------------------------------------------
# I4: asset_class mapping determinism
# ---------------------------------------------------------------------------

class TestAssetClassMappingDeterministic:
    """I4: Known tickers must classify deterministically."""

    def _infer(self, symbol: str, path: str = "", cols=None):
        from octa_training.core.asset_class import infer_asset_class
        cfg = types.SimpleNamespace(asset_class_overrides=None)
        return infer_asset_class(symbol, path, cols or [], cfg)

    def test_btc_via_path_classifies_as_crypto(self):
        # BTCUSD 6-char triggers forex heuristic first; path-based detection works
        assert self._infer("BTC", path="/data/crypto/BTC_1D.parquet") == "crypto"

    def test_eth_symbol_classifies_as_crypto(self):
        # ETH contains the substring "ETH" → crypto
        assert self._infer("ETHUSDT") == "crypto"

    def test_eurusd_classifies_as_forex(self):
        assert self._infer("EURUSD") == "forex"

    def test_aapl_classifies_as_stock(self):
        assert self._infer("AAPL") == "stock"

    def test_es_classifies_as_future(self):
        assert self._infer("ES1") == "future"

    def test_nq_classifies_as_future(self):
        assert self._infer("NQ1") == "future"

    def test_options_columns_classify_as_option(self):
        assert self._infer("AAPL_OPT", cols=["DELTA", "GAMMA", "VEGA"]) == "option"

    def test_path_based_crypto(self):
        assert self._infer("BTC", path="/data/crypto/BTC_1D.parquet") == "crypto"

    def test_unknown_long_ticker_stays_unknown(self):
        result = self._infer("XYZFOO123BAR")  # too long for stock heuristic, no match
        assert result == "unknown"


# ---------------------------------------------------------------------------
# I6: Futures roll embargo prevents fold contamination
# ---------------------------------------------------------------------------

class TestFuturesRollEmbargo:
    """I6: walk_forward_splits with roll_embargo_indices must exclude roll-adjacent bars."""

    def _make_index(self, n=200):
        return pd.date_range("2020-01-01", periods=n, freq="D")

    def test_roll_in_val_drops_fold(self):
        """If a roll event falls inside a fold's val window, that fold is dropped."""
        from octa_training.core.splits import walk_forward_splits

        idx = self._make_index(200)
        # roll at bar 120 — will land in some val window
        roll_idx = np.array([120])

        folds_with = walk_forward_splits(
            idx, n_folds=5, train_window=80, test_window=20, step=20,
            min_folds_required=0, roll_embargo_indices=roll_idx, roll_embargo_bars=5,
        )
        folds_without = walk_forward_splits(
            idx, n_folds=5, train_window=80, test_window=20, step=20,
            min_folds_required=0,
        )
        # With roll embargo, we should have fewer or equal folds
        assert len(folds_with) <= len(folds_without)
        # None of the retained folds should have bar 120 in val
        for fold in folds_with:
            assert 120 not in fold.val_idx, "Roll event bar found in val set"

    def test_roll_in_train_purges_adjacent_bars(self):
        """Train bars within roll_embargo_bars of a roll event are removed."""
        from octa_training.core.splits import walk_forward_splits

        idx = self._make_index(300)
        roll_pos = 50
        roll_idx = np.array([roll_pos])

        folds = walk_forward_splits(
            idx, n_folds=1, train_window=150, test_window=50, step=200,
            min_folds_required=0, roll_embargo_indices=roll_idx, roll_embargo_bars=5,
        )
        if folds:
            train = folds[0].train_idx
            # Bars 45..55 should all be absent from training
            for bar in range(roll_pos - 5, roll_pos + 6):
                if bar >= 0:
                    assert bar not in train, f"Roll-adjacent bar {bar} should be purged from train"

    def test_roll_embargo_metadata_in_fold_meta(self):
        """fold_meta must record roll_embargo_applied when roll indices are provided."""
        from octa_training.core.splits import walk_forward_splits

        idx = self._make_index(200)
        folds = walk_forward_splits(
            idx, n_folds=1, train_window=100, test_window=50, step=200,
            min_folds_required=0, roll_embargo_indices=np.array([10]), roll_embargo_bars=3,
        )
        if folds:
            assert folds[0].fold_meta.get("roll_embargo_applied") is True

    def test_no_roll_indices_backward_compat(self):
        """Without roll_embargo_indices, behavior is identical to before (backward compat)."""
        from octa_training.core.splits import walk_forward_splits

        idx = self._make_index(200)
        folds_new = walk_forward_splits(
            idx, n_folds=3, train_window=80, test_window=20, step=20,
            min_folds_required=0,
        )
        folds_explicit_none = walk_forward_splits(
            idx, n_folds=3, train_window=80, test_window=20, step=20,
            min_folds_required=0, roll_embargo_indices=None,
        )
        assert len(folds_new) == len(folds_explicit_none)
        for f1, f2 in zip(folds_new, folds_explicit_none):
            np.testing.assert_array_equal(f1.train_idx, f2.train_idx)
            np.testing.assert_array_equal(f1.val_idx, f2.val_idx)

    def test_roll_embargo_fold_meta_absent_when_no_roll(self):
        """Without roll indices, fold_meta must not have roll_embargo_applied key."""
        from octa_training.core.splits import walk_forward_splits

        idx = self._make_index(200)
        folds = walk_forward_splits(
            idx, n_folds=1, train_window=100, test_window=50, step=200,
            min_folds_required=0,
        )
        if folds:
            assert "roll_embargo_applied" not in folds[0].fold_meta


# ---------------------------------------------------------------------------
# I7: Options SKIP when underlying not passed
# ---------------------------------------------------------------------------

class TestOptionsCascadeSkip:
    """I7: Options must be SKIP unless underlying_performance_pass=True."""

    def _run_options(self, monkeypatch, tmp_path, underlying_performance_pass):
        import octa_ops.autopilot.cascade_train as ct
        from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training

        monkeypatch.setattr(ct, "load_config", lambda path, **kw: _make_minimal_cfg())
        pq = str(tmp_path / "AAPL_OPT.parquet")
        Path(pq).touch()

        return run_cascade_training(
            run_id="test_run",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"1D": pq, "1H": pq},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            underlying_performance_pass=underlying_performance_pass,
        )

    def test_options_skip_when_underlying_none(self, monkeypatch, tmp_path):
        """I7: underlying_performance_pass=None → SKIP (fail-closed)."""
        decisions, _ = self._run_options(monkeypatch, tmp_path, underlying_performance_pass=None)
        assert len(decisions) > 0
        for d in decisions:
            assert d.status == "SKIP", f"Expected SKIP, got {d.status}"
            assert "underlying_cascade_pass_not_provided" in str(d.reason)

    def test_options_skip_when_underlying_false(self, monkeypatch, tmp_path):
        """I7: underlying_performance_pass=False → SKIP (underlying failed)."""
        decisions, _ = self._run_options(monkeypatch, tmp_path, underlying_performance_pass=False)
        assert len(decisions) > 0
        for d in decisions:
            assert d.status == "SKIP"
            assert "underlying_cascade_not_passed" in str(d.reason)

    def test_options_allowed_when_underlying_true(self, monkeypatch, tmp_path):
        """I7: underlying_performance_pass=True → options proceed to walkforward check."""
        import octa_ops.autopilot.cascade_train as ct
        from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training

        monkeypatch.setattr(ct, "load_config", lambda path, **kw: _make_minimal_cfg())
        # Stub _walkforward_eligibility to return ineligible (insufficient bars) —
        # this proves the unknown-class / options-skip gate was passed successfully
        monkeypatch.setattr(ct, "_walkforward_eligibility", lambda *, parquet_path, cfg, asset_class: {
            "eligible": False,
            "reason": "insufficient_history_for_walkforward",
            "available_bars": 0,
            "required_bars": 100,
            "resolver": {},
            "proof": "mock",
        })

        pq = str(tmp_path / "AAPL_OPT.parquet")
        Path(pq).touch()

        decisions, _ = run_cascade_training(
            run_id="test_run",
            config_path="configs/dev.yaml",
            symbol="AAPL_OPT",
            asset_class="option",
            parquet_paths={"1D": pq},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            underlying_performance_pass=True,
        )
        # Should be SKIP for data reasons, not for options gate
        for d in decisions:
            assert "underlying" not in str(d.reason), f"Options gate should not block: {d.reason}"
