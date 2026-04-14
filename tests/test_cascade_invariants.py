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
- TestRegimeArtifactPersistence (I8): per-regime pkls, router manifest, partial-fail invariants
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
    from octa_ops.autopilot.autopilot_types import GateDecision
    return GateDecision(
        symbol=symbol,
        timeframe=tf,
        stage="train",
        status="PASS",
        reason=None,
        details={"structural_pass": True, "performance_pass": True},
    )


def _make_gate_decision_fail(symbol, tf, status="GATE_FAIL"):
    from octa_ops.autopilot.autopilot_types import GateDecision
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

        def mock_train(*, symbol, cfg, state, run_id, safe_mode, smoke_test, parquet_path, dataset, asset_class, gate_overrides, fast=False, robustness_profile="full"):
            call_log.append({"symbol": symbol, "parquet_path": parquet_path})
            return types.SimpleNamespace(
                passed=first_tf_passes,
                error=None,
                gate_result=None,
                metrics=None,
                pack_result=None,
            )

        monkeypatch.setattr(ct, "train_evaluate_adaptive", mock_train)

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


# ---------------------------------------------------------------------------
# I8: crisis_oos gate disabled for non-crisis regime submodels (v0.0.0)
# ---------------------------------------------------------------------------

class TestCrisisOosDisabledForNonCrisisSubmodels:
    """I8: train_regime_ensemble must NOT apply crisis_oos to non-crisis submodels.

    Non-crisis submodels (bull, bear, neutral) are never deployed during crisis
    periods — the crisis submodel handles those intervals.  Applying the crisis
    OOS gate to non-crisis submodels is architecturally incorrect.
    """

    def test_non_crisis_submodels_get_crisis_oos_disabled(self, tmp_path):
        """Verify cfg passed to non-crisis regimes has crisis_oos.enabled=False."""
        import pandas as pd
        import numpy as np
        from unittest.mock import patch, MagicMock
        from octa_training.core.pipeline import train_regime_ensemble, PipelineResult
        from octa_training.core.config import TrainingConfig, CrisisOosConfig, CrisisWindow

        # Build a real TrainingConfig with crisis_oos enabled
        from octa_training.core.config import RegimeEnsembleConfig
        re_cfg = RegimeEnsembleConfig(
            enabled=True,
            regimes=["bull", "bear", "crisis"],
            min_regimes_trained=2,
        )
        crisis_cfg = CrisisOosConfig(
            enabled=True,
            min_sharpe=0.0,
            max_drawdown_pct=0.4,
            windows=[CrisisWindow(name="covid", start="2020-02-01", end="2020-05-31")],
        )
        cfg = TrainingConfig(regime_ensemble=re_cfg, crisis_oos=crisis_cfg)

        # Build a minimal fake parquet
        pq = tmp_path / "TEST_1D.parquet"
        idx = pd.date_range("2015-01-01", periods=600, freq="B", tz="UTC")
        df = pd.DataFrame({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6}, index=idx)
        df.to_parquet(str(pq))

        # Track which cfg each submodel gets
        captured_cfgs: dict = {}

        def _fake_train_adaptive(symbol, cfg, state, run_id, **kwargs):
            # Extract regime from run_id suffix (e.g. "...regime_bull")
            regime_key = run_id.split("_regime_")[-1] if "_regime_" in run_id else "unknown"
            captured_cfgs[regime_key] = cfg
            return PipelineResult(symbol=symbol, run_id=run_id, passed=True,
                                  metrics={"sharpe": 1.5, "n_trades": 10})

        def _fake_labels(df_full, cfg):
            # Return non-empty labels so regime splits are computed
            labels = pd.Series("bull", index=df_full.index)
            labels.iloc[100:200] = "bear"
            labels.iloc[200:250] = "crisis"
            return labels

        def _fake_splits(df_full, labels, cfg):
            # Return all three regimes as present
            return {
                "bull": df_full.iloc[:200],
                "bear": df_full.iloc[100:300],
                "crisis": df_full.iloc[200:260],
            }

        state = MagicMock()

        with patch("octa_training.core.pipeline.train_evaluate_adaptive", side_effect=_fake_train_adaptive), \
             patch("octa_training.core.regime_labels.classify_regimes", side_effect=_fake_labels), \
             patch("octa_training.core.regime_labels.get_regime_splits", side_effect=_fake_splits):
            result = train_regime_ensemble(
                symbol="TEST",
                timeframe="1D",
                cfg=cfg,
                state=state,
                run_id="test_run",
                parquet_path=str(pq),
            )

        # All three regimes should have been trained
        assert "bull" in captured_cfgs, "bull submodel not trained"
        assert "bear" in captured_cfgs, "bear submodel not trained"
        assert "crisis" in captured_cfgs, "crisis submodel not trained"

        # Non-crisis submodels must have crisis_oos disabled
        bull_oos = getattr(captured_cfgs["bull"], "crisis_oos", None)
        assert bull_oos is not None, "bull cfg missing crisis_oos"
        assert bull_oos.enabled is False, f"bull crisis_oos.enabled should be False, got {bull_oos.enabled}"

        bear_oos = getattr(captured_cfgs["bear"], "crisis_oos", None)
        assert bear_oos is not None, "bear cfg missing crisis_oos"
        assert bear_oos.enabled is False, f"bear crisis_oos.enabled should be False, got {bear_oos.enabled}"

        # Crisis submodel must retain crisis_oos enabled
        crisis_oos = getattr(captured_cfgs["crisis"], "crisis_oos", None)
        assert crisis_oos is not None, "crisis cfg missing crisis_oos"
        assert crisis_oos.enabled is True, f"crisis crisis_oos.enabled should be True, got {crisis_oos.enabled}"

    def test_no_crisis_oos_cfg_is_passthrough(self, tmp_path):
        """When crisis_oos is None, cfg passes through unchanged to all submodels."""
        import pandas as pd
        from unittest.mock import patch, MagicMock
        from octa_training.core.pipeline import train_regime_ensemble, PipelineResult
        from octa_training.core.config import TrainingConfig, RegimeEnsembleConfig

        re_cfg = RegimeEnsembleConfig(enabled=True, regimes=["bull", "crisis"], min_regimes_trained=1)
        cfg = TrainingConfig(regime_ensemble=re_cfg, crisis_oos=None)

        pq = tmp_path / "TEST_1D.parquet"
        idx = pd.date_range("2015-01-01", periods=600, freq="B", tz="UTC")
        df = pd.DataFrame({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6}, index=idx)
        df.to_parquet(str(pq))

        captured_cfgs: dict = {}

        def _fake_train_adaptive(symbol, cfg, state, run_id, **kwargs):
            regime_key = run_id.split("_regime_")[-1] if "_regime_" in run_id else "unknown"
            captured_cfgs[regime_key] = cfg
            return PipelineResult(symbol=symbol, run_id=run_id, passed=True,
                                  metrics={"sharpe": 1.5, "n_trades": 10})

        def _fake_labels(df_full, cfg):
            labels = pd.Series("bull", index=df_full.index)
            labels.iloc[200:250] = "crisis"
            return labels

        def _fake_splits(df_full, labels, cfg):
            return {"bull": df_full.iloc[:200], "crisis": df_full.iloc[200:260]}

        state = MagicMock()

        with patch("octa_training.core.pipeline.train_evaluate_adaptive", side_effect=_fake_train_adaptive), \
             patch("octa_training.core.regime_labels.classify_regimes", side_effect=_fake_labels), \
             patch("octa_training.core.regime_labels.get_regime_splits", side_effect=_fake_splits):
            train_regime_ensemble(
                symbol="TEST",
                timeframe="1D",
                cfg=cfg,
                state=state,
                run_id="test_run",
                parquet_path=str(pq),
            )

        # When crisis_oos is None, the original cfg object should be passed unchanged
        assert "bull" in captured_cfgs
        assert captured_cfgs["bull"] is cfg, "cfg should be passed unchanged when crisis_oos is None"


# ---------------------------------------------------------------------------
# I8: Per-regime artifact persistence and routing manifest
# ---------------------------------------------------------------------------

class TestRegimeArtifactPersistence:
    """Verify per-regime pkl artifacts and the RegimeRouter manifest.

    I8-A: Passing regimes produce <SYM>_<TF>_<regime>.pkl in the artifacts dir.
    I8-B: A failing regime does NOT produce an artifact; passing ones are kept.
    I8-C: Ensemble passes iff bull AND bear both produce validated artifacts.
    I8-D: <SYM>_<TF>_regime.pkl (router manifest) is always written.
    I8-E: All artifact paths in routing_table are load-validated.
    I8-F: require_bull=False lets bear-only pass.
    """

    # ------------------------------------------------------------------ helpers

    def _make_parquet(self, tmp_path: Path) -> Path:
        idx = pd.date_range("2015-01-01", periods=600, freq="B", tz="UTC")
        df = pd.DataFrame(
            {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6},
            index=idx,
        )
        pq = tmp_path / "TEST_1D.parquet"
        df.to_parquet(str(pq))
        return pq

    def _write_fake_pkl(self, path: Path, content: dict) -> Path:
        import pickle
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as fh:
            pickle.dump(content, fh, protocol=4)
        return path

    def _make_cfg(self, tmp_path: Path, require_bull=True, require_bear=True):
        from octa_training.core.config import TrainingConfig, RegimeEnsembleConfig
        arts_dir = str(tmp_path / "regime_arts")
        re_cfg = RegimeEnsembleConfig(
            enabled=True,
            regimes=["bull", "bear", "crisis"],
            min_regimes_trained=2,
            require_bull=require_bull,
            require_bear=require_bear,
            regime_artifacts_dir=arts_dir,
        )
        return TrainingConfig(regime_ensemble=re_cfg)

    def _fake_regime_labels(self, df_full, cfg=None):
        labels = pd.Series("bull", index=df_full.index)
        labels.iloc[100:300] = "bear"
        labels.iloc[300:360] = "crisis"
        return labels

    def _fake_regime_splits(self, df_full, labels, cfg=None):
        return {
            "bull": df_full.iloc[:300],
            "bear": df_full.iloc[100:400],
            "crisis": df_full.iloc[300:365],
        }

    def _run_ensemble(self, tmp_path, cfg, pq, per_regime_results: dict):
        """Run train_regime_ensemble with mocked submodel results keyed by regime."""
        from octa_training.core.pipeline import train_regime_ensemble, PipelineResult as _PR

        # NOTE: train_evaluate_adaptive is called with all keyword args, so the
        # mock must accept **kw rather than named positional params.
        def _fake_train(**kw):
            run_id = kw.get("run_id", "")
            regime_key = run_id.split("_regime_")[-1] if "_regime_" in run_id else "unknown"
            return per_regime_results.get(
                regime_key,
                _PR(symbol=kw.get("symbol", ""), run_id=run_id, passed=False, error="no_result"),
            )

        state = MagicMock()
        with (
            patch("octa_training.core.pipeline.train_evaluate_adaptive", side_effect=_fake_train),
            patch("octa_training.core.regime_labels.classify_regimes", side_effect=self._fake_regime_labels),
            patch("octa_training.core.regime_labels.get_regime_splits", side_effect=self._fake_regime_splits),
        ):
            return train_regime_ensemble(
                symbol="TEST",
                timeframe="1D",
                cfg=cfg,
                state=state,
                run_id="test_run",
                parquet_path=str(pq),
            )

    # ------------------------------------------------------------------ tests

    def test_passing_regimes_produce_per_regime_pkls(self, tmp_path):
        """I8-A: bull, bear, crisis all pass → three per-regime pkls created."""
        import pickle
        from octa_training.core.pipeline import PipelineResult

        pq = self._make_parquet(tmp_path)
        cfg = self._make_cfg(tmp_path)
        arts_dir = Path(cfg.regime_ensemble.regime_artifacts_dir)

        # Create fake source pkls (simulates save_tradeable_artifact output)
        src_pkls = {}
        for regime in ("bull", "bear", "crisis"):
            p = self._write_fake_pkl(tmp_path / f"src_{regime}.pkl", {"regime": regime, "ok": True})
            src_pkls[regime] = p

        per_regime_results = {
            r: PipelineResult(
                symbol="TEST", run_id=f"test_run_regime_{r}", passed=True,
                pack_result={"saved": True, "pkl": str(src_pkls[r])},
            )
            for r in ("bull", "bear", "crisis")
        }

        result = self._run_ensemble(tmp_path, cfg, pq, per_regime_results)

        assert result.passed, f"ensemble should pass with all three regimes; error={result.error}"
        for regime in ("bull", "bear", "crisis"):
            dst = arts_dir / "TEST" / "1D" / f"TEST_1D_{regime}.pkl"
            assert dst.exists(), f"missing per-regime artifact: {dst}"
            with open(dst, "rb") as fh:
                content = pickle.load(fh)
            assert content["regime"] == regime, f"artifact content mismatch for {regime}"

    def test_crisis_failure_preserves_bull_bear_artifacts(self, tmp_path):
        """I8-B: bull+bear pass, crisis fails → bull/bear pkls kept, crisis absent."""
        import pickle
        from octa_training.core.pipeline import PipelineResult

        pq = self._make_parquet(tmp_path)
        cfg = self._make_cfg(tmp_path)
        arts_dir = Path(cfg.regime_ensemble.regime_artifacts_dir)

        bull_src = self._write_fake_pkl(tmp_path / "src_bull.pkl", {"regime": "bull"})
        bear_src = self._write_fake_pkl(tmp_path / "src_bear.pkl", {"regime": "bear"})

        per_regime_results = {
            "bull": PipelineResult(
                symbol="TEST", run_id="run_bull", passed=True,
                pack_result={"saved": True, "pkl": str(bull_src)},
            ),
            "bear": PipelineResult(
                symbol="TEST", run_id="run_bear", passed=True,
                pack_result={"saved": True, "pkl": str(bear_src)},
            ),
            "crisis": PipelineResult(
                symbol="TEST", run_id="run_crisis", passed=False,
                pack_result=None, error="gate_fail",
            ),
        }

        result = self._run_ensemble(tmp_path, cfg, pq, per_regime_results)

        assert result.passed, f"bull+bear should be enough; error={result.error}"
        assert (arts_dir / "TEST" / "1D" / "TEST_1D_bull.pkl").exists()
        assert (arts_dir / "TEST" / "1D" / "TEST_1D_bear.pkl").exists()
        assert not (arts_dir / "TEST" / "1D" / "TEST_1D_crisis.pkl").exists()
        assert "bull" in result.regime_artifact_paths
        assert "bear" in result.regime_artifact_paths
        assert "crisis" not in result.regime_artifact_paths

    def test_bull_only_fails_ensemble(self, tmp_path):
        """I8-C: only bull passes → ensemble fails (bear required by default)."""
        from octa_training.core.pipeline import PipelineResult

        pq = self._make_parquet(tmp_path)
        cfg = self._make_cfg(tmp_path)

        bull_src = self._write_fake_pkl(tmp_path / "src_bull.pkl", {"regime": "bull"})
        per_regime_results = {
            "bull": PipelineResult(
                symbol="TEST", run_id="run_bull", passed=True,
                pack_result={"saved": True, "pkl": str(bull_src)},
            ),
            "bear": PipelineResult(
                symbol="TEST", run_id="run_bear", passed=False,
                error="gate_fail",
            ),
            "crisis": PipelineResult(
                symbol="TEST", run_id="run_crisis", passed=False,
                error="gate_fail",
            ),
        }

        result = self._run_ensemble(tmp_path, cfg, pq, per_regime_results)

        assert not result.passed, "ensemble must fail when bear is absent"
        assert result.error is not None and "bear" in result.error, (
            f"error should mention missing bear; got: {result.error}"
        )

    def test_router_manifest_written_with_correct_structure(self, tmp_path):
        """I8-D: regime.pkl written; manifest contains symbol/tf/run_id/routing_table."""
        import pickle
        from octa_training.core.pipeline import PipelineResult

        pq = self._make_parquet(tmp_path)
        cfg = self._make_cfg(tmp_path)
        arts_dir = Path(cfg.regime_ensemble.regime_artifacts_dir)

        bull_src = self._write_fake_pkl(tmp_path / "src_bull.pkl", {"regime": "bull"})
        bear_src = self._write_fake_pkl(tmp_path / "src_bear.pkl", {"regime": "bear"})

        per_regime_results = {
            "bull": PipelineResult(
                symbol="TEST", run_id="run_bull", passed=True,
                pack_result={"saved": True, "pkl": str(bull_src)},
            ),
            "bear": PipelineResult(
                symbol="TEST", run_id="run_bear", passed=True,
                pack_result={"saved": True, "pkl": str(bear_src)},
            ),
            "crisis": PipelineResult(
                symbol="TEST", run_id="run_crisis", passed=False,
                error="gate_fail",
            ),
        }

        result = self._run_ensemble(tmp_path, cfg, pq, per_regime_results)

        router_path = arts_dir / "TEST" / "1D" / "TEST_1D_regime.pkl"
        assert router_path.exists(), "regime.pkl (router manifest) must be written"
        assert result.router_path == str(router_path)

        with open(router_path, "rb") as fh:
            manifest = pickle.load(fh)

        assert manifest["kind"] == "regime_router"
        assert manifest["symbol"] == "TEST"
        assert manifest["timeframe"] == "1D"
        assert manifest["run_id"] == "test_run"
        assert manifest["passed"] is True
        assert manifest["fail_closed"] is True
        # routing_table must have exactly bull and bear
        rt = manifest["routing_table"]
        assert "bull" in rt
        assert "bear" in rt
        assert "crisis" not in rt
        # per-regime status
        assert manifest["regimes"]["bull"]["status"] == "PASS"
        assert manifest["regimes"]["bear"]["status"] == "PASS"
        assert manifest["regimes"]["crisis"]["status"] == "FAIL"
        assert manifest["regimes"]["bull"]["artifact_validated"] is True
        assert manifest["regimes"]["bear"]["artifact_validated"] is True
        assert manifest["regimes"]["crisis"]["artifact_validated"] is False

    def test_router_manifest_artifact_paths_all_loadable(self, tmp_path):
        """I8-E: every path in routing_table points to a loadable pkl."""
        import pickle
        from octa_training.core.pipeline import PipelineResult

        pq = self._make_parquet(tmp_path)
        cfg = self._make_cfg(tmp_path)
        arts_dir = Path(cfg.regime_ensemble.regime_artifacts_dir)

        bull_src = self._write_fake_pkl(tmp_path / "src_bull.pkl", {"regime": "bull"})
        bear_src = self._write_fake_pkl(tmp_path / "src_bear.pkl", {"regime": "bear"})

        per_regime_results = {
            "bull": PipelineResult(
                symbol="TEST", run_id="run_bull", passed=True,
                pack_result={"saved": True, "pkl": str(bull_src)},
            ),
            "bear": PipelineResult(
                symbol="TEST", run_id="run_bear", passed=True,
                pack_result={"saved": True, "pkl": str(bear_src)},
            ),
            "crisis": PipelineResult(
                symbol="TEST", run_id="run_crisis", passed=False,
                error="gate_fail",
            ),
        }

        self._run_ensemble(tmp_path, cfg, pq, per_regime_results)

        router_path = arts_dir / "TEST" / "1D" / "TEST_1D_regime.pkl"
        with open(router_path, "rb") as fh:
            manifest = pickle.load(fh)

        for regime, art_path in manifest["routing_table"].items():
            p = Path(art_path)
            assert p.exists(), f"routing_table[{regime}] path does not exist: {art_path}"
            with open(p, "rb") as fh:
                obj = pickle.load(fh)
            assert obj is not None, f"routing_table[{regime}] artifact is empty"

    def test_require_bull_false_allows_bear_only_ensemble(self, tmp_path):
        """I8-F: require_bull=False → ensemble passes if bear passes, even without bull."""
        from octa_training.core.pipeline import PipelineResult

        pq = self._make_parquet(tmp_path)
        cfg = self._make_cfg(tmp_path, require_bull=False, require_bear=True)

        bear_src = self._write_fake_pkl(tmp_path / "src_bear.pkl", {"regime": "bear"})
        per_regime_results = {
            "bull": PipelineResult(
                symbol="TEST", run_id="run_bull", passed=False, error="gate_fail",
            ),
            "bear": PipelineResult(
                symbol="TEST", run_id="run_bear", passed=True,
                pack_result={"saved": True, "pkl": str(bear_src)},
            ),
            "crisis": PipelineResult(
                symbol="TEST", run_id="run_crisis", passed=False, error="gate_fail",
            ),
        }

        result = self._run_ensemble(tmp_path, cfg, pq, per_regime_results)

        assert result.passed, f"bear-only should pass when require_bull=False; error={result.error}"
        assert "bear" in result.regime_artifact_paths
        assert "bull" not in result.regime_artifact_paths
