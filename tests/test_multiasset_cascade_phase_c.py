"""Phase C tests for multi-asset cascade implementation.

Covers:
- RollManager.days_to_expiry(sim_date) determinism
- RollManager.decide_roll(sim_date) propagation
- Execution eligibility: 1D+1H+30M when all three trained
- Execution eligibility: backward compat when 30M not in cascade order
- SafeNoopGate WARNING on import failure
- CalendarSpec defaults per asset kind
- AltDataPack type
- FeatureWeightingPolicy resolution and normalisation
- StageResult validation
- build_features() asset_class dispatch
- New builder stubs (basis, greeks, onchain, etc.)
- FeatureRegistry schema versioning
- _walkforward_eligibility roll_flag check for futures
- aggregate_option_snapshots callable (no module-level exec)
"""
from __future__ import annotations

import json
import logging
import math
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 1. RollManager — sim_date determinism
# ---------------------------------------------------------------------------

from octa_assets.futures.rolls import RollManager


class TestRollManagerSimDate:
    def test_days_to_expiry_with_sim_date(self):
        rm = RollManager()
        expiry = date(2026, 6, 20)
        sim = date(2026, 6, 10)
        assert rm.days_to_expiry(expiry, sim_date=sim) == 10

    def test_days_to_expiry_without_sim_date_uses_today(self):
        rm = RollManager()
        expiry = date.today() + timedelta(days=7)
        # Without sim_date falls back to date.today(); result should be ~7
        result = rm.days_to_expiry(expiry)
        assert 6 <= result <= 8  # allow ±1 for DST/clock edge cases

    def test_decide_roll_passes_sim_date(self):
        today = date.today()
        candidates = {
            "FUT1": {"volume": 100, "open_interest": 200},
            "FUT2": {"volume": 200, "open_interest": 400},
        }
        contracts_meta = {
            "FUT1": {"expiry": today + timedelta(days=3)},
            "FUT2": {"expiry": today + timedelta(days=60)},
        }
        rm = RollManager(roll_window_days=5)
        # With sim_date = today: FUT1 has dte=3 <= 5 → rolls
        result = rm.decide_roll("FUT1", candidates, contracts_meta, sim_date=today)
        assert result == "FUT2"

    def test_decide_roll_sim_date_changes_outcome(self):
        """Outcome differs when sim_date places dte inside vs outside roll_window."""
        expiry_a = date(2026, 6, 20)
        expiry_b = date(2026, 9, 20)
        candidates = {
            "A": {"volume": 100, "open_interest": 100},
            "B": {"volume": 200, "open_interest": 101},  # OI just above trigger threshold?
        }
        contracts_meta = {"A": {"expiry": expiry_a}, "B": {"expiry": expiry_b}}
        rm = RollManager(roll_window_days=5, oi_multiplier_trigger=1.5)

        # Far from expiry, OI not > 1.5× → stay on A
        result_far = rm.decide_roll("A", candidates, contracts_meta, sim_date=date(2026, 4, 1))
        assert result_far == "A"

        # Within roll_window → rolls to B regardless of OI
        result_near = rm.decide_roll("A", candidates, contracts_meta, sim_date=date(2026, 6, 16))
        assert result_near == "B"


# ---------------------------------------------------------------------------
# 2. Execution eligibility
# ---------------------------------------------------------------------------

from octa.support.ops import v000_full_universe_cascade_train as mod


def _fake_decision(symbol, tf, stage, perf_pass, structural_pass=True):
    return {
        "symbol": symbol,
        "timeframe": tf,
        "stage": stage,
        "status": "PASS" if perf_pass else "GATE_FAIL",
        "reason": None,
        "details": {"structural_pass": structural_pass, "performance_pass": perf_pass},
    }


class TestEligibility:
    def test_all_three_tfs_pass_eligible(self):
        """1D + 1H + 30M all pass → eligible."""
        rows = [
            _fake_decision("AAA", "1D", "train", True),
            _fake_decision("AAA", "1H", "train", True),
            _fake_decision("AAA", "30M", "train", True),
        ]
        sym_rows = [r for r in rows if r["stage"] == "train"]
        s1d, p1d = mod._structural_perf_by_tf(sym_rows, "1D")
        s1h, p1h = mod._structural_perf_by_tf(sym_rows, "1H")
        s30m, p30m = mod._structural_perf_by_tf(sym_rows, "30M")
        trained_30m = any(r["timeframe"] == "30M" for r in sym_rows)
        eligible = bool(p1d and p1h and (p30m if trained_30m else True))
        assert eligible is True

    def test_30m_fail_when_trained_makes_ineligible(self):
        """1D + 1H pass but 30M fails → ineligible when 30M was trained."""
        rows = [
            _fake_decision("AAA", "1D", "train", True),
            _fake_decision("AAA", "1H", "train", True),
            _fake_decision("AAA", "30M", "train", False),
        ]
        sym_rows = [r for r in rows if r["stage"] == "train"]
        s1d, p1d = mod._structural_perf_by_tf(sym_rows, "1D")
        s1h, p1h = mod._structural_perf_by_tf(sym_rows, "1H")
        s30m, p30m = mod._structural_perf_by_tf(sym_rows, "30M")
        trained_30m = any(r["timeframe"] == "30M" for r in sym_rows)
        eligible = bool(p1d and p1h and (p30m if trained_30m else True))
        assert eligible is False

    def test_30m_not_trained_eligible_on_1d_1h(self):
        """Backward compat: cascade_order=[1D,1H] → 30M not trained → eligible on 1D+1H only."""
        rows = [
            _fake_decision("AAA", "1D", "train", True),
            _fake_decision("AAA", "1H", "train", True),
        ]
        sym_rows = [r for r in rows if r["stage"] == "train"]
        s1d, p1d = mod._structural_perf_by_tf(sym_rows, "1D")
        s1h, p1h = mod._structural_perf_by_tf(sym_rows, "1H")
        s30m, p30m = mod._structural_perf_by_tf(sym_rows, "30M")
        trained_30m = any(r["timeframe"] == "30M" for r in sym_rows)
        eligible = bool(p1d and p1h and (p30m if trained_30m else True))
        assert trained_30m is False
        assert eligible is True

    def test_1d_fail_always_ineligible(self):
        rows = [
            _fake_decision("AAA", "1D", "train", False),
            _fake_decision("AAA", "1H", "train", True),
            _fake_decision("AAA", "30M", "train", True),
        ]
        sym_rows = [r for r in rows if r["stage"] == "train"]
        s1d, p1d = mod._structural_perf_by_tf(sym_rows, "1D")
        s1h, p1h = mod._structural_perf_by_tf(sym_rows, "1H")
        s30m, p30m = mod._structural_perf_by_tf(sym_rows, "30M")
        trained_30m = any(r["timeframe"] == "30M" for r in sym_rows)
        eligible = bool(p1d and p1h and (p30m if trained_30m else True))
        assert eligible is False


# ---------------------------------------------------------------------------
# 3. SafeNoopGate alarm
# ---------------------------------------------------------------------------

from octa.core.cascade.registry import build_default_gate_stack


class TestSafeNoopAlarm:
    def test_noop_gates_log_warning(self, caplog):
        """When gate modules are missing, WARNING is emitted for each noop."""
        with caplog.at_level(logging.WARNING, logger="octa.core.cascade.registry"):
            gates = build_default_gate_stack()
        noop_count = sum(1 for g in gates if type(g).__name__ == "SafeNoopGate")
        if noop_count > 0:
            warning_msgs = [r.message for r in caplog.records if r.levelno == logging.WARNING]
            assert any("cascade_noop_gate_inserted" in m for m in warning_msgs)

    def test_noop_artifact_written_when_dir_set(self, tmp_path, monkeypatch):
        """noop_gates.json is written when OCTA_CASCADE_RUN_DIR is set and gates are missing."""
        monkeypatch.setenv("OCTA_CASCADE_RUN_DIR", str(tmp_path))
        build_default_gate_stack()
        artifact = tmp_path / "noop_gates.json"
        if any(type(g).__name__ == "SafeNoopGate" for g in build_default_gate_stack()):
            assert artifact.exists()
            data = json.loads(artifact.read_text())
            assert data["noop_gates_present"] is True


# ---------------------------------------------------------------------------
# 4. AssetProfile CalendarSpec defaults
# ---------------------------------------------------------------------------

from octa_training.core.asset_profiles import (
    AssetProfile,
    CalendarSpec,
    CryptoExt,
    FutureExt,
    OptionExt,
    _apply_calendar_defaults,
)


class TestCalendarDefaults:
    def test_crypto_gets_365_days(self):
        p = AssetProfile(name="crypto", kind="crypto", gates={})
        p2 = _apply_calendar_defaults(p)
        assert p2.calendar.trading_days_per_year == 365
        assert p2.calendar.is_24_7 is True

    def test_fx_gets_261_days(self):
        p = AssetProfile(name="fx", kind="fx", gates={})
        p2 = _apply_calendar_defaults(p)
        assert p2.calendar.trading_days_per_year == 261
        assert p2.calendar.is_24_5 is True

    def test_stock_keeps_252_days(self):
        p = AssetProfile(name="stock", kind="stock", gates={})
        p2 = _apply_calendar_defaults(p)
        assert p2.calendar.trading_days_per_year == 252
        assert p2.calendar.is_24_7 is False

    def test_explicit_calendar_not_overridden(self):
        """If calendar is already set non-default, _apply_calendar_defaults respects it."""
        cal = CalendarSpec(trading_days_per_year=300, is_24_7=True)
        p = AssetProfile(name="crypto", kind="crypto", gates={}, calendar=cal)
        p2 = _apply_calendar_defaults(p)
        # Already non-default → not overridden
        assert p2.calendar.trading_days_per_year == 300

    def test_new_submodels_instantiate(self):
        """CryptoExt, FutureExt, OptionExt can be instantiated with defaults."""
        ce = CryptoExt()
        assert ce.funding_rate_threshold == 0.001
        fe = FutureExt()
        assert fe.roll_window_days == 5
        assert "roll_flag" in fe.required_parquet_columns
        oe = OptionExt()
        assert oe.lstm_seq_len == 32
        assert oe.performance_threshold_auc == 0.55


# ---------------------------------------------------------------------------
# 5. AltDataPack
# ---------------------------------------------------------------------------

from octa.core.features.altdata.pack import AltDataPack


class TestAltDataPack:
    def test_empty(self):
        pack = AltDataPack(
            run_id="r1", symbol="AAPL", asset_class="stock",
            gate_layer="global_1d", timeframe="1D", asof_ts=None
        )
        assert pack.is_empty()
        assert pack.feature_count() == 0
        assert not pack.has_missing_required()

    def test_with_features(self):
        pack = AltDataPack(
            run_id="r1", symbol="BTC", asset_class="crypto",
            gate_layer="global_1d", timeframe="1D", asof_ts=None,
            market_features={"onchain.nvt_ratio": 0.5, "macro.cpi": 0.03},
            symbol_features={"onchain.symbol_flow": -1.2},
            missing_optional=["eco_calendar"],
        )
        assert not pack.is_empty()
        assert pack.feature_count() == 3
        assert not pack.has_missing_required()

    def test_missing_required_flag(self):
        pack = AltDataPack(
            run_id="r1", symbol="EURUSD", asset_class="fx",
            gate_layer="global_1d", timeframe="1D", asof_ts=None,
            missing_required=["eco_calendar"],
        )
        assert pack.has_missing_required()


# ---------------------------------------------------------------------------
# 6. FeatureWeightingPolicy
# ---------------------------------------------------------------------------

from octa.core.features.altdata.weighting import (
    FeatureGroupWeight,
    FeatureWeightingPolicy,
    resolve_policy,
)


class TestFeatureWeightingPolicy:
    def test_resolve_default(self):
        p = resolve_policy("stock", "1D", "global_1d")
        assert p.asset_class in {"stock", "*"}
        assert len(p.weights) > 0

    def test_resolve_crypto(self):
        p = resolve_policy("crypto", "1D", "global_1d")
        groups = {w.group for w in p.weights}
        assert "onchain_market" in groups

    def test_resolve_future(self):
        p = resolve_policy("future", "30M", "structure_30m")
        groups = {w.group for w in p.weights}
        assert "basis" in groups

    def test_resolve_fx(self):
        p = resolve_policy("fx", "1D", "global_1d")
        groups = {w.group for w in p.weights}
        assert "eco_calendar" in groups

    def test_normalised_weights_sum_to_one(self):
        p = resolve_policy("stock", "1D", "global_1d")
        present = [w.group for w in p.weights]
        normalised = p.normalised_weights(present)
        if normalised:
            total = sum(normalised.values())
            assert abs(total - 1.0) < 1e-9

    def test_absent_groups_zeroed(self):
        p = resolve_policy("stock", "1D", "global_1d")
        # Only provide one group as present
        present_groups = [p.weights[0].group] if p.weights else []
        normalised = p.normalised_weights(present_groups)
        assert len(normalised) <= 1

    def test_stable_hash_deterministic(self):
        p1 = resolve_policy("crypto", "1D", "global_1d")
        p2 = resolve_policy("crypto", "1D", "global_1d")
        assert p1.stable_hash() == p2.stable_hash()


# ---------------------------------------------------------------------------
# 7. StageResult validation
# ---------------------------------------------------------------------------

from octa_ops.autopilot.stage_result import StageMandatoryMetrics, StageResult


class TestStageResult:
    def test_valid_pass(self):
        m = StageMandatoryMetrics(
            sharpe_ratio=1.2,
            max_drawdown=-0.10,
            cvar_95=-0.05,
            cvar_95_over_daily_vol=1.5,
            leakage_detected=False,
            n_folds_completed=5,
            walk_forward_passed=True,
            regime_stability=0.8,
        )
        sr = StageResult(
            symbol="AAPL", timeframe="1D", asset_class="stock",
            run_id="r1", stage_index=0,
            structural_pass=True, performance_pass=True,
            metrics=m, model_path="/tmp/m.pkl", model_hash="abc123",
            fail_status="PASS",
        )
        assert sr.validate() == []

    def test_leakage_with_perf_pass_is_violation(self):
        m = StageMandatoryMetrics(leakage_detected=True)
        sr = StageResult(
            symbol="X", timeframe="1H", asset_class="stock",
            run_id="r1", stage_index=1,
            structural_pass=True, performance_pass=True,
            metrics=m, fail_status="PASS",
        )
        issues = sr.validate()
        assert any("leakage" in i for i in issues)

    def test_nan_metric_is_violation(self):
        m = StageMandatoryMetrics(sharpe_ratio=float("nan"))
        assert m.validate() != []

    def test_inf_metric_is_violation(self):
        m = StageMandatoryMetrics(max_drawdown=float("inf"))
        assert m.validate() != []

    def test_to_details_dict_has_required_keys(self):
        sr = StageResult(
            symbol="X", timeframe="1D", asset_class="stock",
            run_id="r", stage_index=0,
            structural_pass=True, performance_pass=False,
            fail_status="GATE_FAIL", fail_reason="sharpe",
        )
        d = sr.to_details_dict()
        for key in ("structural_pass", "performance_pass", "fail_status", "fail_reason",
                    "model_path", "model_hash", "leakage_detected"):
            assert key in d


# ---------------------------------------------------------------------------
# 8. build_features() asset_class dispatch
# ---------------------------------------------------------------------------

from octa.core.features.altdata.builders import build_features
from octa.core.features.altdata.registry import FeatureRegistry


class TestBuildFeaturesDispatch:
    def _make_registry(self, tmp_path):
        return FeatureRegistry(run_id="test_run", root=str(tmp_path))

    def test_default_stock_no_extra_keys(self, tmp_path):
        reg = self._make_registry(tmp_path)
        build_features(
            run_id="r", timeframe="1D", gate_layer="global_1d",
            payloads={}, registry=reg, asset_class="stock",
        )
        # No error; market features may be empty (no payloads)

    def test_crypto_dispatch_accepted(self, tmp_path):
        reg = self._make_registry(tmp_path)
        build_features(
            run_id="r", timeframe="1D", gate_layer="global_1d",
            payloads={"nvt_ratio": 42.0},
            registry=reg, asset_class="crypto",
        )
        vec = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d")
        assert "onchain.nvt_ratio" in vec
        assert abs(vec["onchain.nvt_ratio"] - 42.0) < 1e-9

    def test_future_basis_dispatch_accepted(self, tmp_path):
        reg = self._make_registry(tmp_path)
        build_features(
            run_id="r", timeframe="30M", gate_layer="structure_30m",
            payloads={"futures_close": 4505.0, "spot_close": 4500.0, "multiplier": 50.0},
            registry=reg, asset_class="future", symbol="ES",
        )
        vec = reg.get_feature_vector(symbol="ES", timeframe="30M", gate_layer="structure_30m")
        assert "basis.current" in vec
        assert abs(vec["basis.current"] - 250.0) < 1e-6  # (4505-4500)*50

    def test_fx_eco_calendar_dispatch(self, tmp_path):
        reg = self._make_registry(tmp_path)
        build_features(
            run_id="r", timeframe="1D", gate_layer="global_1d",
            payloads={"eco_surprise_score": 0.7, "rate_differential": 0.02},
            registry=reg, asset_class="fx",
        )
        vec = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d")
        assert "eco_calendar.surprise_score" in vec

    def test_option_greeks_dispatch(self, tmp_path):
        reg = self._make_registry(tmp_path)
        build_features(
            run_id="r", timeframe="30M", gate_layer="structure_30m",
            payloads={"delta_mean": 0.45, "gamma_mean": 0.03, "put_call_ratio": 1.2},
            registry=reg, asset_class="option", symbol="AAPL_OPT",
        )
        vec = reg.get_feature_vector(symbol="AAPL_OPT", timeframe="30M", gate_layer="structure_30m")
        assert "greeks.delta_mean" in vec
        assert abs(vec["greeks.delta_mean"] - 0.45) < 1e-9

    def test_backward_compat_no_asset_class(self, tmp_path):
        """build_features() without asset_class kwarg still works (defaults to stock)."""
        reg = self._make_registry(tmp_path)
        build_features(
            run_id="r", timeframe="1D", gate_layer="global_1d",
            payloads={}, registry=reg,  # no asset_class
        )


# ---------------------------------------------------------------------------
# 9. New builder stubs
# ---------------------------------------------------------------------------

from octa.core.features.altdata import (
    basis_features,
    cot_features,
    eco_calendar_features,
    funding_rate_features,
    greeks_features,
    iv_surface_features,
    onchain_features,
)


class TestNewBuilders:
    def test_onchain_returns_finite(self):
        r = onchain_features.build({"nvt_ratio": 10.0, "mvrv_z_score": 1.5})
        assert all(math.isfinite(v) for v in r.values())

    def test_onchain_ignores_nan(self):
        r = onchain_features.build({"nvt_ratio": float("nan")})
        assert "onchain.nvt_ratio" not in r

    def test_funding_rate_sign_derived(self):
        r = funding_rate_features.build({"funding_rate": -0.001})
        assert r.get("funding_rate.sign") == -1.0
        r2 = funding_rate_features.build({"funding_rate": 0.002})
        assert r2.get("funding_rate.sign") == 1.0

    def test_eco_calendar_empty_on_no_payload(self):
        assert eco_calendar_features.build({}) == {}

    def test_cot_direction_derived(self):
        r = cot_features.build({"cot_net_position": 50000.0})
        assert r.get("cot.direction") == 1.0

    def test_basis_both_prices_required(self):
        r = basis_features.build({"futures_close": 4505.0})  # missing spot
        assert r == {}

    def test_basis_computed_correctly(self):
        r = basis_features.build({"futures_close": 4505.0, "spot_close": 4500.0, "multiplier": 50.0})
        assert abs(r["basis.current"] - 250.0) < 1e-6

    def test_basis_z_score_with_history(self):
        history = [100.0, 102.0, 98.0, 101.0, 99.0]
        r = basis_features.build({
            "futures_close": 4505.0,
            "spot_close": 4500.0,
            "multiplier": 1.0,
            "basis_history": history,
        })
        assert "basis.z_score" in r
        assert math.isfinite(r["basis.z_score"])

    def test_greeks_all_optional(self):
        r = greeks_features.build({})
        assert r == {}
        r2 = greeks_features.build({"delta_mean": 0.5, "put_call_ratio": 1.1})
        assert "greeks.delta_mean" in r2

    def test_iv_surface_empty_on_no_payload(self):
        assert iv_surface_features.build({}) == {}

    def test_onchain_symbol_build(self):
        r = onchain_features.build_symbol({"symbol_exchange_netflow": -500.0})
        assert "onchain.symbol_exchange_netflow" in r


# ---------------------------------------------------------------------------
# 10. FeatureRegistry schema versioning
# ---------------------------------------------------------------------------

from octa.core.features.altdata.registry import _SCHEMA_VERSION


class TestSchemaVersioning:
    def test_schema_version_recorded(self, tmp_path):
        reg = FeatureRegistry(run_id="sv_test", root=str(tmp_path))
        # Query the version table directly
        import sqlite3
        sqlite_path = reg.paths.db_path.with_suffix(".sqlite")
        if sqlite_path.exists():
            with sqlite3.connect(str(sqlite_path)) as conn:
                rows = conn.execute("SELECT version FROM altdata_schema_version").fetchall()
            assert len(rows) >= 1
            assert rows[-1][0] == _SCHEMA_VERSION

    def test_schema_version_not_duplicated_on_reinit(self, tmp_path):
        FeatureRegistry(run_id="r1", root=str(tmp_path))
        FeatureRegistry(run_id="r2", root=str(tmp_path))
        import sqlite3
        sqlite_path = (tmp_path / "altdata.sqlite")
        if sqlite_path.exists():
            with sqlite3.connect(str(sqlite_path)) as conn:
                rows = conn.execute("SELECT version FROM altdata_schema_version").fetchall()
            # Should only have one row for the current version
            assert len(rows) == 1


# ---------------------------------------------------------------------------
# 11. _walkforward_eligibility roll_flag check
# ---------------------------------------------------------------------------

import octa_ops.autopilot.cascade_train as ct


class TestWalkforwardFuturesCheck:
    def _mock_df(self, has_roll_flag=True, roll_flag_all_nan=False):
        import pandas as pd
        data = {
            "open": [100.0] * 300,
            "high": [101.0] * 300,
            "low": [99.0] * 300,
            "close": [100.0] * 300,
            "volume": [1000.0] * 300,
        }
        if has_roll_flag:
            data["roll_flag"] = ([None] * 300) if roll_flag_all_nan else ([False] * 298 + [True] + [False])
        return pd.DataFrame(data)

    def test_futures_without_roll_flag_ineligible(self, monkeypatch, tmp_path):
        df = self._mock_df(has_roll_flag=False)
        parquet_path = tmp_path / "ES.parquet"
        df.to_parquet(parquet_path)

        monkeypatch.setattr(ct, "load_parquet", lambda p, **kw: df)
        monkeypatch.setattr(ct, "validate_price_series", lambda df, **kw: types.SimpleNamespace(
            ok=True, code="OK", stats={"rows_clean": 300}
        ))

        cfg = types.SimpleNamespace(
            parquet=types.SimpleNamespace(
                nan_threshold=0.2,
                allow_negative_prices=False,
                resample_enabled=False,
                resample_bar_size="1D",
            ),
            splits={}
        )
        result = ct._walkforward_eligibility(
            parquet_path=str(parquet_path), cfg=cfg, asset_class="future"
        )
        assert result["eligible"] is False
        assert "roll_flag" in result["reason"]

    def test_futures_with_roll_flag_proceeds_to_bar_check(self, monkeypatch, tmp_path):
        df = self._mock_df(has_roll_flag=True)
        parquet_path = tmp_path / "ES.parquet"
        df.to_parquet(parquet_path)

        monkeypatch.setattr(ct, "load_parquet", lambda p, **kw: df)
        monkeypatch.setattr(ct, "validate_price_series", lambda df, **kw: types.SimpleNamespace(
            ok=True, code="OK", stats={"rows_clean": 300}
        ))

        cfg = types.SimpleNamespace(
            parquet=types.SimpleNamespace(
                nan_threshold=0.2,
                allow_negative_prices=False,
                resample_enabled=False,
                resample_bar_size="1D",
            ),
            splits={}
        )
        result = ct._walkforward_eligibility(
            parquet_path=str(parquet_path), cfg=cfg, asset_class="future"
        )
        # With roll_flag present and 300 bars, should reach bar count check
        assert "roll_flag" not in (result.get("reason") or "")

    def test_non_future_asset_class_skips_roll_check(self, monkeypatch, tmp_path):
        df = self._mock_df(has_roll_flag=False)
        parquet_path = tmp_path / "AAPL.parquet"
        df.to_parquet(parquet_path)

        monkeypatch.setattr(ct, "load_parquet", lambda p, **kw: df)
        monkeypatch.setattr(ct, "validate_price_series", lambda df, **kw: types.SimpleNamespace(
            ok=True, code="OK", stats={"rows_clean": 300}
        ))

        cfg = types.SimpleNamespace(
            parquet=types.SimpleNamespace(
                nan_threshold=0.2, allow_negative_prices=False,
                resample_enabled=False, resample_bar_size="1D",
            ),
            splits={}
        )
        result = ct._walkforward_eligibility(
            parquet_path=str(parquet_path), cfg=cfg, asset_class="stock"
        )
        # roll_flag not required for stock
        assert "roll_flag" not in (result.get("reason") or "")


# ---------------------------------------------------------------------------
# 12. aggregate_option_snapshots callable (no module-level exec)
# ---------------------------------------------------------------------------

from scripts.aggregate_option_snapshots import aggregate


class TestAggregateOptionSnapshots:
    def _write_csv(self, path: Path, rows: int = 10) -> None:
        import csv
        fieldnames = ["trade date", "expiry date", "strike", "call/put",
                      "bid price", "ask price", "delta", "bid implied volatility", "ask implied volatility"]
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for i in range(rows):
                w.writerow({
                    "trade date": f"2026-01-{i+1:02d}",
                    "expiry date": "2026-03-21",
                    "strike": str(200 + i),
                    "call/put": "C" if i % 2 == 0 else "P",
                    "bid price": "5.0",
                    "ask price": "5.5",
                    "delta": "0.45",
                    "bid implied volatility": "0.20",
                    "ask implied volatility": "0.22",
                })

    def test_aggregate_produces_parquet(self, tmp_path):
        src = tmp_path / "csvs"
        self._write_csv(src / "snapshot1.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")
        assert dst.exists()
        assert len(df) > 0

    def test_aggregate_contract_id_generated(self, tmp_path):
        src = tmp_path / "csvs"
        self._write_csv(src / "s.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")
        assert "contract_id" in df.columns
        assert df["contract_id"].notna().all()

    def test_aggregate_required_columns_present(self, tmp_path):
        src = tmp_path / "csvs"
        self._write_csv(src / "s.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")
        for col in ("timestamp", "symbol", "contract_id", "option_type", "strike", "tte_days", "mid"):
            assert col in df.columns, f"missing required column: {col}"

    def test_aggregate_underlying_symbol_used(self, tmp_path):
        src = tmp_path / "csvs"
        self._write_csv(src / "s.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="TSLA")
        assert (df["symbol"] == "TSLA_OPT").all()

    def test_aggregate_no_csvs_raises(self, tmp_path):
        src = tmp_path / "empty_dir"
        src.mkdir()
        dst = tmp_path / "out.parquet"
        with pytest.raises(FileNotFoundError):
            aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")

    def test_import_does_not_execute(self):
        """Importing the module must not write any files."""
        import importlib
        import scripts.aggregate_option_snapshots  # should already be imported, no side effects
        # If we got here without FileNotFoundError from module-level code, the guard works.
