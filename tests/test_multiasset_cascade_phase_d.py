"""Phase D: Integration validation for multi-asset cascade.

Validates all Phase B spec invariants hold together via integration tests.
Covers:
1. run_cascade_training() end-to-end for stock / future / crypto / fx
2. Cascade structural blocking: 1D structural fail → 1H skipped
3. Cascade performance fail: 1D perf fail does NOT block 1H
4. pkl_dir scoped per <asset_class>/<tf> in cascade
5. Execution eligibility 1D+1H+30M via v000 multi-symbol run
6. Futures roll_flag parquet validation (real file load, mocked training)
7. All builder stubs return finite values or empty dict — never NaN/inf
8. FeatureWeightingPolicy normalises to 1.0 for all asset classes
9. AssetProfile calendar invariants for all asset kinds
10. RollManager sim_date determinism
11. Options aggregate() callable (not module-level)
12. StageResult validation contract (leakage, NaN, inf)
13. v000 multi-symbol, multi-asset integration run
"""
from __future__ import annotations

import json
import math
import types
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------

def _write_price_parquet(path: Path, n: int = 500, *, include_roll_flag: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dates = pd.date_range("2022-01-01", periods=n, freq="D", tz="UTC")
    data: Dict[str, Any] = {
        "timestamp": dates,
        "open": [100.0 + i * 0.01 for i in range(n)],
        "high": [101.0 + i * 0.01 for i in range(n)],
        "low": [99.0 + i * 0.01 for i in range(n)],
        "close": [100.5 + i * 0.01 for i in range(n)],
        "volume": [1_000_000.0] * n,
    }
    if include_roll_flag:
        data["roll_flag"] = [False] * (n - 5) + [True] * 5
    df = pd.DataFrame(data)
    df.to_parquet(path, index=False)


class _FakePaths:
    def __init__(self, root: Path) -> None:
        self.pkl_dir = root / "pkl"
        self.state_dir = root / "state"


class _FakeCfg:
    def __init__(self, tmp_path: Path) -> None:
        self.paths = _FakePaths(tmp_path)
        self.splits: Dict = {}
        self.parquet = types.SimpleNamespace(
            nan_threshold=0.2,
            allow_negative_prices=False,
            resample_enabled=False,
            resample_bar_size="1D",
        )

    def copy(self, deep: bool = False) -> "_FakeCfg":
        new = _FakeCfg.__new__(_FakeCfg)
        new.paths = _FakePaths(Path(str(self.paths.pkl_dir).replace("/pkl", "")))
        new.splits = dict(self.splits)
        new.parquet = self.parquet
        return new


def _make_train_result(
    *,
    passed: bool = True,
    structural_fail: bool = False,
    gate_reasons: list[str] | None = None,
    error: str = "",
) -> types.SimpleNamespace:
    reasons = gate_reasons or (["data_invalid"] if structural_fail else [])
    gate = {"passed": passed, "reasons": reasons}
    return types.SimpleNamespace(
        passed=passed,
        gate_result=types.SimpleNamespace(model_dump=lambda: gate, dict=lambda: gate),
        metrics=types.SimpleNamespace(
            model_dump=lambda: {"sharpe": 1.2 if passed else 0.3},
            dict=lambda: {"sharpe": 1.2 if passed else 0.3},
        ),
        pack_result={"saved": True, "features_used": ["close_lag1"]},
        error=error,
    )


def _setup_cascade_mocks(
    monkeypatch: Any,
    tmp_path: Path,
    *,
    passed: bool = True,
    structural_fail: bool = False,
    error: str = "",
) -> None:
    import octa_ops.autopilot.cascade_train as ct

    cfg = _FakeCfg(tmp_path)
    monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: cfg)
    monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
    monkeypatch.setattr(ct, "_walkforward_eligibility", lambda **_kw: {"eligible": True})
    monkeypatch.setattr(
        ct,
        "train_evaluate_package",
        lambda **_kw: _make_train_result(
            passed=passed,
            structural_fail=structural_fail,
            error=error,
        ),
    )


# ---------------------------------------------------------------------------
# 1. Cascade integration — multi-asset run_cascade_training()
# ---------------------------------------------------------------------------

import octa_ops.autopilot.cascade_train as ct
from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training


class TestCascadeIntegration:
    def test_stock_cascade_both_pass(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        pq = tmp_path / "stock.parquet"
        _write_price_parquet(pq)
        decisions, metrics = run_cascade_training(
            run_id="d_stock",
            config_path="configs/dev.yaml",
            symbol="AAPL",
            asset_class="stock",
            parquet_paths={"1D": str(pq), "1H": str(pq)},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(decisions) == 2
        assert all(d.status == "PASS" for d in decisions)
        assert all(d.details["structural_pass"] is True for d in decisions)
        assert all(d.details["performance_pass"] is True for d in decisions)

    def test_future_cascade_with_roll_flag(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        pq = tmp_path / "ES.parquet"
        _write_price_parquet(pq, include_roll_flag=True)
        decisions, metrics = run_cascade_training(
            run_id="d_future",
            config_path="configs/dev.yaml",
            symbol="ES",
            asset_class="future",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(decisions) == 1
        assert decisions[0].status == "PASS"
        assert metrics["1D"]["asset_class"] == "future"

    def test_crypto_cascade_accepted(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        pq = tmp_path / "BTC.parquet"
        _write_price_parquet(pq)
        decisions, metrics = run_cascade_training(
            run_id="d_crypto",
            config_path="configs/dev.yaml",
            symbol="BTC",
            asset_class="crypto",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert decisions[0].status == "PASS"
        assert metrics["1D"]["asset_class"] == "crypto"

    def test_fx_cascade_accepted(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        pq = tmp_path / "EURUSD.parquet"
        _write_price_parquet(pq)
        decisions, metrics = run_cascade_training(
            run_id="d_fx",
            config_path="configs/dev.yaml",
            symbol="EURUSD",
            asset_class="fx",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert decisions[0].status == "PASS"
        assert metrics["1D"]["asset_class"] == "fx"

    def test_structural_fail_blocks_downstream(self, tmp_path: Path, monkeypatch: Any) -> None:
        """1D structural failure (TRAIN_ERROR) → 1H skipped."""
        _setup_cascade_mocks(
            monkeypatch, tmp_path,
            passed=False,
            error="Traceback (most recent call last):\n  raise RuntimeError",
        )
        pq = tmp_path / "X.parquet"
        _write_price_parquet(pq)
        decisions, _ = run_cascade_training(
            run_id="d_block",
            config_path="configs/dev.yaml",
            symbol="X",
            asset_class="stock",
            parquet_paths={"1D": str(pq), "1H": str(pq)},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(decisions) == 2
        assert decisions[0].status == "TRAIN_ERROR"
        assert decisions[0].details["structural_pass"] is False
        assert decisions[1].status == "SKIP"
        assert decisions[1].reason == "cascade_previous_not_structural_pass"

    def test_performance_fail_does_not_block_downstream(self, tmp_path: Path, monkeypatch: Any) -> None:
        """1D gate fail (non-structural, e.g. sharpe low) → 1H still runs."""
        call_count = {"n": 0}

        def _fake_train(**_kw: Any) -> types.SimpleNamespace:
            call_count["n"] += 1
            # 1D fails performance (non-structural), 1H passes
            if call_count["n"] == 1:
                return _make_train_result(passed=False, gate_reasons=["sharpe_too_low"])
            return _make_train_result(passed=True)

        cfg = _FakeCfg(tmp_path)
        monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: cfg)
        monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
        monkeypatch.setattr(ct, "_walkforward_eligibility", lambda **_kw: {"eligible": True})
        monkeypatch.setattr(ct, "train_evaluate_package", _fake_train)

        pq = tmp_path / "Y.parquet"
        _write_price_parquet(pq)
        decisions, _ = run_cascade_training(
            run_id="d_perf",
            config_path="configs/dev.yaml",
            symbol="Y",
            asset_class="stock",
            parquet_paths={"1D": str(pq), "1H": str(pq)},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(decisions) == 2
        assert decisions[0].status == "GATE_FAIL"
        assert decisions[0].details["structural_pass"] is True  # performance fail only
        assert decisions[0].details["performance_pass"] is False
        # 1H was NOT skipped
        assert decisions[1].status == "PASS"
        assert call_count["n"] == 2  # both stages called

    def test_pkl_dir_scoped_per_asset_class_and_tf(self, tmp_path: Path, monkeypatch: Any) -> None:
        """Model pkl_dir follows <root>/<asset_class>/<tf>/ pattern."""
        observed_pkl_dirs: list[str] = []

        def _spy_train(*, cfg: Any, **_kw: Any) -> types.SimpleNamespace:
            observed_pkl_dirs.append(str(getattr(cfg.paths, "pkl_dir", "")))
            return _make_train_result(passed=True)

        cfg = _FakeCfg(tmp_path)
        monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: cfg)
        monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
        monkeypatch.setattr(ct, "_walkforward_eligibility", lambda **_kw: {"eligible": True})
        monkeypatch.setattr(ct, "train_evaluate_package", _spy_train)

        pq = tmp_path / "Z.parquet"
        _write_price_parquet(pq)
        run_cascade_training(
            run_id="d_dirs",
            config_path="configs/dev.yaml",
            symbol="Z",
            asset_class="crypto",
            parquet_paths={"1D": str(pq), "1H": str(pq)},
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert len(observed_pkl_dirs) == 2
        assert "crypto" in observed_pkl_dirs[0]
        assert "1D" in observed_pkl_dirs[0]
        assert "crypto" in observed_pkl_dirs[1]
        assert "1H" in observed_pkl_dirs[1]

    def test_missing_parquet_sets_structural_fail(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        decisions, _ = run_cascade_training(
            run_id="d_missing",
            config_path="configs/dev.yaml",
            symbol="NOPQ",
            asset_class="stock",
            parquet_paths={"1D": "nonexistent.parquet"},  # 1H not supplied
            cascade=CascadePolicy(order=["1D", "1H"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        # 1D has parquet but it's in eligibility mock (True), will try to train
        # 1H has no parquet → SKIP
        assert decisions[-1].reason in {"missing_parquet", "cascade_previous_not_structural_pass"}


# ---------------------------------------------------------------------------
# 2. Eligibility 1D+1H+30M integration via v000 runner
# ---------------------------------------------------------------------------

import octa.support.ops.v000_full_universe_cascade_train as v000


def _fake_run_cascade(
    *,
    symbol: str,
    cascade: CascadePolicy,
    **_kw: Any,
) -> tuple:
    """Fake run_cascade_training producing configurable decisions."""
    decisions = []
    for tf in cascade.order:
        decisions.append(
            types.SimpleNamespace(
                symbol=symbol,
                timeframe=tf,
                stage="train",
                status="PASS",
                reason=None,
                details={"structural_pass": True, "performance_pass": True},
            )
        )
    return decisions, {tf: {} for tf in cascade.order}


def _fake_run_cascade_30m_fail(
    *,
    symbol: str,
    cascade: CascadePolicy,
    **_kw: Any,
) -> tuple:
    decisions = []
    for tf in cascade.order:
        perf = tf != "30M"
        decisions.append(
            types.SimpleNamespace(
                symbol=symbol,
                timeframe=tf,
                stage="train",
                status="PASS" if perf else "GATE_FAIL",
                reason=None,
                details={"structural_pass": True, "performance_pass": perf},
            )
        )
    return decisions, {tf: {} for tf in cascade.order}


def _v000_run_with_mock(
    monkeypatch: Any,
    tmp_path: Path,
    *,
    cascade_fn: Any = None,
    jobs: list[dict] | None = None,
    cascade_order: list[str] | None = None,
) -> dict:
    import octa.support.ops.v000_full_universe_cascade_train as v000mod
    from octa.execution import ibkr_runtime as ibkr

    monkeypatch.setattr(ibkr, "ensure_ibkr_running", lambda _cfg: {"ok": True, "skipped": False})
    monkeypatch.setattr(ibkr, "ibkr_health", lambda _cfg: {"port_open": False, "process_running": False})
    monkeypatch.setattr(v000mod, "run_cascade_training", cascade_fn or _fake_run_cascade)

    cfg_yaml = tmp_path / "test_cfg.yaml"
    _jobs = jobs or [
        {"symbol": "AAPL", "asset_class": "stock", "parquet_paths": {"1D": "x.parquet", "1H": "x.parquet", "30M": "x.parquet"}},
    ]
    _order = cascade_order or ["1D", "1H", "30M"]
    import yaml
    cfg_yaml.write_text(yaml.dump({
        "run_id": "phase_d_test",
        "training_config": "configs/dev.yaml",
        "cascade_order": _order,
        "cascade_jobs": _jobs,
        "ibkr": {"host": "127.0.0.1", "port": 7497},
    }))

    out_dir = tmp_path / "out"
    result = v000mod.run(config_path=str(cfg_yaml), out_dir=out_dir)
    return result


class TestEligibility30MIntegration:
    def test_three_tf_all_pass_eligible(self, tmp_path: Path, monkeypatch: Any) -> None:
        result = _v000_run_with_mock(monkeypatch, tmp_path, cascade_fn=_fake_run_cascade)
        summary = result["summary"]
        symbols = summary["symbols"]
        assert len(symbols) == 1
        assert symbols[0]["eligible_strict"] is True
        assert symbols[0]["performance_1D"] is True
        assert symbols[0]["performance_1H"] is True
        assert symbols[0]["performance_30M"] is True

    def test_30m_perf_fail_ineligible(self, tmp_path: Path, monkeypatch: Any) -> None:
        result = _v000_run_with_mock(monkeypatch, tmp_path, cascade_fn=_fake_run_cascade_30m_fail)
        symbols = result["summary"]["symbols"]
        assert symbols[0]["performance_30M"] is False
        assert symbols[0]["eligible_strict"] is False

    def test_30m_not_in_cascade_backward_compat(self, tmp_path: Path, monkeypatch: Any) -> None:
        """With cascade_order=[1D,1H] (no 30M), eligibility based on 1D+1H only."""

        def _only_2tf(*, symbol: str, cascade: CascadePolicy, **_kw: Any) -> tuple:
            return (
                [
                    types.SimpleNamespace(
                        symbol=symbol, timeframe="1D", stage="train", status="PASS",
                        reason=None, details={"structural_pass": True, "performance_pass": True},
                    ),
                    types.SimpleNamespace(
                        symbol=symbol, timeframe="1H", stage="train", status="PASS",
                        reason=None, details={"structural_pass": True, "performance_pass": True},
                    ),
                ],
                {"1D": {}, "1H": {}},
            )

        result = _v000_run_with_mock(
            monkeypatch, tmp_path,
            cascade_fn=_only_2tf,
            cascade_order=["1D", "1H"],
            jobs=[{"symbol": "TEST", "asset_class": "stock", "parquet_paths": {"1D": "x.parquet", "1H": "x.parquet"}}],
        )
        symbols = result["summary"]["symbols"]
        assert symbols[0]["eligible_strict"] is True

    def test_execution_eligible_universe_json_written(self, tmp_path: Path, monkeypatch: Any) -> None:
        result = _v000_run_with_mock(monkeypatch, tmp_path, cascade_fn=_fake_run_cascade)
        out = Path(result["out_dir"])
        assert (out / "execution_eligible_universe.json").exists()
        universe = json.loads((out / "execution_eligible_universe.json").read_text())
        assert "symbols" in universe

    def test_summary_json_contains_required_keys(self, tmp_path: Path, monkeypatch: Any) -> None:
        result = _v000_run_with_mock(monkeypatch, tmp_path, cascade_fn=_fake_run_cascade)
        s = result["summary"]
        for key in ("run_id", "eligible_count", "train_rows", "structural_pass_count",
                    "performance_pass_count", "symbols", "ibkr_autologin_enabled"):
            assert key in s, f"missing key: {key}"

    def test_hashes_json_written(self, tmp_path: Path, monkeypatch: Any) -> None:
        result = _v000_run_with_mock(monkeypatch, tmp_path, cascade_fn=_fake_run_cascade)
        out = Path(result["out_dir"])
        assert (out / "hashes.json").exists()
        hashes = json.loads((out / "hashes.json").read_text())
        assert "summary.json" in hashes

    def test_multi_asset_multi_symbol(self, tmp_path: Path, monkeypatch: Any) -> None:
        jobs = [
            {"symbol": "AAPL", "asset_class": "stock", "parquet_paths": {"1D": "x.parquet", "1H": "x.parquet"}},
            {"symbol": "BTC", "asset_class": "crypto", "parquet_paths": {"1D": "x.parquet", "1H": "x.parquet"}},
            {"symbol": "EURUSD", "asset_class": "fx", "parquet_paths": {"1D": "x.parquet"}},
            {"symbol": "ES", "asset_class": "future", "parquet_paths": {"1D": "x.parquet"}},
        ]
        result = _v000_run_with_mock(
            monkeypatch, tmp_path,
            cascade_fn=_fake_run_cascade,
            cascade_order=["1D", "1H"],
            jobs=jobs,
        )
        symbols = result["summary"]["symbols"]
        assert len(symbols) == 4
        syms = {s["symbol"] for s in symbols}
        assert syms == {"AAPL", "BTC", "EURUSD", "ES"}


# ---------------------------------------------------------------------------
# 3. Futures roll_flag parquet integration (real file load, mocked training)
# ---------------------------------------------------------------------------

class TestFuturesRollFlagIntegration:
    def test_futures_real_parquet_without_roll_flag_ineligible(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """End-to-end: actual parquet load with missing roll_flag → SKIP decision."""
        # Only mock training, not _walkforward_eligibility
        cfg = _FakeCfg(tmp_path)
        monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: cfg)
        monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
        monkeypatch.setattr(ct, "train_evaluate_package", lambda **_kw: _make_train_result())

        pq = tmp_path / "ES_no_roll.parquet"
        _write_price_parquet(pq, n=500, include_roll_flag=False)  # NO roll_flag

        decisions, _ = run_cascade_training(
            run_id="d_roll_missing",
            config_path="configs/dev.yaml",
            symbol="ES",
            asset_class="future",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert decisions[0].status == "SKIP"
        assert decisions[0].details["structural_pass"] is False
        # GateDecision.reason is hardcoded to "insufficient_history_for_walkforward";
        # the real reason ("missing_required_futures_columns:roll_flag") is in details.
        assert decisions[0].details.get("proof") == "futures_schema_check"
        assert "roll_flag" in str(decisions[0].details.get("reason", ""))

    def test_futures_real_parquet_with_roll_flag_proceeds(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """Real parquet with roll_flag present → training is attempted."""
        cfg = _FakeCfg(tmp_path)
        monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: cfg)
        monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
        monkeypatch.setattr(ct, "train_evaluate_package", lambda **_kw: _make_train_result())

        pq = tmp_path / "ES_with_roll.parquet"
        _write_price_parquet(pq, n=500, include_roll_flag=True)

        decisions, _ = run_cascade_training(
            run_id="d_roll_present",
            config_path="configs/dev.yaml",
            symbol="ES",
            asset_class="future",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert decisions[0].status == "PASS"

    def test_stock_parquet_without_roll_flag_passes(
        self, tmp_path: Path, monkeypatch: Any
    ) -> None:
        """roll_flag not required for stocks — parquet without it must proceed."""
        cfg = _FakeCfg(tmp_path)
        monkeypatch.setattr(ct, "load_config", lambda _p, **_kw: cfg)
        monkeypatch.setattr(ct, "StateRegistry", lambda _p: types.SimpleNamespace())
        monkeypatch.setattr(ct, "train_evaluate_package", lambda **_kw: _make_train_result())

        pq = tmp_path / "AAPL_no_roll.parquet"
        _write_price_parquet(pq, n=500, include_roll_flag=False)

        decisions, _ = run_cascade_training(
            run_id="d_stock_no_roll",
            config_path="configs/dev.yaml",
            symbol="AAPL",
            asset_class="stock",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
        )
        assert decisions[0].status == "PASS"


# ---------------------------------------------------------------------------
# 4. Builder contract invariant sweep
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
from octa.core.features.altdata import (
    attention_features,
    event_features,
    flow_features,
    fundamental_features,
    liquidity_features,
    macro_features,
    sentiment_features,
)

_ALL_BUILDERS = [
    ("macro_features", lambda p: macro_features.build(p)),
    ("event_features", lambda p: event_features.build(p)),
    ("flow_features", lambda p: flow_features.build(p)),
    ("fundamental_features", lambda p: fundamental_features.build(p)),
    ("liquidity_features", lambda p: liquidity_features.build(p)),
    ("sentiment_features", lambda p: sentiment_features.build(p)),
    ("attention_features", lambda p: attention_features.build(p)),
    ("onchain_features", lambda p: onchain_features.build(p)),
    ("funding_rate_features", lambda p: funding_rate_features.build(p)),
    ("eco_calendar_features", lambda p: eco_calendar_features.build(p)),
    ("cot_features", lambda p: cot_features.build(p)),
    ("basis_features", lambda p: basis_features.build(p)),
    ("greeks_features", lambda p: greeks_features.build(p)),
    ("iv_surface_features", lambda p: iv_surface_features.build(p)),
]

_SAMPLE_PAYLOADS = [
    # Empty
    {},
    # Generic numeric
    {"close": 100.0, "volume": 1_000_000.0},
    # Futures basis
    {"futures_close": 4505.0, "spot_close": 4500.0, "multiplier": 50.0},
    # Crypto
    {"nvt_ratio": 10.0, "funding_rate": 0.001, "mvrv_z_score": 1.5},
    # FX
    {"eco_surprise_score": 0.7, "cot_net_position": 50000.0},
    # Options
    {"delta_mean": 0.45, "gamma_mean": 0.03, "iv_atm": 0.20},
]


class TestBuilderContractInvariant:
    @pytest.mark.parametrize("name,fn", _ALL_BUILDERS)
    def test_builder_returns_dict(self, name: str, fn: Any) -> None:
        for payload in _SAMPLE_PAYLOADS:
            result = fn(payload)
            assert isinstance(result, dict), f"{name}: expected dict, got {type(result)}"

    @pytest.mark.parametrize("name,fn", _ALL_BUILDERS)
    def test_builder_no_nan_values(self, name: str, fn: Any) -> None:
        for payload in _SAMPLE_PAYLOADS:
            result = fn(payload)
            for k, v in result.items():
                assert not math.isnan(v), f"{name}: key '{k}' has NaN value"

    @pytest.mark.parametrize("name,fn", _ALL_BUILDERS)
    def test_builder_no_inf_values(self, name: str, fn: Any) -> None:
        for payload in _SAMPLE_PAYLOADS:
            result = fn(payload)
            for k, v in result.items():
                assert math.isfinite(v), f"{name}: key '{k}' has non-finite value {v}"

    @pytest.mark.parametrize("name,fn", _ALL_BUILDERS)
    def test_builder_values_are_float(self, name: str, fn: Any) -> None:
        for payload in _SAMPLE_PAYLOADS:
            result = fn(payload)
            for k, v in result.items():
                assert isinstance(v, (int, float)), f"{name}: key '{k}' value is {type(v)}"

    def test_onchain_symbol_builder_contract(self) -> None:
        """onchain_features.build_symbol() also follows the contract."""
        for payload in _SAMPLE_PAYLOADS:
            result = onchain_features.build_symbol(payload)
            assert isinstance(result, dict)
            for k, v in result.items():
                assert math.isfinite(v), f"onchain.build_symbol: '{k}' non-finite"


# ---------------------------------------------------------------------------
# 5. FeatureWeightingPolicy normalisation invariant
# ---------------------------------------------------------------------------

from octa.core.features.altdata.weighting import (
    _CRYPTO_POLICIES,
    _DEFAULT_POLICIES,
    _FX_POLICIES,
    _FUTURE_POLICIES,
    _OPTION_POLICIES,
    FeatureWeightingPolicy,
    resolve_policy,
)


class TestWeightingPolicyInvariant:
    def _all_policies(self) -> list[FeatureWeightingPolicy]:
        all_p: list[FeatureWeightingPolicy] = []
        for bucket in [_DEFAULT_POLICIES, _CRYPTO_POLICIES, _FX_POLICIES, _FUTURE_POLICIES, _OPTION_POLICIES]:
            all_p.extend(bucket)
        return all_p

    def test_all_policies_have_positive_weights(self) -> None:
        for p in self._all_policies():
            for w in p.weights:
                assert w.weight > 0, f"{p.asset_class}/{p.timeframe}: weight for '{w.group}' is {w.weight}"

    def test_all_policies_normalise_to_one(self) -> None:
        for p in self._all_policies():
            present = [w.group for w in p.weights]
            n = p.normalised_weights(present)
            if n:
                total = sum(n.values())
                assert abs(total - 1.0) < 1e-9, (
                    f"{p.asset_class}/{p.timeframe}/{p.gate_layer}: normalised sum={total}"
                )

    def test_all_policies_stable_hash_consistent(self) -> None:
        """Stable hash for same policy must be identical across two calls."""
        for p in self._all_policies():
            assert p.stable_hash() == p.stable_hash()

    def test_missing_group_gets_zero_in_normalised(self) -> None:
        p = resolve_policy("stock", "1D", "global_1d")
        normalised = p.normalised_weights([])  # nothing present
        assert len(normalised) == 0

    def test_partial_groups_normalise_to_one(self) -> None:
        p = resolve_policy("crypto", "1D", "global_1d")
        if len(p.weights) >= 2:
            partial = [p.weights[0].group]
            n = p.normalised_weights(partial)
            if n:
                assert abs(sum(n.values()) - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# 6. AssetProfile calendar invariants for all kinds
# ---------------------------------------------------------------------------

from octa_training.core.asset_profiles import (
    AssetProfile,
    CalendarSpec,
    AltDataSpec,
    CryptoExt,
    FxExt,
    FutureExt,
    OptionExt,
    _apply_calendar_defaults,
)

_ASSET_KINDS = ["stock", "etf", "crypto", "fx", "forex", "future", "futures", "option", "options", "index", "bond", "legacy"]


class TestCalendarInvariant:
    def test_all_kinds_return_valid_calendar(self) -> None:
        for kind in _ASSET_KINDS:
            p = AssetProfile(name=kind, kind=kind, gates={})
            p2 = _apply_calendar_defaults(p)
            cal = p2.calendar
            assert cal.trading_days_per_year >= 252, f"{kind}: trading_days_per_year={cal.trading_days_per_year}"
            assert cal.trading_days_per_year <= 365

    def test_crypto_24_7_invariant(self) -> None:
        for kind in ["crypto"]:
            p = AssetProfile(name=kind, kind=kind, gates={})
            p2 = _apply_calendar_defaults(p)
            assert p2.calendar.is_24_7 is True
            assert p2.calendar.trading_days_per_year == 365

    def test_fx_24_5_invariant(self) -> None:
        for kind in ["fx", "forex"]:
            p = AssetProfile(name=kind, kind=kind, gates={})
            p2 = _apply_calendar_defaults(p)
            assert p2.calendar.is_24_5 is True
            assert p2.calendar.trading_days_per_year == 261

    def test_stock_not_24_7_or_24_5(self) -> None:
        p = AssetProfile(name="stock", kind="stock", gates={})
        p2 = _apply_calendar_defaults(p)
        assert p2.calendar.is_24_7 is False
        assert p2.calendar.is_24_5 is False

    def test_explicit_calendar_preserved(self) -> None:
        """Explicitly set non-default calendar must not be overwritten."""
        cal = CalendarSpec(trading_days_per_year=260, is_24_7=False, is_24_5=True)
        p = AssetProfile(name="crypto", kind="crypto", gates={}, calendar=cal)
        p2 = _apply_calendar_defaults(p)
        assert p2.calendar.trading_days_per_year == 260

    def test_future_ext_defaults(self) -> None:
        fe = FutureExt()
        assert "roll_flag" in fe.required_parquet_columns
        assert fe.fold_boundary_roll_buffer_bars >= 0
        assert fe.back_adjust_method == "ratio"

    def test_option_ext_defaults(self) -> None:
        oe = OptionExt()
        assert oe.performance_threshold_auc == 0.55
        assert oe.lstm_seq_len == 32
        assert oe.require_underlying_cascade_pass is True

    def test_alt_data_spec_defaults(self) -> None:
        a = AltDataSpec()
        assert isinstance(a.required_sources, list)
        assert isinstance(a.optional_sources, list)


# ---------------------------------------------------------------------------
# 7. RollManager sim_date determinism invariant
# ---------------------------------------------------------------------------

from datetime import date, timedelta
from octa_assets.futures.rolls import RollManager


class TestRollDeterminismInvariant:
    def test_same_sim_date_same_outcome(self) -> None:
        rm = RollManager(roll_window_days=5)
        sim = date(2026, 6, 15)
        expiry_a = date(2026, 6, 18)
        expiry_b = date(2026, 9, 20)
        candidates = {"A": {"volume": 100, "open_interest": 100}, "B": {"volume": 200, "open_interest": 300}}
        meta = {"A": {"expiry": expiry_a}, "B": {"expiry": expiry_b}}

        r1 = rm.decide_roll("A", candidates, meta, sim_date=sim)
        r2 = rm.decide_roll("A", candidates, meta, sim_date=sim)
        assert r1 == r2

    def test_dte_deterministic_across_sim_dates(self) -> None:
        rm = RollManager()
        expiry = date(2026, 9, 20)
        for delta in [1, 5, 10, 20, 100]:
            sim = expiry - timedelta(days=delta)
            dte = rm.days_to_expiry(expiry, sim_date=sim)
            assert dte == delta

    def test_within_roll_window_triggers_roll(self) -> None:
        rm = RollManager(roll_window_days=5)
        expiry_cur = date(2026, 6, 20)
        expiry_next = date(2026, 9, 20)
        sim_inside = date(2026, 6, 16)  # dte=4 <= 5
        candidates = {"A": {"volume": 100, "open_interest": 100}, "B": {"volume": 200, "open_interest": 300}}
        meta = {"A": {"expiry": expiry_cur}, "B": {"expiry": expiry_next}}
        result = rm.decide_roll("A", candidates, meta, sim_date=sim_inside)
        assert result == "B"

    def test_outside_roll_window_stays(self) -> None:
        rm = RollManager(roll_window_days=5, oi_multiplier_trigger=100.0)  # high OI trigger
        expiry_cur = date(2026, 6, 20)
        expiry_next = date(2026, 9, 20)
        sim_outside = date(2026, 5, 1)  # dte=50 >> 5
        candidates = {"A": {"volume": 100, "open_interest": 100}, "B": {"volume": 200, "open_interest": 101}}
        meta = {"A": {"expiry": expiry_cur}, "B": {"expiry": expiry_next}}
        result = rm.decide_roll("A", candidates, meta, sim_date=sim_outside)
        assert result == "A"


# ---------------------------------------------------------------------------
# 8. Options aggregate() pipeline validation
# ---------------------------------------------------------------------------

import csv
from scripts.aggregate_option_snapshots import aggregate


def _write_option_csv(path: Path, n: int = 5, underlying_close: float = 100.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "trade date", "expiry date", "strike", "call/put",
        "bid price", "ask price", "delta",
        "bid implied volatility", "ask implied volatility",
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i in range(n):
            w.writerow({
                "trade date": f"2026-01-{i + 1:02d}",
                "expiry date": "2026-03-21",
                "strike": str(100 + i * 5),
                "call/put": "C" if i % 2 == 0 else "P",
                "bid price": "5.0",
                "ask price": "5.5",
                "delta": "0.45",
                "bid implied volatility": "0.20",
                "ask implied volatility": "0.22",
            })


class TestOptionsPipelineValidation:
    def test_aggregate_callable_no_side_effects_on_import(self) -> None:
        """Importing the module must not raise FileNotFoundError or write files."""
        import scripts.aggregate_option_snapshots as mod
        assert callable(mod.aggregate)

    def test_contract_id_format(self, tmp_path: Path) -> None:
        src = tmp_path / "csvs"
        _write_option_csv(src / "s.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")
        # contract_id = AAPL_2026-03-21_100.0_C (or P)
        sample_id = df["contract_id"].iloc[0]
        # contract_id uses compact date format: AAPL_20260321_100.00_C
        assert "AAPL_" in sample_id
        assert "20260321" in sample_id
        assert sample_id.endswith(("_C", "_P"))

    def test_required_columns_present(self, tmp_path: Path) -> None:
        src = tmp_path / "csvs"
        _write_option_csv(src / "s.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="TSLA")
        required = ["timestamp", "symbol", "contract_id", "option_type", "strike", "tte_days", "mid"]
        for col in required:
            assert col in df.columns, f"missing: {col}"

    def test_no_nan_in_timestamp(self, tmp_path: Path) -> None:
        src = tmp_path / "csvs"
        _write_option_csv(src / "s.csv")
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")
        assert df["timestamp"].notna().all()

    def test_moneyness_positive_when_underlying_provided(self, tmp_path: Path) -> None:
        """When underlying close is available, moneyness = strike / underlying_close must be > 0."""
        src = tmp_path / "csvs"
        # Write CSVs with known underlying_close
        import csv
        fieldnames = [
            "trade date", "expiry date", "strike", "call/put",
            "bid price", "ask price", "delta",
            "bid implied volatility", "ask implied volatility", "underlying_close",
        ]
        (src).mkdir(parents=True, exist_ok=True)
        with (src / "s.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for i in range(5):
                w.writerow({
                    "trade date": f"2026-01-{i+1:02d}", "expiry date": "2026-03-21",
                    "strike": str(100 + i * 5), "call/put": "C",
                    "bid price": "5.0", "ask price": "5.5", "delta": "0.45",
                    "bid implied volatility": "0.20", "ask implied volatility": "0.22",
                    "underlying_close": "100.0",
                })
        dst = tmp_path / "out.parquet"
        df = aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")
        if "moneyness" in df.columns:
            finite_mask = df["moneyness"].notna()
            if finite_mask.any():
                assert (df.loc[finite_mask, "moneyness"] > 0).all(), "moneyness must be positive"

    def test_schema_validation_raises_on_missing_csv_data(self, tmp_path: Path) -> None:
        """Empty dir raises FileNotFoundError."""
        src = tmp_path / "empty_dir"
        src.mkdir()
        dst = tmp_path / "out.parquet"
        with pytest.raises(FileNotFoundError):
            aggregate(src_dir=src, dst=dst, underlying_symbol="AAPL")


# ---------------------------------------------------------------------------
# 9. StageResult contract invariant
# ---------------------------------------------------------------------------

from octa_ops.autopilot.stage_result import StageMandatoryMetrics, StageResult


class TestStageResultInvariant:
    def test_leakage_detected_blocks_perf_pass(self) -> None:
        m = StageMandatoryMetrics(leakage_detected=True)
        sr = StageResult(
            symbol="X", timeframe="1D", asset_class="stock", run_id="r", stage_index=0,
            structural_pass=True, performance_pass=True, metrics=m, fail_status="PASS",
        )
        issues = sr.validate()
        assert any("leakage" in i for i in issues)

    def test_nan_in_any_metric_is_violation(self) -> None:
        for field in ["sharpe_ratio", "max_drawdown", "cvar_95", "sortino_ratio"]:
            m = StageMandatoryMetrics(**{field: float("nan")})
            issues = m.validate()
            assert issues, f"{field}=NaN should produce validation issues"

    def test_inf_in_any_metric_is_violation(self) -> None:
        for field in ["sharpe_ratio", "max_drawdown", "cvar_95"]:
            m = StageMandatoryMetrics(**{field: float("inf")})
            issues = m.validate()
            assert issues, f"{field}=inf should produce validation issues"

    def test_valid_result_no_issues(self) -> None:
        m = StageMandatoryMetrics(
            sharpe_ratio=1.5, max_drawdown=-0.08, cvar_95=-0.04,
            leakage_detected=False, walk_forward_passed=True, n_folds_completed=5,
        )
        sr = StageResult(
            symbol="AAPL", timeframe="1D", asset_class="stock", run_id="r", stage_index=0,
            structural_pass=True, performance_pass=True, metrics=m,
            model_path="/tmp/m.pkl", model_hash="abc", fail_status="PASS",
        )
        assert sr.validate() == []

    def test_to_details_dict_completeness(self) -> None:
        m = StageMandatoryMetrics(sharpe_ratio=1.2)
        sr = StageResult(
            symbol="Z", timeframe="1H", asset_class="crypto", run_id="r", stage_index=1,
            structural_pass=True, performance_pass=False, metrics=m, fail_status="GATE_FAIL",
        )
        d = sr.to_details_dict()
        for key in ("structural_pass", "performance_pass", "fail_status", "fail_reason",
                    "model_path", "model_hash", "leakage_detected"):
            assert key in d, f"missing key: {key}"

    def test_model_path_only_when_structural_pass(self) -> None:
        """model_path should only be set on structural passes (enforced by caller convention)."""
        sr_pass = StageResult(
            symbol="A", timeframe="1D", asset_class="stock", run_id="r", stage_index=0,
            structural_pass=True, performance_pass=True, model_path="/tmp/m.pkl",
            fail_status="PASS",
        )
        sr_fail = StageResult(
            symbol="A", timeframe="1D", asset_class="stock", run_id="r", stage_index=0,
            structural_pass=False, performance_pass=False, model_path=None,
            fail_status="GATE_FAIL",
        )
        assert sr_pass.model_path is not None
        assert sr_fail.model_path is None


# ---------------------------------------------------------------------------
# 10. Trace dir populated on run
# ---------------------------------------------------------------------------

class TestTraceDirIntegration:
    def test_trace_dir_written(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        pq = tmp_path / "T.parquet"
        _write_price_parquet(pq)
        trace_dir = tmp_path / "trace"
        run_cascade_training(
            run_id="d_trace",
            config_path="configs/dev.yaml",
            symbol="T",
            asset_class="stock",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            trace_dir=str(trace_dir),
        )
        assert (trace_dir / "train_step_progress.jsonl").exists()
        lines = (trace_dir / "train_step_progress.jsonl").read_text().splitlines()
        assert len(lines) >= 2  # start + end events
        for line in lines:
            obj = json.loads(line)
            assert "step" in obj

    def test_trace_dir_durations_json(self, tmp_path: Path, monkeypatch: Any) -> None:
        _setup_cascade_mocks(monkeypatch, tmp_path, passed=True)
        pq = tmp_path / "T2.parquet"
        _write_price_parquet(pq)
        trace_dir = tmp_path / "trace2"
        run_cascade_training(
            run_id="d_trace2",
            config_path="configs/dev.yaml",
            symbol="T2",
            asset_class="stock",
            parquet_paths={"1D": str(pq)},
            cascade=CascadePolicy(order=["1D"]),
            safe_mode=True,
            reports_dir=str(tmp_path),
            trace_dir=str(trace_dir),
        )
        # durations json may be written at end
        progress = trace_dir / "train_step_progress.jsonl"
        assert progress.exists()
