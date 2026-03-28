from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from octa.core.pipeline.shadow_runner import run_shadow_pipeline
from octa.core.shadow.shadow_engine import run_shadow_trading
from octa.core.shadow.shadow_validation import validate_shadow_run
from octa.research.export.vectorbt_export import export_strategy_outputs


def _signals() -> pd.DataFrame:
    index = pd.date_range("2026-02-01", periods=8, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "long_signal": [0, 1, 1, 0, 0, 1, 0, 0],
            "short_signal": [0, 0, 0, 1, 0, 0, 0, 0],
            "signal_strength": [0.0, 1.0, 1.0, 0.8, 0.0, 0.7, 0.0, 0.0],
        },
        index=index,
    )


def _prices() -> pd.DataFrame:
    index = pd.date_range("2026-02-01", periods=8, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 100.5, 99.5, 101.5, 103.0, 104.0],
            "close": [100.5, 102.0, 100.0, 99.0, 100.0, 103.0, 104.5, 105.0],
        },
        index=index,
    )


def _config(**overrides: float | bool | str) -> dict[str, float | bool | str]:
    config: dict[str, float | bool | str] = {
        "position_size": 0.5,
        "fee": 0.001,
        "slippage": 0.0005,
        "capital": 100000.0,
        "max_drawdown_limit": 0.25,
        "max_position_size": 0.5,
        "allow_short": True,
        "max_equity_jump": 1.0,
    }
    config.update(overrides)
    return config


def _export_for_runner(tmp_path: Path) -> Path:
    export_dir = tmp_path / "research_export"
    index = pd.date_range("2026-02-01", periods=8, freq="D", tz="UTC")
    raw_signals = pd.DataFrame(
        {
            "signal": [1.0, 1.0, 0.0, -1.0, 0.0, 1.0, 0.0, 0.0],
            "signal_strength": [1.0, 1.0, 0.0, 0.8, 0.0, 0.7, 0.0, 0.0],
        },
        index=index,
    )
    returns = pd.DataFrame({"strategy_return": [0.01, 0.005, -0.01, -0.015, 0.005, 0.01, 0.006, 0.004]}, index=index)
    metadata = {
        "strategy_name": "shadow_test",
        "timeframe": "1D",
        "params": {"window": 10},
        "source": "unit_test",
    }
    export_strategy_outputs(raw_signals, returns, metadata, export_dir)
    return export_dir


def test_run_shadow_trading_is_deterministic() -> None:
    result_one = run_shadow_trading(_signals(), _prices(), _config())
    result_two = run_shadow_trading(_signals(), _prices(), _config())

    pdt.assert_frame_equal(result_one["trades"], result_two["trades"])
    pdt.assert_frame_equal(result_one["equity_curve"], result_two["equity_curve"])
    assert result_one["metrics"] == result_two["metrics"]


def test_same_inputs_yield_same_pipeline_outputs(tmp_path: Path) -> None:
    export_dir = _export_for_runner(tmp_path)
    evidence_root = tmp_path / "evidence"
    prices = _prices()

    first = run_shadow_pipeline(
        research_export_path=export_dir,
        prices_df=prices,
        config=_config(run_id="shadow_run_one"),
        evidence_root=evidence_root,
    )
    second = run_shadow_pipeline(
        research_export_path=export_dir,
        prices_df=prices,
        config=_config(run_id="shadow_run_two"),
        evidence_root=evidence_root,
    )

    pdt.assert_frame_equal(first["trades"], second["trades"])
    pdt.assert_frame_equal(first["equity_curve"], second["equity_curve"])
    assert first["metrics"] == second["metrics"]

    manifest_one = json.loads((first["evidence_dir"] / "run_manifest.json").read_text(encoding="utf-8"))
    manifest_two = json.loads((second["evidence_dir"] / "run_manifest.json").read_text(encoding="utf-8"))
    for name in (
        "trades.parquet",
        "equity_curve.parquet",
        "metrics.json",
        "sample_trades.txt",
        "sample_equity_head.txt",
    ):
        assert manifest_one["hashes"][name] == manifest_two["hashes"][name]


def test_manipulation_fails_validation() -> None:
    result = run_shadow_trading(_signals(), _prices(), _config())
    tampered_equity = result["equity_curve"].copy()
    tampered_equity.iloc[3, tampered_equity.columns.get_loc("equity")] = float("nan")

    with pytest.raises(ValueError, match="contains NaNs"):
        validate_shadow_run(result["trades"], tampered_equity)


def test_risk_kill_switch_stops_trading() -> None:
    crashing_prices = _prices().copy()
    crashing_prices.loc[:, "close"] = [100.5, 80.0, 60.0, 55.0, 54.0, 53.0, 52.0, 51.0]
    result = run_shadow_trading(
        _signals(),
        crashing_prices,
        _config(max_drawdown_limit=0.05),
    )

    assert result["metrics"]["kill_switch_triggered"] is True
    assert bool(result["equity_curve"]["kill_switch"].max()) is True
