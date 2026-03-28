from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class DummyGate:
    passed: bool
    gate_version: str = "hf_gate_2026-01-03_v1"
    reasons: list[str] | None = None


@dataclass
class DummyRes:
    passed: bool
    error: object | None = None
    gate_result: DummyGate | None = None


@dataclass
class DummyPInfo:
    symbol: str
    path: Path


def _mk_cfg(tmp_path, monkeypatch):
    # Use the project loader to build a full TrainingConfig, then override paths.
    from octa_training.core.config import load_config

    cfg = load_config(None)

    # cfg is a pydantic object; patch via model_dump deep merge is overkill here.
    # We only need paths for this test.
    cfg.paths.raw_dir = str(tmp_path / "raw")
    cfg.paths.reports_dir = str(tmp_path / "reports")
    cfg.paths.state_dir = str(tmp_path / "state")

    # Ensure reports dir exists.
    Path(cfg.paths.reports_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.paths.raw_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.paths.state_dir).mkdir(parents=True, exist_ok=True)

    # Disk checks must not block tests.
    import scripts.train_multiframe_symbol as mod

    monkeypatch.setattr(mod, "ensure_disk_space", lambda *a, **k: True)
    # Safety lock must not block unit tests (we're not exercising manifest/ARM logic here).
    monkeypatch.setattr(mod, "assert_training_armed", lambda *a, **k: None)
    return cfg


def test_cascade_stop_after_1d_fail(monkeypatch, tmp_path):
    import scripts.train_multiframe_symbol as mod
    from octa_training.core.state import StateRegistry

    cfg = _mk_cfg(tmp_path, monkeypatch)
    state = StateRegistry(cfg.paths.state_dir)

    # Make discovery return a daily parquet for the base symbol.
    dummy_path = tmp_path / "raw" / "SYM.parquet"
    monkeypatch.setattr(mod, "discover_parquets", lambda *a, **k: [DummyPInfo("SYM", dummy_path)])
    monkeypatch.setattr(mod, "find_symbol_variant", lambda discovered, base_symbol, suffix: "SYM")
    monkeypatch.setattr(mod, "inspect_parquet", lambda *a, **k: {"columns": ["open", "high", "low", "close"]})
    monkeypatch.setattr(mod, "load_parquet", lambda *a, **k: pd.DataFrame({"close": list(range(2000))}))

    calls: list[str] = []

    def fake_train(sym, cfg_layer, state, run_id, safe_mode=False, smoke_test=False, parquet_path=None, **kwargs):
        calls.append(sym)
        return DummyRes(
            passed=False,
            error=None,
            gate_result=DummyGate(passed=False, reasons=["folds pass ratio too low"]),
        )

    monkeypatch.setattr(mod, "train_evaluate_adaptive", fake_train)

    mod.run_sequence("SYM", cfg, state, run_id="t1", mode="live", config_raw={})

    assert calls == ["SYM"], "Only 1D should be trained when 1D fails"
    run_dir = Path(cfg.paths.reports_dir) / "cascade" / "t1"
    assert (run_dir / "SYM" / "1D" / "decision.json").exists()
    assert not (run_dir / "SYM" / "1H" / "decision.json").exists()


def test_cascade_stop_after_1h_fail(monkeypatch, tmp_path):
    import scripts.train_multiframe_symbol as mod
    from octa_training.core.state import StateRegistry

    cfg = _mk_cfg(tmp_path, monkeypatch)
    state = StateRegistry(cfg.paths.state_dir)

    dummy_path = tmp_path / "raw" / "SYM.parquet"
    monkeypatch.setattr(mod, "discover_parquets", lambda *a, **k: [DummyPInfo("SYM", dummy_path)])
    monkeypatch.setattr(mod, "find_symbol_variant", lambda discovered, base_symbol, suffix: "SYM")
    monkeypatch.setattr(mod, "inspect_parquet", lambda *a, **k: {"columns": ["open", "high", "low", "close"]})
    monkeypatch.setattr(mod, "load_parquet", lambda *a, **k: pd.DataFrame({"close": list(range(2000))}))

    calls: list[str] = []

    def fake_train(sym, cfg_layer, state, run_id, safe_mode=False, smoke_test=False, parquet_path=None, **kwargs):
        calls.append(sym)
        # 1D passes, 1H fails.
        if len(calls) == 1:
            return DummyRes(passed=True, error=None, gate_result=DummyGate(passed=True, reasons=[]))
        return DummyRes(passed=False, error=None, gate_result=DummyGate(passed=False, reasons=["subwindow_stability_failed"]))

    monkeypatch.setattr(mod, "train_evaluate_adaptive", fake_train)

    mod.run_sequence("SYM", cfg, state, run_id="t2", mode="live", config_raw={})

    assert calls == ["SYM", "SYM"], "Only 1D and 1H should be trained when 1H fails"
    run_dir = Path(cfg.paths.reports_dir) / "cascade" / "t2"
    assert (run_dir / "SYM" / "1D" / "decision.json").exists()
    assert (run_dir / "SYM" / "1H" / "decision.json").exists()
    assert not (run_dir / "SYM" / "30m" / "decision.json").exists()
