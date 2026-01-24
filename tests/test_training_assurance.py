from __future__ import annotations

from pathlib import Path

import pandas as pd

from octa_training.core.assurance import emit_assurance_report
from octa_training.core.config import PathsConfig, TrainingConfig
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry


def _write_min_parquet(path: Path, n: int = 600) -> None:
    # Must satisfy io_parquet.load_parquet requirements:
    # - time column
    # - close present (and price cols positive)
    # - high >= max(open,close), low <= min(open,close)
    ts = pd.date_range("2020-01-01", periods=n, freq="h", tz="UTC")
    close = pd.Series(range(1, n + 1), dtype=float)
    open_ = close.shift(1).fillna(close.iloc[0])
    high = pd.concat([open_, close], axis=1).max(axis=1) + 0.1
    low = pd.concat([open_, close], axis=1).min(axis=1) - 0.1
    vol = pd.Series(1000.0, index=ts)

    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": vol.values,
        }
    )
    df.to_parquet(path, index=False)


def test_emit_assurance_report_writes_json(tmp_path: Path):
    paths = PathsConfig(
        raw_dir=tmp_path / "raw",
        pkl_dir=tmp_path / "pkl",
        logs_dir=tmp_path / "logs",
        state_dir=tmp_path / "state",
        reports_dir=tmp_path / "reports",
    )
    for d in (paths.raw_dir, paths.pkl_dir, paths.logs_dir, paths.state_dir, paths.reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    cfg = TrainingConfig(paths=paths)
    out = emit_assurance_report(
        cfg=cfg,
        symbol="TEST",
        run_id="r1",
        passed=False,
        reasons=["unit_test"],
        safe_mode=True,
        asset_class="stock",
        parquet_path=str(tmp_path / "raw" / "TEST.parquet"),
        parquet_stat={"size": 123},
        metrics_summary={"foo": "bar"},
        pack_result={"saved": False},
    )

    assert out.get("enabled") is True
    rp = out.get("report_path")
    assert rp
    p = Path(rp)
    assert p.exists()

    data = p.read_text(encoding="utf-8")
    assert "snapshot_id" in data
    assert out.get("snapshot_id") and len(out["snapshot_id"]) == 64


def test_pipeline_calls_assurance_hook(tmp_path: Path, monkeypatch):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    p = raw_dir / "TEST.parquet"
    _write_min_parquet(p, n=600)

    paths = PathsConfig(
        raw_dir=raw_dir,
        pkl_dir=tmp_path / "pkl",
        logs_dir=tmp_path / "logs",
        state_dir=tmp_path / "state",
        reports_dir=tmp_path / "reports",
    )
    for d in (paths.pkl_dir, paths.logs_dir, paths.state_dir, paths.reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    cfg = TrainingConfig(paths=paths)

    # make training light and deterministic
    cfg.models_order = ["ridge"]
    cfg.splits = {
        "n_folds": 2,
        "train_window": 200,
        "test_window": 50,
        "step": 50,
        "purge_size": 0,
        "embargo_size": 0,
        "min_train_size": 100,
        "min_test_size": 20,
        "min_folds_required": 1,
        "expanding": True,
    }
    cfg.assurance.enabled = True

    called = {"n": 0}

    import octa_training.core.assurance as assurance_mod

    def _stub_emit_assurance_report(**kwargs):
        called["n"] += 1
        return {"enabled": True, "report_path": str(tmp_path / "reports" / "stub.json"), "snapshot_id": "0" * 64}

    monkeypatch.setattr(assurance_mod, "emit_assurance_report", _stub_emit_assurance_report)

    state = StateRegistry(str(paths.state_dir))
    res = train_evaluate_package("TEST", cfg, state, run_id="r2", safe_mode=True, smoke_test=False)

    assert res.run_id == "r2"
    assert called["n"] >= 1
