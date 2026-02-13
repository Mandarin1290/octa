from __future__ import annotations

import json
from pathlib import Path

from octa.execution.evidence_selection import build_ml_selection


def _write_result(path: Path, symbol: str, stages: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"symbol": symbol, "asset_class": "equities", "stages": stages}
    path.write_text(json.dumps(payload), encoding="utf-8")


def _stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def test_selection_entry_and_scaling(tmp_path: Path) -> None:
    base = tmp_path / "evidence"
    run = base / "run_a"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")

    _write_result(
        run / "results" / "AAA.json",
        "AAA",
        [
            _stage("1D", "PASS", {"n_trades": 1}),
            _stage("1H", "PASS", {"n_trades": 1}),
            _stage("30M", "PASS", {"n_trades": 1}),
            _stage("5M", "PASS", {"n_trades": 1}),
            _stage("1M", "PASS", {"n_trades": 1}),
        ],
    )
    _write_result(
        run / "results" / "BBB.json",
        "BBB",
        [_stage("1D", "PASS", {"n_trades": 1}), _stage("1H", "SKIP", {"n_trades": 1})],
    )

    out = build_ml_selection(evidence_out_dir=tmp_path / "exec", base_evidence_dir=base)
    elig = out["eligible_rows"]
    assert len(elig) == 1
    assert elig[0]["symbol"] == "AAA"
    assert elig[0]["scaling_level"] == 3

    txt = (tmp_path / "exec" / "selection" / "eligible_symbols.txt").read_text(encoding="utf-8")
    assert txt.strip() == "AAA"


def test_selection_fail_closed_on_missing_metrics(tmp_path: Path) -> None:
    base = tmp_path / "evidence"
    run = base / "run_b"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    _write_result(
        run / "results" / "CCC.json",
        "CCC",
        [_stage("1D", "PASS", {}), _stage("1H", "PASS", {"n_trades": 1})],
    )
    out = build_ml_selection(evidence_out_dir=tmp_path / "exec", base_evidence_dir=base)
    assert out["eligible_rows"] == []
