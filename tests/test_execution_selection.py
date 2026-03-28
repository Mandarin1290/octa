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


def test_selection_skips_latest_preflight_only_run(tmp_path: Path) -> None:
    base = tmp_path / "evidence"

    incomplete = base / "run_newest"
    (incomplete / "preflight").mkdir(parents=True, exist_ok=True)
    (incomplete / "preflight" / "summary.json").write_text("{}", encoding="utf-8")

    run = base / "run_with_results"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    _write_result(
        run / "results" / "AAA.json",
        "AAA",
        [_stage("1D", "PASS", {"n_trades": 1}), _stage("1H", "PASS", {"n_trades": 1})],
    )

    out = build_ml_selection(evidence_out_dir=tmp_path / "exec", base_evidence_dir=base)
    assert [row["symbol"] for row in out["eligible_rows"]] == ["AAA"]


def test_selection_skips_foundation_validation_run(tmp_path: Path) -> None:
    base = tmp_path / "evidence"

    blocked = base / "run_validation"
    (blocked / "preflight").mkdir(parents=True, exist_ok=True)
    (blocked / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    (blocked / "run_manifest.json").write_text(
        json.dumps({"training_regime": "foundation_validation"}),
        encoding="utf-8",
    )
    _write_result(
        blocked / "results" / "AAA.json",
        "AAA",
        [_stage("1D", "PASS", {"n_trades": 1}), _stage("1H", "PASS", {"n_trades": 1})],
    )

    allowed = base / "run_prod"
    (allowed / "preflight").mkdir(parents=True, exist_ok=True)
    (allowed / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    (allowed / "run_manifest.json").write_text(
        json.dumps({"training_regime": "institutional_production"}),
        encoding="utf-8",
    )
    _write_result(
        allowed / "results" / "BBB.json",
        "BBB",
        [_stage("1D", "PASS", {"n_trades": 1}), _stage("1H", "PASS", {"n_trades": 1})],
    )

    out = build_ml_selection(evidence_out_dir=tmp_path / "exec", base_evidence_dir=base)
    assert [row["symbol"] for row in out["eligible_rows"]] == ["BBB"]
