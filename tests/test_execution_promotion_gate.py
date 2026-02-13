from __future__ import annotations

import json
from pathlib import Path

from octa.execution.evidence_selection import build_ml_selection


def _run_with_stages(tmp_path: Path, stages: list[dict]) -> list[dict]:
    base = tmp_path / "evidence"
    run = base / "run_gate"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    (run / "results").mkdir(parents=True, exist_ok=True)
    (run / "results" / "SYM.json").write_text(
        json.dumps({"symbol": "SYM", "asset_class": "equities", "stages": stages}),
        encoding="utf-8",
    )
    out = build_ml_selection(evidence_out_dir=tmp_path / "exec", base_evidence_dir=base)
    return out["eligible_rows"]


def test_entry_requires_1d_and_1h_pass_exact() -> None:
    rows = _run_with_stages(
        Path(__import__("tempfile").mkdtemp()),
        [
            {"timeframe": "1D", "status": "PASS", "metrics_summary": {"n_trades": 1}},
            {"timeframe": "1H", "status": "PASS", "metrics_summary": {"n_trades": 1}},
        ],
    )
    assert len(rows) == 1

    rows2 = _run_with_stages(
        Path(__import__("tempfile").mkdtemp()),
        [
            {"timeframe": "1D", "status": "PASS", "metrics_summary": {"n_trades": 1}},
            {"timeframe": "1H", "status": "SKIP", "metrics_summary": {"n_trades": 1}},
        ],
    )
    assert rows2 == []
