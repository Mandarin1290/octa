from __future__ import annotations

import json
from pathlib import Path


def test_meta_contains_required_fields(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    (out_root / "meta").mkdir(parents=True)

    sym = "META"
    meta = {
        "symbol": sym,
        "asset_profile": "stock",
        "run_id": "test",
        "created_utc": "2026-01-01T00:00:00Z",
        "gate_version": "hf_gate_2026-01-03_v1",
        "timeframe_status": {"1D": "PASS", "1H": "SKIP_H1_NOT_ELIGIBLE"},
        "datasets": {"1D": {"path": "x", "sha256": "y", "rows": 1, "start": "s", "end": "e"}, "1H": None},
        "models": {"1D": {"pkl": "p", "sha256": "h"}, "1H": None},
        "gates": {"1D": {"status": "PASS_FULL", "gate_version": "hf_gate_2026-01-03_v1", "reasons": [], "passed_checks": []}, "1H": {"status": "SKIP_H1_NOT_ELIGIBLE"}},
        "armed": True,
    }

    (out_root / "meta" / f"{sym}.meta.json").write_text(json.dumps(meta))

    loaded = json.loads((out_root / "meta" / f"{sym}.meta.json").read_text())
    assert loaded["symbol"] == sym
    assert loaded["asset_profile"] == "stock"
    assert "timeframe_status" in loaded
    assert "datasets" in loaded
    assert "models" in loaded
    assert "gates" in loaded
    assert loaded["datasets"]["1D"]["sha256"]
    assert loaded["gates"]["1D"]["gate_version"]
