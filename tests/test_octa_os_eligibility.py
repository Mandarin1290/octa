from __future__ import annotations

import json
from pathlib import Path

from octa.os.eligibility import read_blessed_eligibility


def test_primary_approved_loader(monkeypatch, tmp_path: Path) -> None:
    rows = [
        {
            "symbol": "AAPL",
            "timeframe": "1D",
            "has_model": True,
            "has_sig": True,
            "has_sha256": True,
        },
        {
            "symbol": "AAPL",
            "timeframe": "1H",
            "has_model": True,
            "has_sig": True,
            "has_sha256": True,
        },
    ]

    import octa.models.approved_loader as mod

    monkeypatch.setattr(mod, "list_approved_models", lambda *_a, **_k: rows)
    snap = read_blessed_eligibility(tmp_path / "missing.jsonl")
    assert snap.eligible_symbols == ["AAPL"]
    assert snap.reason == "ok"


def test_fallback_jsonl_when_approved_empty(monkeypatch, tmp_path: Path) -> None:
    import octa.models.approved_loader as mod

    monkeypatch.setattr(mod, "list_approved_models", lambda *_a, **_k: [])

    reg = tmp_path / "blessed_models.jsonl"
    reg.write_text(
        json.dumps({"symbol": "MSFT", "performance_1d": "PASS", "performance_1h": "PASS"}) + "\n",
        encoding="utf-8",
    )
    snap = read_blessed_eligibility(reg)
    assert snap.eligible_symbols == ["MSFT"]
    assert snap.reason == "ok"
