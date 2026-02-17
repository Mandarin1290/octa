from __future__ import annotations

import json
from pathlib import Path

from octa.support.ops import v000_full_universe_cascade_train as mod


def test_v000_writes_evidence_and_strict_eligibility(monkeypatch, tmp_path: Path):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        """
run_id: run_x
training_config: configs/dev.yaml
cascade_order: [1D,1H]
cascade_jobs:
  - symbol: AAA
    asset_class: stock
    parquet_paths:
      1D: /tmp/a.parquet
      1H: /tmp/b.parquet
ibkr:
  host: 127.0.0.1
  port: 7497
  require_port: false
  autologin:
    enabled: false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def _fake_run_cascade_training(**_kwargs):
        decisions = [
            type("D", (), {"symbol": "AAA", "timeframe": "1D", "stage": "train", "status": "GATE_FAIL", "reason": "sharpe", "details": {"structural_pass": True, "performance_pass": False}})(),
            type("D", (), {"symbol": "AAA", "timeframe": "1H", "stage": "train", "status": "PASS", "reason": None, "details": {"structural_pass": True, "performance_pass": True}})(),
        ]
        return decisions, {"1D": {}, "1H": {}}

    monkeypatch.setattr(mod, "run_cascade_training", _fake_run_cascade_training)
    monkeypatch.setattr(mod, "ibkr_health", lambda _cfg: {"healthy": False})
    monkeypatch.setattr(mod, "ensure_ibkr_running", lambda _cfg: {"ok": False, "skipped": True})

    out = tmp_path / "out"
    result = mod.run(str(cfg_path), out)
    assert result["summary"]["eligible_count"] == 0

    assert (out / "summary.json").exists()
    assert (out / "cascade_status_table.json").exists()
    assert (out / "discovery_ibkr.json").exists()
    assert (out / "ibkr_health.json").exists()
    assert (out / "ibkr_autologin_events.jsonl").exists()
    assert (out / "hashes.json").exists()
    assert (out / "hashes.txt").exists()

    eligible = json.loads((out / "execution_eligible_universe.json").read_text(encoding="utf-8"))
    assert eligible["symbols"] == []


def test_v000_emits_disclaimer_when_x11_missing(monkeypatch, tmp_path: Path):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text("run_id: run_y\n", encoding="utf-8")
    monkeypatch.delenv("DISPLAY", raising=False)

    out = tmp_path / "out"
    mod.run(str(cfg_path), out)
    disc = json.loads((out / "disclaimer.json").read_text(encoding="utf-8"))
    assert disc["disclaimer_code"] == "IBKR_X11_UNAVAILABLE"
    assert disc["action"] == "LOCK_EXECUTION_SHADOW_ONLY"
