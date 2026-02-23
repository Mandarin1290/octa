from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scripts.octa_autopilot import (
    _load_dynamic_gate_cfg,
    _resolve_universe_limit,
    _write_dynamic_gate_evidence,
)


def test_dynamic_gate_cfg_defaults_when_not_configured() -> None:
    cfg, snapshot = _load_dynamic_gate_cfg({})
    assert cfg.enabled is False
    assert snapshot["configured"] is False
    assert snapshot["validation_errors"] == []
    assert "dynamic_gate_section_missing" in snapshot["defaults_applied"]


def test_dynamic_gate_cfg_enabled_valid() -> None:
    raw = {
        "dynamic_gate": {
            "enabled": True,
            "method": "percentile_rank",
            "min_population": 5,
            "top_percentile": 0.70,
            "fallback_top_k_if_empty": 1,
            "required_metrics": ["sharpe", "profit_factor"],
            "metrics": {
                "sharpe": {"direction": "high", "weight": 0.6},
                "profit_factor": {"direction": "high", "weight": 0.4},
            },
        }
    }
    cfg, snapshot = _load_dynamic_gate_cfg(raw)
    assert cfg.enabled is True
    assert cfg.required_metrics == ("sharpe", "profit_factor")
    assert snapshot["validation_errors"] == []


def test_dynamic_gate_cfg_enabled_invalid_missing_required_keys() -> None:
    raw = {"dynamic_gate": {"enabled": True}}
    cfg, snapshot = _load_dynamic_gate_cfg(raw)
    assert cfg.enabled is True
    errs = set(snapshot["validation_errors"])
    assert "missing_required_metrics" in errs
    assert "missing_metrics" in errs


def test_dynamic_gate_evidence_files_written(tmp_path) -> None:
    run_dir = tmp_path / "run"
    snapshot = {"configured": False, "raw": {}, "resolved": {"enabled": False}, "defaults_applied": [], "validation_errors": []}
    status = {"enabled": False, "status": "dynamic_gate_not_configured", "fallback_used": False, "population_n_by_stage": {}}
    _write_dynamic_gate_evidence(run_dir, snapshot, status)
    snap_path = run_dir / "resolved_config_snapshot.json"
    status_path = run_dir / "dynamic_gate_status.json"
    assert snap_path.exists()
    assert status_path.exists()
    snap_json = json.loads(snap_path.read_text(encoding="utf-8"))
    status_json = json.loads(status_path.read_text(encoding="utf-8"))
    assert "dynamic_gate" in snap_json
    assert status_json["status"] == "dynamic_gate_not_configured"


def test_universe_limit_zero_fails() -> None:
    with pytest.raises(SystemExit, match="universe_limit_zero_invalid"):
        _resolve_universe_limit(0, {"universe": {"limit": 0}})


def test_dynamic_gate_present_and_enabled() -> None:
    cfg = yaml.safe_load(Path("configs/autonomous_paper.yaml").read_text(encoding="utf-8")) or {}
    dg = cfg.get("dynamic_gate", {}) if isinstance(cfg.get("dynamic_gate"), dict) else {}
    assert bool(dg.get("enabled", False)) is True


def test_config_resolution_snapshot_written(tmp_path) -> None:
    cfg, snapshot = _load_dynamic_gate_cfg(
        {
            "dynamic_gate": {
                "enabled": True,
                "method": "percentile_rank",
                "min_population": 30,
                "top_percentile": 0.70,
                "fallback_top_k_if_empty": 1,
                "required_metrics": ["sharpe", "pf", "calmar", "max_dd"],
                "metrics": {
                    "sharpe": {"direction": "high", "weight": 0.35},
                    "pf": {"direction": "high", "weight": 0.25},
                    "calmar": {"direction": "high", "weight": 0.20},
                    "max_dd": {"direction": "low", "weight": 0.20},
                },
            }
        }
    )
    run_dir = tmp_path / "run2"
    _write_dynamic_gate_evidence(
        run_dir,
        snapshot,
        {"enabled": bool(cfg.enabled), "status": "dynamic_gate_enabled", "fallback_used": False, "population_n_by_stage": {}},
    )
    snap = json.loads((run_dir / "resolved_config_snapshot.json").read_text(encoding="utf-8"))
    assert snap["dynamic_gate"]["resolved"]["enabled"] is True
