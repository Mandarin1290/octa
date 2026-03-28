"""P0-2, P0-4, P0-5 shadow activation and wiring tests.

Coverage:
- P0-2: run_foundation_shadow() accepts ml_inference_enabled, artifact_dir, raw_data_dir, inference_timeframe
- P0-2: CLI --ml-inference-enabled flag activates inference
- P0-2: Default ml_inference_enabled=False (safe default preserved)
- P0-4: run_foundation_shadow() accepts broker_cfg_path; wired to ExecutionConfig
- P0-4: broker_cfg_path=None → pre-execution gate skipped (dry-run safe)
- P0-5: paper_ready_dir wired through control_plane → ExecutionConfig → inference_bridge
- P0-5: paper mode remains blocked by _enforce_foundation_scope()
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# P0-2: Canonical Shadow ML activation
# ---------------------------------------------------------------------------

def test_run_foundation_shadow_default_ml_disabled(tmp_path: Path) -> None:
    """P0-2: Default ml_inference_enabled=False — inference never fires without explicit flag."""
    from octa.foundation.control_plane import run_foundation_shadow

    captured = {}

    def mock_run_execution(cfg):
        captured["ml_enabled"] = cfg.ml_inference_enabled
        return {"status": "dry_run_complete", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        run_foundation_shadow(
            max_symbols=1,
            evidence_dir=tmp_path / "ev",
        )

    assert captured["ml_enabled"] is False


def test_run_foundation_shadow_ml_enabled_activates(tmp_path: Path) -> None:
    """P0-2: ml_inference_enabled=True → ExecutionConfig.ml_inference_enabled=True."""
    from octa.foundation.control_plane import run_foundation_shadow

    captured = {}

    def mock_run_execution(cfg):
        captured["ml_enabled"] = cfg.ml_inference_enabled
        captured["artifact_dir"] = cfg.artifact_dir
        captured["raw_data_dir"] = cfg.raw_data_dir
        captured["timeframe"] = cfg.inference_timeframe
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        run_foundation_shadow(
            max_symbols=1,
            evidence_dir=tmp_path / "ev",
            ml_inference_enabled=True,
            artifact_dir=Path("raw") / "PKL",
            raw_data_dir=Path("raw"),
            inference_timeframe="1D",
        )

    assert captured["ml_enabled"] is True
    assert captured["artifact_dir"] == Path("raw") / "PKL"
    assert captured["raw_data_dir"] == Path("raw")
    assert captured["timeframe"] == "1D"


def test_cli_shadow_default_no_ml_flag(tmp_path: Path) -> None:
    """P0-2: CLI shadow without --ml-inference-enabled → ml_inference_enabled=False."""
    from octa.foundation.control_plane import main

    captured = {}

    def mock_run_execution(cfg):
        captured["ml_enabled"] = cfg.ml_inference_enabled
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        main([
            "shadow",
            "--max-symbols", "1",
            "--evidence-dir", str(tmp_path / "ev"),
        ])

    assert captured["ml_enabled"] is False


def test_cli_shadow_with_ml_flag(tmp_path: Path) -> None:
    """P0-2: CLI shadow with --ml-inference-enabled → ml_inference_enabled=True."""
    from octa.foundation.control_plane import main

    captured = {}

    def mock_run_execution(cfg):
        captured["ml_enabled"] = cfg.ml_inference_enabled
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        main([
            "shadow",
            "--max-symbols", "1",
            "--evidence-dir", str(tmp_path / "ev"),
            "--ml-inference-enabled",
        ])

    assert captured["ml_enabled"] is True


def test_cli_shadow_inference_timeframe_flag(tmp_path: Path) -> None:
    """P0-2: CLI --inference-timeframe is normalized to uppercase and passed through."""
    from octa.foundation.control_plane import main

    captured = {}

    def mock_run_execution(cfg):
        captured["timeframe"] = cfg.inference_timeframe
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        main([
            "shadow",
            "--max-symbols", "1",
            "--evidence-dir", str(tmp_path / "ev"),
            "--inference-timeframe", "1h",
        ])

    assert captured["timeframe"] == "1H"


# ---------------------------------------------------------------------------
# P0-4: TWS pre-execution gate wiring
# ---------------------------------------------------------------------------

def test_run_foundation_shadow_broker_cfg_none_skips_gate(tmp_path: Path) -> None:
    """P0-4: broker_cfg_path=None (default) → ExecutionConfig.broker_cfg_path=None → gate skipped."""
    from octa.foundation.control_plane import run_foundation_shadow

    captured = {}

    def mock_run_execution(cfg):
        captured["broker_cfg_path"] = cfg.broker_cfg_path
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        run_foundation_shadow(
            max_symbols=1,
            evidence_dir=tmp_path / "ev",
        )

    assert captured["broker_cfg_path"] is None


def test_run_foundation_shadow_broker_cfg_wired(tmp_path: Path) -> None:
    """P0-4: broker_cfg_path set → wired into ExecutionConfig."""
    from octa.foundation.control_plane import run_foundation_shadow

    captured = {}
    broker_cfg = tmp_path / "broker.yaml"
    broker_cfg.touch()

    def mock_run_execution(cfg):
        captured["broker_cfg_path"] = cfg.broker_cfg_path
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        run_foundation_shadow(
            max_symbols=1,
            evidence_dir=tmp_path / "ev",
            broker_cfg_path=broker_cfg,
        )

    assert captured["broker_cfg_path"] == broker_cfg


def test_cli_shadow_broker_cfg_flag(tmp_path: Path) -> None:
    """P0-4: CLI --broker-cfg passes path to ExecutionConfig."""
    from octa.foundation.control_plane import main

    captured = {}
    broker_cfg = tmp_path / "broker.yaml"
    broker_cfg.touch()

    def mock_run_execution(cfg):
        captured["broker_cfg_path"] = cfg.broker_cfg_path
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        main([
            "shadow",
            "--max-symbols", "1",
            "--evidence-dir", str(tmp_path / "ev"),
            "--broker-cfg", str(broker_cfg),
        ])

    assert captured["broker_cfg_path"] == broker_cfg


# ---------------------------------------------------------------------------
# P0-5: Shadow → paper_ready handoff wiring
# ---------------------------------------------------------------------------

def test_run_foundation_shadow_paper_ready_dir_default(tmp_path: Path) -> None:
    """P0-5: Default paper_ready_dir is None (not forced) — caller must be explicit."""
    from octa.foundation.control_plane import run_foundation_shadow

    captured = {}

    def mock_run_execution(cfg):
        captured["paper_ready_dir"] = cfg.paper_ready_dir
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        run_foundation_shadow(
            max_symbols=1,
            evidence_dir=tmp_path / "ev",
        )

    # Default is None — explicit wiring requires explicit opt-in
    assert captured["paper_ready_dir"] is None


def test_run_foundation_shadow_paper_ready_dir_explicit(tmp_path: Path) -> None:
    """P0-5: paper_ready_dir explicitly passed → wired into ExecutionConfig."""
    from octa.foundation.control_plane import run_foundation_shadow

    captured = {}
    paper_dir = tmp_path / "paper_ready"

    def mock_run_execution(cfg):
        captured["paper_ready_dir"] = cfg.paper_ready_dir
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        run_foundation_shadow(
            max_symbols=1,
            evidence_dir=tmp_path / "ev",
            paper_ready_dir=paper_dir,
        )

    assert captured["paper_ready_dir"] == paper_dir


def test_cli_shadow_paper_ready_dir_flag(tmp_path: Path) -> None:
    """P0-5: CLI --paper-ready-dir wires paper_ready_dir to ExecutionConfig."""
    from octa.foundation.control_plane import main

    captured = {}
    paper_dir = tmp_path / "paper_ready"

    def mock_run_execution(cfg):
        captured["paper_ready_dir"] = cfg.paper_ready_dir
        return {"status": "ok", "cycles": 0, "exit_code": 0}

    with patch("octa.foundation.control_plane.run_execution", side_effect=mock_run_execution):
        main([
            "shadow",
            "--max-symbols", "1",
            "--evidence-dir", str(tmp_path / "ev"),
            "--paper-ready-dir", str(paper_dir),
        ])

    assert captured["paper_ready_dir"] == paper_dir


def test_paper_mode_blocked_by_scope_enforcement() -> None:
    """P0-5: Paper execution remains blocked — _enforce_foundation_scope() raises SystemExit."""
    from octa.execution.runner import ExecutionConfig, run_execution

    cfg = ExecutionConfig(
        mode="paper",
        enable_live=False,
        i_understand_live_risk=False,
    )

    with pytest.raises((SystemExit, RuntimeError)):
        run_execution(cfg)
