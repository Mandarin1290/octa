"""Tests for paper_observe runtime profile and promotion stage reachability.

Verifies:
1. smoke_plus still clamps max_train_per_tf=1 and per_symbol_timeout=360 (regression guard)
2. paper_observe uses config-provided values without clamping to 1
3. paper_observe applies conservative ceilings (max 5 symbols, max 1800s)
4. promotion.stage_reached event is emitted in stage_progress.jsonl when pipeline reaches promotion
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers to import _load_training_budget_cfg without running main()
# ---------------------------------------------------------------------------

def _load_budget(raw_cfg: Dict[str, Any], runtime_profile: str) -> Dict[str, Any]:
    """Call _load_training_budget_cfg with a minimal step_budgets dict."""
    import importlib, types

    # Import the function directly from the module
    spec = importlib.util.spec_from_file_location(
        "octa_autopilot_mod",
        Path(__file__).parent.parent / "scripts" / "octa_autopilot.py",
    )
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)
    # Patch sys.argv so argparse doesn't run
    orig_argv = sys.argv
    sys.argv = ["octa_autopilot.py"]
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except SystemExit:
        pass
    finally:
        sys.argv = orig_argv

    step_budgets: Dict[str, int] = {
        "training_symbol": 300,
        "training_loop": 1200,
    }
    return mod._load_training_budget_cfg(raw_cfg, step_budgets, runtime_profile=runtime_profile)


# ---------------------------------------------------------------------------
# 1) smoke_plus still clamps
# ---------------------------------------------------------------------------

def test_smoke_plus_still_clamps() -> None:
    """smoke_plus must clamp max_train_per_tf to 1 and timeout to 360 regardless of config."""
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": {"1D": 3, "1H": 4, "30M": 3},
            "per_symbol_timeout_s": 900,
            "stage_timeout_s": 3600,
        }
    }
    result = _load_budget(cfg, "smoke_plus")

    assert result["runtime_profile"] == "smoke_plus"
    mt = result["max_train_symbols_per_tf"]
    if isinstance(mt, dict):
        assert all(v == 1 for v in mt.values()), f"smoke_plus must clamp all per-TF values to 1, got {mt}"
    else:
        assert mt == 1, f"smoke_plus must clamp scalar max_train to 1, got {mt}"
    assert result["per_symbol_timeout_s"] == 360, (
        f"smoke_plus must clamp per_symbol_timeout_s to 360, got {result['per_symbol_timeout_s']}"
    )


# ---------------------------------------------------------------------------
# 2) paper_observe uses config values without clamping to 1
# ---------------------------------------------------------------------------

def test_paper_observe_uses_config_with_no_clamp_to_one() -> None:
    """paper_observe must NOT clamp max_train_per_tf to 1 or timeout to 360."""
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": {"1D": 2, "1H": 3, "30M": 2},
            "per_symbol_timeout_s": 900,
            "stage_timeout_s": 3600,
        }
    }
    result = _load_budget(cfg, "paper_observe")

    assert result["runtime_profile"] == "paper_observe"
    mt = result["max_train_symbols_per_tf"]
    if isinstance(mt, dict):
        assert mt.get("1D", 0) >= 2, f"paper_observe must not clamp 1D to 1, got {mt}"
        assert mt.get("1H", 0) >= 3, f"paper_observe must not clamp 1H to 1, got {mt}"
    assert result["per_symbol_timeout_s"] >= 900, (
        f"paper_observe must not clamp timeout to 360, got {result['per_symbol_timeout_s']}"
    )


# ---------------------------------------------------------------------------
# 3) paper_observe applies conservative ceilings (max 5 / 1800s)
# ---------------------------------------------------------------------------

def test_paper_observe_applies_ceiling() -> None:
    """paper_observe must apply ceilings: max_train ≤ 5, timeout ≤ 1800s."""
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": {"1D": 999, "1H": 999},
            "per_symbol_timeout_s": 99999,
            "stage_timeout_s": 99999,
        }
    }
    result = _load_budget(cfg, "paper_observe")

    mt = result["max_train_symbols_per_tf"]
    if isinstance(mt, dict):
        assert all(v <= 5 for v in mt.values()), f"paper_observe ceiling must be ≤ 5, got {mt}"
    else:
        assert mt <= 5, f"paper_observe ceiling must be ≤ 5, got {mt}"
    assert result["per_symbol_timeout_s"] <= 1800, (
        f"paper_observe timeout ceiling must be ≤ 1800s, got {result['per_symbol_timeout_s']}"
    )


# ---------------------------------------------------------------------------
# 4) default profile is unaffected
# ---------------------------------------------------------------------------

def test_default_profile_unaffected() -> None:
    """default profile must not apply any clamping from smoke_plus or paper_observe."""
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": {"1D": 10, "1H": 10},
            "per_symbol_timeout_s": 1200,
            "stage_timeout_s": 7200,
        }
    }
    result = _load_budget(cfg, "default")

    assert result["runtime_profile"] == "default"
    mt = result["max_train_symbols_per_tf"]
    if isinstance(mt, dict):
        assert mt.get("1D", 0) >= 10, f"default must not clamp, got {mt}"
    assert result["per_symbol_timeout_s"] >= 1200, (
        f"default must not clamp timeout, got {result['per_symbol_timeout_s']}"
    )


# ---------------------------------------------------------------------------
# 5) paper_observe in validation whitelist (no SystemExit on valid profile)
# ---------------------------------------------------------------------------

def test_paper_observe_accepted_by_validation() -> None:
    """paper_observe must be in the runtime_profile validation whitelist."""
    valid_profiles = {"default", "fast_smoke", "smoke_plus", "paper_observe"}
    src = (Path(__file__).parent.parent / "scripts" / "octa_autopilot.py").read_text()
    # Find the validation line
    for line in src.splitlines():
        if "runtime_profile not in" in line and "default" in line:
            assert "paper_observe" in line, (
                f"paper_observe missing from validation whitelist:\n{line}"
            )
            break
    else:
        pytest.fail("Could not find runtime_profile validation line in octa_autopilot.py")


# ---------------------------------------------------------------------------
# 6) promotion.stage_reached event emitted in stage_progress.jsonl
# ---------------------------------------------------------------------------

def test_promotion_stage_reached_event_in_stage_progress(tmp_path: Path) -> None:
    """_append_stage_progress with step='promotion.stage_reached' writes correct JSONL."""
    import importlib

    spec = importlib.util.spec_from_file_location(
        "octa_autopilot_mod2",
        Path(__file__).parent.parent / "scripts" / "octa_autopilot.py",
    )
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)
    orig_argv = sys.argv
    sys.argv = ["octa_autopilot.py"]
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except SystemExit:
        pass
    finally:
        sys.argv = orig_argv

    mod._append_stage_progress(
        tmp_path,
        tf="1D",
        step="promotion.stage_reached",
        event="reached",
        elapsed_s=0.0,
        counts={
            "symbol": "AAPL",
            "gate_status": "PASS",
            "pkl_path_present": True,
            "runtime_profile": "paper_observe",
        },
    )

    jsonl = tmp_path / "stage_progress.jsonl"
    assert jsonl.exists(), "stage_progress.jsonl must be created"
    lines = [json.loads(l) for l in jsonl.read_text().strip().splitlines()]
    promo_events = [l for l in lines if l.get("step") == "promotion.stage_reached"]
    assert len(promo_events) == 1, f"Expected 1 promotion.stage_reached event, got {promo_events}"
    evt = promo_events[0]
    assert evt["event"] == "reached"
    assert evt["counts"]["symbol"] == "AAPL"
    assert evt["counts"]["gate_status"] == "PASS"
    assert evt["counts"]["pkl_path_present"] is True
    assert evt["counts"]["runtime_profile"] == "paper_observe"
