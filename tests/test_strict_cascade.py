"""Tests for strict survivor cascade (Phase 1 A-fix).

Covers _resolve_cascade_input_symbols and cascade_mode snapshot writing.
All tests are deterministic, offline, and complete in <2 s.
"""
from __future__ import annotations

import json
from pathlib import Path

from scripts.octa_autopilot import (
    _resolve_cascade_input_symbols,
    _write_resolved_config_snapshot,
)


def test_strict_cascade_uses_survivors_not_reselect() -> None:
    """strict_survivor + non-empty prev_survivors → pool == survivors exactly."""
    survivors = ["AAPL", "MSFT"]
    gg_pass = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]

    source, pool = _resolve_cascade_input_symbols(
        tf="1H",
        prev_survivors=survivors,
        gg_pass_symbols=gg_pass,
        cascade_mode="strict_survivor",
        decouple_tf_pool_from_prev_tf=True,
    )
    assert source == "strict_survivor", f"expected 'strict_survivor', got {source!r}"
    assert pool == {"AAPL", "MSFT"}, f"pool should be survivors only, got {pool}"
    # Crucially: gg_pass symbols that are NOT survivors must be excluded
    assert "GOOG" not in pool
    assert "AMZN" not in pool


def test_strict_cascade_fallback_when_no_survivors() -> None:
    """strict_survivor + empty prev_survivors → fallback to gg_pass_symbols."""
    gg_pass = ["GOOG", "AMZN", "TSLA"]

    source, pool = _resolve_cascade_input_symbols(
        tf="30M",
        prev_survivors=[],
        gg_pass_symbols=gg_pass,
        cascade_mode="strict_survivor",
        decouple_tf_pool_from_prev_tf=True,
    )
    assert source == "strict_survivor_fallback_empty", f"unexpected source: {source!r}"
    assert pool == {"GOOG", "AMZN", "TSLA"}, f"fallback pool mismatch: {pool}"


def test_strict_cascade_legacy_mode_unchanged() -> None:
    """cascade_mode='legacy' with decouple=True → gg_pass_symbols (existing behavior)."""
    survivors = ["AAPL"]
    gg_pass = ["AAPL", "MSFT", "GOOG"]

    source, pool = _resolve_cascade_input_symbols(
        tf="1H",
        prev_survivors=survivors,
        gg_pass_symbols=gg_pass,
        cascade_mode="legacy",
        decouple_tf_pool_from_prev_tf=True,
    )
    # Legacy decouple=True path: returns gg_pass_symbols
    assert pool == {"AAPL", "MSFT", "GOOG"}, f"legacy pool should be gg_pass, got {pool}"
    assert source == "tf_precheck_pass"


def test_strict_cascade_1d_unaffected_by_cascade_mode() -> None:
    """1D stage is never affected by cascade_mode — always entry_1D_global_pass."""
    survivors = ["AAPL", "MSFT"]
    gg_pass = ["AAPL", "MSFT", "GOOG"]

    source, pool = _resolve_cascade_input_symbols(
        tf="1D",
        prev_survivors=survivors,
        gg_pass_symbols=gg_pass,
        cascade_mode="strict_survivor",
        decouple_tf_pool_from_prev_tf=True,
    )
    # 1D: delegates to _resolve_stage_pool which returns prev_survivors (eligible_symbols)
    assert source == "entry_1D_global_pass"
    assert pool == {"AAPL", "MSFT"}


def test_snapshot_has_cascade_mode(tmp_path: Path) -> None:
    """_write_resolved_config_snapshot persists cascade_mode in JSON."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    _write_resolved_config_snapshot(run_dir, {"training_config_path": "configs/dev.yaml", "cascade_mode": "strict_survivor"})

    snap = json.loads((run_dir / "resolved_config_snapshot.json").read_text())
    assert snap.get("cascade_mode") == "strict_survivor", f"cascade_mode missing or wrong: {snap}"
    assert snap.get("training_config_path") == "configs/dev.yaml"


def test_snapshot_cascade_mode_default_legacy(tmp_path: Path) -> None:
    """When cascade_mode not in cfg, snapshot gets 'legacy'."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    # Simulate the autopilot path: cfg has no cascade_mode key
    cfg: dict = {}
    cascade_mode = str(cfg.get("cascade_mode", "legacy"))
    _write_resolved_config_snapshot(run_dir, {"cascade_mode": cascade_mode})

    snap = json.loads((run_dir / "resolved_config_snapshot.json").read_text())
    assert snap.get("cascade_mode") == "legacy"
