"""A3 regression: all universe *_dir paths in autopilot configs must exist on disk."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_CONFIGS = [
    "configs/autopilot_daily.yaml",
    "configs/autopilot_test_50.yaml",
    "configs/autopilot_test_100.yaml",
    "configs/autonomous_paper.yaml",
]
_REPO = Path(__file__).parent.parent


def _universe_dirs(cfg_path: str):
    """Return list of (key, path_str) for all universe *_dir entries."""
    full = _REPO / cfg_path
    data = yaml.safe_load(full.read_text(encoding="utf-8")) or {}
    universe = data.get("universe") or {}
    return [(k, v) for k, v in universe.items() if k.endswith("_dir")]


@pytest.mark.parametrize("cfg_path", _CONFIGS)
def test_universe_dirs_all_exist(cfg_path: str) -> None:
    """Every *_dir value in the universe section must resolve to an existing directory."""
    pairs = _universe_dirs(cfg_path)
    assert pairs, f"{cfg_path} has no universe.*_dir entries"
    missing = []
    for key, val in pairs:
        p = _REPO / val
        if not p.exists():
            missing.append(f"{key}={val!r} (resolved: {p})")
    assert not missing, f"{cfg_path} has missing dirs:\n" + "\n".join(missing)


def test_futures_dir_is_Futures_Parquet_not_stub() -> None:
    """futures_dir must point to raw/Futures_Parquet (has data) not raw/futures (empty stub)."""
    bad = []
    for cfg_path in _CONFIGS:
        full = _REPO / cfg_path
        data = yaml.safe_load(full.read_text(encoding="utf-8")) or {}
        universe = data.get("universe") or {}
        val = universe.get("futures_dir")
        if val is not None and val != "raw/Futures_Parquet":
            bad.append(f"{cfg_path}: futures_dir={val!r}")
    assert not bad, "futures_dir still points to wrong path:\n" + "\n".join(bad)


def test_fx_dir_is_FX_parquet_not_stub() -> None:
    """fx_dir must point to raw/FX_parquet (has data) not raw/fx (empty stub)."""
    bad = []
    for cfg_path in _CONFIGS:
        full = _REPO / cfg_path
        data = yaml.safe_load(full.read_text(encoding="utf-8")) or {}
        universe = data.get("universe") or {}
        val = universe.get("fx_dir")
        if val is not None and val != "raw/FX_parquet":
            bad.append(f"{cfg_path}: fx_dir={val!r}")
    assert not bad, "fx_dir still points to wrong path:\n" + "\n".join(bad)
