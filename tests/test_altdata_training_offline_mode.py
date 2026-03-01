"""Tests: training sidecar uses ONLY local cache — no live network calls.

Evidence: altdata_diag_20260228T184706Z
Root cause: config/altdat.yaml had no offline_only key.
  FRED_API_KEY set in env + offline_only missing → live FRED fetch during training.

Fix: offline_only: true added to config/altdat.yaml.
  feature_builder.py:160 routes FRED through read_snapshot(fallback_nearest=True).
  If cache missing → meta["sources"]["fred"]["error"]="missing_cache", no exception.
"""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_bars(n: int = 30, freq: str = "B") -> pd.DataFrame:
    idx = pd.date_range("2026-01-01", periods=n, freq=freq, tz="UTC")
    np.random.seed(0)
    close = 100 + np.cumsum(np.random.randn(n))
    return pd.DataFrame({"open": close, "high": close + 0.5, "low": close - 0.5,
                         "close": close, "volume": 1000.0}, index=idx)


def _altdat_cfg_offline(tmp_path: Path) -> dict:
    """Minimal altdat config with offline_only=True and FRED enabled."""
    return {
        "enabled": True,
        "offline_only": True,
        "strict_mode": False,
        "auto_install": False,
        "storage": {"root": str(tmp_path / "altdata")},
        "asof": {"tolerance_seconds": {"1D": 3888000}},
        "sources": {
            "fred": {
                "enabled": True,
                "api_key_env": "FRED_API_KEY",
                "series": ["FEDFUNDS", "DGS10"],
            },
            "edgar": {"enabled": False},
            "gdelt": {"enabled": False},
            "satellite": {"enabled": False},
        },
    }


# ---------------------------------------------------------------------------
# Test: offline_only blocks network even when FRED_API_KEY is set
# ---------------------------------------------------------------------------

def test_offline_only_blocks_fred_network_call(tmp_path, monkeypatch):
    """With offline_only=True, fetch_fred_series must never be called even if key is set."""
    from octa.core.features.transforms.feature_builder import build_altdata_features

    monkeypatch.setenv("FRED_API_KEY", "fake-key-should-not-be-used")
    monkeypatch.delenv("OKTA_ALTDATA_OFFLINE_ONLY", raising=False)

    bars = _make_bars()
    cfg = _altdat_cfg_offline(tmp_path)

    with patch("octa.core.features.transforms.feature_builder.fetch_fred_series") as mock_net:
        result = build_altdata_features(bars_df=bars, symbol="ABM", altdat_cfg=cfg, tz="UTC")

    mock_net.assert_not_called()
    assert result.meta.get("offline_only") is True


def test_offline_only_missing_cache_returns_evidence_error_not_exception(tmp_path, monkeypatch):
    """With offline_only=True and NO cache present: meta has error, no exception raised."""
    from octa.core.features.transforms.feature_builder import build_altdata_features

    monkeypatch.setenv("FRED_API_KEY", "fake-key-should-not-be-used")
    bars = _make_bars()
    cfg = _altdat_cfg_offline(tmp_path)

    result = build_altdata_features(bars_df=bars, symbol="ABM", altdat_cfg=cfg, tz="UTC")

    # Must not raise — returns graceful result
    fred_meta = result.meta.get("sources", {}).get("fred", {})
    assert fred_meta.get("ok") is False
    assert fred_meta.get("error") == "missing_cache"
    # features_df is still a DataFrame (empty or partial)
    assert isinstance(result.features_df, pd.DataFrame)


def test_offline_only_uses_cache_when_present(tmp_path, monkeypatch):
    """With offline_only=True and cache present: macro features loaded from snapshot."""
    from octa.core.data.sources.altdata.cache import write_snapshot
    from octa.core.features.transforms.feature_builder import build_altdata_features

    monkeypatch.setenv("FRED_API_KEY", "fake-key-should-not-be-used")
    bars = _make_bars(n=30, freq="B")
    cfg = _altdat_cfg_offline(tmp_path)
    cache_root = str(tmp_path / "altdata")

    asof = bars.index[-1].date()
    payload = {
        "series": {
            "FEDFUNDS": [{"ts": str(bars.index[0].date()), "value": 5.33}],
            "DGS10": [{"ts": str(bars.index[0].date()), "value": 4.21}],
        }
    }
    write_snapshot(source="fred", asof=asof, payload=payload,
                   meta={"seed": True, "source": "fred"}, root=cache_root)

    with patch("octa.core.features.transforms.feature_builder.fetch_fred_series") as mock_net:
        result = build_altdata_features(bars_df=bars, symbol="ABM", altdat_cfg=cfg, tz="UTC")

    mock_net.assert_not_called()
    fred_meta = result.meta.get("sources", {}).get("fred", {})
    assert fred_meta.get("ok") is True
    assert "fred" in result.meta.get("sources_used", [])


# ---------------------------------------------------------------------------
# Test: config/altdat.yaml (repo file) has offline_only=True
# ---------------------------------------------------------------------------

def test_repo_altdat_yaml_has_offline_only_true():
    """The repo config/altdat.yaml must have offline_only: true (training mode)."""
    cfg_path = Path("config/altdat.yaml")
    assert cfg_path.exists(), "config/altdat.yaml missing from repo"
    cfg = yaml.safe_load(cfg_path.read_text())
    assert cfg.get("offline_only") is True, (
        "config/altdat.yaml must have offline_only: true to prevent live "
        "FRED network calls during training. "
        "Refresh uses build_altdata_stack(allow_net=True) which is unaffected."
    )


# ---------------------------------------------------------------------------
# Test: sidecar.try_run never raises even if altdata is fully offline
# ---------------------------------------------------------------------------

def test_sidecar_try_run_never_raises_in_offline_mode(tmp_path, monkeypatch):
    """sidecar.try_run is fail-safe: always returns (DataFrame, meta), never raises."""
    from octa.core.data.sources.altdata.sidecar import try_run

    # Point to tmp altdat config with offline_only and no real cache
    cfg_content = yaml.dump({
        "enabled": True,
        "offline_only": True,
        "strict_mode": False,
        "auto_install": False,
        "storage": {"root": str(tmp_path / "altdata")},
        "sources": {
            "fred": {"enabled": True, "api_key_env": "FRED_API_KEY", "series": ["FEDFUNDS"]},
        },
    })
    cfg_path = tmp_path / "altdat.yaml"
    cfg_path.write_text(cfg_content)
    monkeypatch.setenv("OKTA_ALTDATA_CONFIG", str(cfg_path))
    monkeypatch.setenv("FRED_API_KEY", "fake-key")

    bars = _make_bars()
    settings = MagicMock()
    settings.symbol = "ABM"
    settings.timezone = "UTC"

    features_df, meta = try_run(bars_df=bars, settings=settings, asset_class="stock")

    assert isinstance(features_df, pd.DataFrame)
    assert isinstance(meta, dict)
    # No live call should happen
    assert meta.get("status") != "ERROR" or "fetch" not in str(meta.get("error", "")).lower()


# ---------------------------------------------------------------------------
# Test: OKTA_ALTDATA_OFFLINE_ONLY env var also engages offline mode
# ---------------------------------------------------------------------------

def test_env_var_offline_only_blocks_network(tmp_path, monkeypatch):
    """OKTA_ALTDATA_OFFLINE_ONLY=1 in env must block network even if config says offline_only=False."""
    from octa.core.features.transforms.feature_builder import build_altdata_features

    monkeypatch.setenv("FRED_API_KEY", "fake-key")
    monkeypatch.setenv("OKTA_ALTDATA_OFFLINE_ONLY", "1")

    bars = _make_bars()
    # Config deliberately has offline_only=False to test env-var override
    cfg = _altdat_cfg_offline(tmp_path)
    cfg["offline_only"] = False  # override to False — env var should still win

    with patch("octa.core.features.transforms.feature_builder.fetch_fred_series") as mock_net:
        result = build_altdata_features(bars_df=bars, symbol="ABM", altdat_cfg=cfg, tz="UTC")

    mock_net.assert_not_called()
    assert result.meta.get("offline_only") is True
