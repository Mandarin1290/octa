"""P4c: Live Regime Detector tests."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from octa.core.regime.live_regime import detect_live_regime, load_live_regime


def _make_prices(vol_target: float, n: int = 50) -> list:
    """Generate a price series with approximately the target annualised vol."""
    rng = np.random.default_rng(42)
    daily_vol = vol_target / np.sqrt(252)
    returns = rng.normal(0, daily_vol, n)
    prices = [100.0]
    for r in returns:
        prices.append(prices[-1] * float(np.exp(r)))
    return prices


def test_low_vol_regime() -> None:
    prices = _make_prices(vol_target=0.05, n=50)
    regime = detect_live_regime("TEST", prices, lookback=20)
    assert regime == "low_vol"


def test_high_vol_regime() -> None:
    prices = _make_prices(vol_target=0.50, n=50)
    regime = detect_live_regime("TEST", prices, lookback=20)
    assert regime == "high_vol"


def test_mid_vol_regime() -> None:
    prices = _make_prices(vol_target=0.18, n=50)
    regime = detect_live_regime("TEST", prices, lookback=20)
    assert regime == "mid_vol"


def test_insufficient_prices_returns_mid_vol() -> None:
    regime = detect_live_regime("TEST", [100.0, 101.0], lookback=20)
    assert regime == "mid_vol"


def test_load_live_regime_missing_file(tmp_path: Path) -> None:
    result = load_live_regime("ADC", regime_path=str(tmp_path / "nonexistent.json"))
    assert result is None


def test_load_live_regime_stale(tmp_path: Path) -> None:
    stale_time = (datetime.utcnow() - timedelta(hours=30)).isoformat()
    path = tmp_path / "regime.json"
    path.write_text(json.dumps({
        "updated_at": stale_time,
        "regimes": {"ADC": "low_vol"},
    }), encoding="utf-8")
    result = load_live_regime("ADC", regime_path=str(path))
    assert result is None


def test_load_live_regime_fresh(tmp_path: Path) -> None:
    path = tmp_path / "regime.json"
    path.write_text(json.dumps({
        "updated_at": datetime.utcnow().isoformat(),
        "regimes": {"ADC": "high_vol"},
    }), encoding="utf-8")
    result = load_live_regime("ADC", regime_path=str(path))
    assert result == "high_vol"
