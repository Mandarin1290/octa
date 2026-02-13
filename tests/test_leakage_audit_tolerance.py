from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from octa.core.features.features import build_features, leakage_audit


def _settings(rtol: float = 1e-4, atol: float = 1e-6) -> SimpleNamespace:
    features = {
        "window_short": 5,
        "window_med": 20,
        "window_long": 60,
        "vol_window": 20,
        "horizons": [1, 3, 5],
        "leakage_audit_rtol": rtol,
        "leakage_audit_atol": atol,
        "macro": {"enabled": False},
    }
    return SimpleNamespace(
        features=features,
        window_short=5,
        window_med=20,
        window_long=60,
        vol_window=20,
        horizons=[1, 3, 5],
        leakage_audit_rtol=rtol,
        leakage_audit_atol=atol,
    )


def _raw_bars(n: int = 420) -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=n, freq="D", tz="UTC")
    base = np.linspace(100.0, 160.0, num=n)
    close = base + 0.3 * np.sin(np.linspace(0, 20, num=n))
    open_ = close - 0.1
    high = close + 0.5
    low = close - 0.5
    volume = np.linspace(1_000.0, 2_000.0, num=n)
    return pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        },
        index=idx,
    )


def test_leakage_audit_small_drift_within_tolerance_is_ok() -> None:
    settings = _settings(rtol=1e-4, atol=1e-6)
    raw = _raw_bars()
    res = build_features(raw, settings=settings, asset_class="stock")
    X = res.X.copy()
    target_idx = X.index[-7]
    col = "return_1"
    X.at[target_idx, col] = float(X.at[target_idx, col]) + 1e-8

    ok, report = leakage_audit(
        X,
        res.y_dict,
        raw,
        [1, 3, 5],
        settings=settings,
        asset_class="stock",
        return_report=True,
    )

    assert ok is True
    assert report["audit_drift_ok"] is True
    assert report["status"] == "audit_drift_ok"
    assert report["outside_tolerance_count"] == 0
    assert report["max_abs_diff"] > 0.0


def test_leakage_audit_future_shift_fails_closed() -> None:
    settings = _settings(rtol=1e-4, atol=1e-6)
    raw = _raw_bars()
    res = build_features(raw, settings=settings, asset_class="stock")
    X = res.X.copy()
    col = "return_1"
    leaked = raw["close"].shift(-1).reindex(X.index)
    X[col] = leaked.to_numpy()

    ok, report = leakage_audit(
        X,
        res.y_dict,
        raw,
        [1, 3, 5],
        settings=settings,
        asset_class="stock",
        return_report=True,
    )

    assert ok is False
    assert report["status"] == "leakage_detected"
    assert report["outside_tolerance_count"] > 0
    assert len(report["outside_tolerance_examples"]) > 0
