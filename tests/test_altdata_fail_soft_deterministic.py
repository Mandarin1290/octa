from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from octa.core.features.features import build_features


def _bars() -> pd.DataFrame:
    idx = pd.date_range("2021-01-01", periods=180, freq="H", tz="UTC")
    px = np.linspace(100.0, 120.0, len(idx))
    return pd.DataFrame(
        {
            "open": px,
            "high": px + 0.5,
            "low": px - 0.5,
            "close": px + np.sin(np.arange(len(idx)) / 10.0),
            "volume": np.full(len(idx), 1000.0),
        },
        index=idx,
    )


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        features={"window_short": 5, "window_med": 20, "window_long": 60, "vol_window": 20, "horizons": [1]},
        window_short=5,
        window_med=20,
        window_long=60,
        vol_window=20,
        horizons=[1],
        raw_dir="raw",
        symbol="TEST",
        timezone="UTC",
        timeframe="1H",
    )


def test_altdata_missing_sources_fail_soft_and_stable(monkeypatch) -> None:
    bars = _bars()
    settings = _settings()

    def _sidecar_a(**kwargs):
        idx = kwargs["bars_df"].index
        return (
            pd.DataFrame(index=idx),
            {
                "enabled": True,
                "status": "MISSING_SOURCES",
                "sources": {
                    "fred": {"status": "MISSING", "rows": 0, "error": "missing_cache"},
                    "edgar": {"status": "ERROR", "rows": 0, "error": "source_error"},
                    "stooq": {"status": "OK", "rows": 10},
                },
                "coverage": {"fred": 0.0, "edgar": 0.0, "stooq": 0.8},
            },
        )

    monkeypatch.setattr("octa.core.data.sources.altdata.sidecar.try_run", _sidecar_a)
    r1 = build_features(bars, settings, asset_class="stock", build_targets=False)
    assert r1.meta["altdata_enabled"] is True
    assert r1.meta["altdata_degraded"] is True
    assert any(x["source"] == "fred" and x["status"] == "MISSING" for x in r1.meta["altdata_meta"])

    def _sidecar_b(**kwargs):
        idx = kwargs["bars_df"].index
        return (
            pd.DataFrame(index=idx),
            {
                "enabled": True,
                "status": "MISSING_SOURCES",
                "sources": {
                    "fred": {"status": "ERROR", "rows": 0, "error": "timeout"},
                    "edgar": {"status": "MISSING", "rows": 0},
                    "stooq": {"status": "OK", "rows": 12},
                },
                "coverage": {"fred": 0.0, "edgar": 0.0, "stooq": 0.9},
            },
        )

    monkeypatch.setattr("octa.core.data.sources.altdata.sidecar.try_run", _sidecar_b)
    r2 = build_features(bars, settings, asset_class="stock", build_targets=False)
    assert list(r1.X.columns) == list(r2.X.columns)
    assert "altdat_source_fred_present" in r2.X.columns
    assert "altdat_source_edgar_present" in r2.X.columns
    assert "altdat_source_stooq_present" in r2.X.columns
