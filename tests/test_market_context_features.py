from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from octa_training.core.features import build_features


@dataclass
class _Settings:
    features: dict
    raw_dir: Path
    timeframe: str

    def __post_init__(self) -> None:
        # build_features accesses some feature groups via direct attributes
        # (legacy compatibility), mirroring how the pipeline constructs eff_settings.
        for k, v in (self.features or {}).items():
            if isinstance(k, str):
                setattr(self, k, v)


def _make_ohlcv(n: int = 200, freq: str = "1h") -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n, freq=freq, tz="UTC")
    close = pd.Series(100 + np.cumsum(np.random.normal(0, 0.1, size=n)), index=idx)
    df = pd.DataFrame(
        {
            "open": close.shift(1).fillna(close.iloc[0]),
            "high": close * 1.001,
            "low": close * 0.999,
            "close": close,
            "volume": 1000.0,
        },
        index=idx,
    )
    return df


def test_market_context_enabled_but_missing_files_does_not_crash(tmp_path: Path):
    raw = _make_ohlcv()
    settings = _Settings(
        features={
            "horizons": [1],
            "macro": {"enabled": False},
            "market_context": {"enabled": True, "symbols": ["SPX"], "shift_bars": 1},
        },
        raw_dir=tmp_path,
        timeframe="1H",
    )

    res = build_features(raw, settings, asset_class="stock")
    assert not res.X.empty
    # No Indices_parquet present => no ctx columns, but should still run.
    assert not any(c.startswith("ctx_") for c in res.X.columns)


def test_market_context_loads_indices_parquet_when_present(tmp_path: Path):
    raw = _make_ohlcv()

    idx_dir = tmp_path / "Indices_parquet"
    idx_dir.mkdir(parents=True)

    # Minimal context parquet for SPX (index convention)
    ctx = raw[["close"]].copy().reset_index().rename(columns={"index": "timestamp"})
    ctx.to_parquet(idx_dir / "SPX_full_1hour.parquet", index=False)

    settings = _Settings(
        features={
            "horizons": [1],
            "macro": {"enabled": False},
            "market_context": {"enabled": True, "symbols": ["SPX"], "shift_bars": 1, "vol_window": 10, "corr_window": 20},
        },
        raw_dir=tmp_path,
        timeframe="1H",
    )

    res = build_features(raw, settings, asset_class="stock")
    assert any(c.startswith("ctx_SPX_") for c in res.X.columns)
    assert "ctx_SPX_ret1" in res.X.columns
