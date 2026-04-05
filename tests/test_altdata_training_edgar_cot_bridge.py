from __future__ import annotations

from pathlib import Path

import pandas as pd

from octa.core.data.sources.altdata.cache import write_snapshot
from octa.core.features.transforms.feature_builder import build_altdata_features


def _bars() -> pd.DataFrame:
    idx = pd.date_range("2024-01-02", periods=20, freq="B", tz="UTC")
    px = pd.Series(range(len(idx)), index=idx, dtype=float) + 100.0
    return pd.DataFrame(
        {
            "open": px,
            "high": px + 1.0,
            "low": px - 1.0,
            "close": px + 0.5,
            "volume": 1000.0,
        },
        index=idx,
    )


def _cfg(tmp_path: Path) -> dict:
    return {
        "enabled": True,
        "offline_only": True,
        "strict_mode": False,
        "auto_install": False,
        "storage": {"root": str(tmp_path / "altdata")},
        "asof": {"tolerance_seconds": {"1D": 3888000}},
        "sources": {
            "fred": {"enabled": False},
            "edgar": {"enabled": True, "forms": ["10-K", "10-Q", "8-K"]},
            "cot": {
                "enabled": True,
                "targets": [
                    {"id": "es", "candidates": ["e-mini s&p 500"]},
                ],
            },
        },
        "weights": {"base": {"edgar": 0.5, "cot": 0.5}, "quality": {"min_coverage": 0.5}},
    }


def test_edgar_and_cot_missing_cache_emit_neutral_fallback_columns(tmp_path: Path) -> None:
    result = build_altdata_features(bars_df=_bars(), symbol="ABM", altdat_cfg=_cfg(tmp_path), tz="UTC")
    cols = list(result.features_df.columns)
    assert "altdat_edgar_event" in cols
    assert "altdat_edgar_10k" in cols
    assert "altdat_cot_risk_score" in cols
    assert "altdat_cot_net_position_es" in cols
    assert float(result.features_df["altdat_edgar_event"].sum()) == 0.0
    assert float(result.features_df["altdat_cot_net_position_es"].sum()) == 0.0


def test_edgar_and_cot_snapshots_build_real_feature_columns(tmp_path: Path) -> None:
    bars = _bars()
    cfg = _cfg(tmp_path)
    cache_root = str(tmp_path / "altdata")
    asof = bars.index[-1].date()

    write_snapshot(
        source="edgar",
        asof=asof,
        key_suffix="ABM",
        payload={
            "filings": [
                {"ticker": "ABM", "form": "10-K", "accepted_datetime": "2024-01-10T21:00:00+00:00"},
                {"ticker": "ABM", "form": "8-K", "accepted_datetime": "2024-01-16T21:00:00+00:00"},
            ]
        },
        meta={"seed": True},
        root=cache_root,
    )
    write_snapshot(
        source="cot",
        asof=asof,
        payload={
            "rows": [
                {
                    "market_id": "es",
                    "market_name": "E-MINI S&P 500",
                    "report_date": "2024-01-09",
                    "release_ts": "2024-01-12T20:00:00+00:00",
                    "noncommercial_long": 20000,
                    "noncommercial_short": 15000,
                    "open_interest": 100000,
                },
                {
                    "market_id": "es",
                    "market_name": "E-MINI S&P 500",
                    "report_date": "2024-01-16",
                    "release_ts": "2024-01-19T20:00:00+00:00",
                    "noncommercial_long": 25000,
                    "noncommercial_short": 14000,
                    "open_interest": 110000,
                },
            ]
        },
        meta={"seed": True},
        root=cache_root,
    )

    result = build_altdata_features(bars_df=bars, symbol="ABM", altdat_cfg=cfg, tz="UTC")
    cols = list(result.features_df.columns)
    assert "altdat_edgar_event" in cols
    assert "altdat_cot_net_position_es" in cols
    assert result.meta["sources"]["edgar"]["ok"] is True
    assert result.meta["sources"]["cot"]["ok"] is True
