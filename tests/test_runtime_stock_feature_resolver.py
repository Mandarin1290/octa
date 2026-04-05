from __future__ import annotations

from pathlib import Path

import pandas as pd

from octa.core.features.features import build_features
from octa_training.core.config import (
    canonical_training_altdata_config_path,
    load_config,
    resolve_feature_settings,
)


def _bars() -> pd.DataFrame:
    idx = pd.date_range("2024-01-02", periods=40, freq="B", tz="UTC")
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


def test_resolve_feature_settings_applies_stock_overlay() -> None:
    cfg = load_config()
    resolved = resolve_feature_settings(cfg, "stock")
    assert resolved.get("macro", {}).get("enabled") is True
    assert resolved.get("macro", {}).get("source") == "duckdb"


def test_load_config_sets_canonical_training_altdata_env(monkeypatch) -> None:
    monkeypatch.delenv("OKTA_ALTDATA_CONFIG", raising=False)
    load_config()
    assert Path(canonical_training_altdata_config_path()).resolve() == Path("config/altdat.yaml").resolve()


def test_build_features_dict_settings_uses_stock_runtime_overlay() -> None:
    cfg = load_config()
    res = build_features(_bars(), cfg.features, asset_class="stock", build_targets=False)
    macro_cols = [c for c in res.X.columns if c.startswith("macro_")]
    assert "macro_FEDFUNDS" in macro_cols
    assert "macro_DGS10" in macro_cols
    assert "macro_DGS2" in macro_cols
