import types

from octa_training.core.asset_profiles import resolve_asset_profile


def test_profile_routing_by_dataset():
    cfg = types.SimpleNamespace(
        asset_defaults={
            "default_profile": "legacy",
            "by_dataset": {"stocks": "stock", "fx": "fx", "indices": "index"},
            "by_asset_class": {},
        },
        asset_profiles={
            "stock": {"name": "stock", "kind": "stock", "gates": {"global": {"sharpe_min": 0.1}}},
            "fx": {"name": "fx", "kind": "fx", "gates": {"global": {"sharpe_min": 0.2}}},
            "index": {"name": "index", "kind": "index", "gates": {"global": {"sharpe_min": 0.3}}},
        },
    )

    p = resolve_asset_profile(symbol="AAPL", dataset="stocks", asset_class="stock", parquet_path="/tmp/x.parquet", cfg=cfg)
    assert p.name == "stock"

    p = resolve_asset_profile(symbol="EURUSD", dataset="fx", asset_class="fx", parquet_path="/tmp/x.parquet", cfg=cfg)
    assert p.name == "fx"

    p = resolve_asset_profile(symbol="SPX", dataset="indices", asset_class="index", parquet_path="/tmp/x.parquet", cfg=cfg)
    assert p.name == "index"


def test_profile_routing_fallbacks_to_legacy_when_missing_profiles():
    cfg = types.SimpleNamespace(asset_defaults={}, asset_profiles={})
    p = resolve_asset_profile(symbol="X", dataset=None, asset_class=None, parquet_path=None, cfg=cfg)
    assert p.name == "legacy"
