from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def normalize_policy_asset_class(asset_class: Optional[str]) -> str:
    label = str(asset_class or "").strip().lower()
    aliases = {
        "equity": "stock",
        "equities": "stock",
        "stocks": "stock",
        "etfs": "etf",
        "forex": "fx",
        "futures": "future",
        "indices": "index",
        "options": "option",
    }
    return aliases.get(label, label or "unknown")


@dataclass(frozen=True)
class TrainingPolicySpec:
    name: str
    asset_class: str
    universe_policy: str
    feature_policy: str
    target_policy: str
    validation_policy: str
    model_policy: str
    tuning_policy: str
    gate_overlay_policy: str
    altdata_policy: str
    packaging_provenance_policy: str
    prototype_enabled: bool = False


_REGISTRY: Dict[str, TrainingPolicySpec] = {
    "stock": TrainingPolicySpec(
        name="StockTrainingPolicy",
        asset_class="stock",
        universe_policy="stock_universe_only",
        feature_policy="shared_features_with_stock_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="stock_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
        prototype_enabled=True,
    ),
    "etf": TrainingPolicySpec(
        name="ETFTrainingPolicy",
        asset_class="etf",
        universe_policy="etf_universe_only",
        feature_policy="shared_features_with_etf_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="etf_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
    ),
    "fx": TrainingPolicySpec(
        name="FXTrainingPolicy",
        asset_class="fx",
        universe_policy="fx_universe_only",
        feature_policy="shared_features_with_fx_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="fx_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
    ),
    "future": TrainingPolicySpec(
        name="FuturesTrainingPolicy",
        asset_class="future",
        universe_policy="futures_universe_only",
        feature_policy="shared_features_with_futures_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="futures_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
    ),
    "index": TrainingPolicySpec(
        name="IndexTrainingPolicy",
        asset_class="index",
        universe_policy="index_universe_only",
        feature_policy="shared_features_with_index_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="index_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
    ),
    "crypto": TrainingPolicySpec(
        name="CryptoTrainingPolicy",
        asset_class="crypto",
        universe_policy="crypto_universe_only",
        feature_policy="shared_features_with_crypto_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="crypto_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
    ),
    "option": TrainingPolicySpec(
        name="OptionsTrainingPolicy",
        asset_class="option",
        universe_policy="options_universe_only",
        feature_policy="shared_features_with_options_hooks",
        target_policy="shared_target_hook",
        validation_policy="shared_validation_hook",
        model_policy="shared_model_hook",
        tuning_policy="shared_tuning_hook",
        gate_overlay_policy="options_gate_overlay",
        altdata_policy="shared_altdata_hook",
        packaging_provenance_policy="explicit_policy_provenance",
    ),
}


def get_training_policy_registry() -> Dict[str, TrainingPolicySpec]:
    return dict(_REGISTRY)


def resolve_training_policy(asset_class: Optional[str]) -> Optional[TrainingPolicySpec]:
    return _REGISTRY.get(normalize_policy_asset_class(asset_class))


def resolve_training_policy_details(asset_class: Optional[str]) -> Dict[str, Any]:
    spec = resolve_training_policy(asset_class)
    if spec is None:
        return {
            "policy_name": None,
            "policy_asset_class": normalize_policy_asset_class(asset_class),
            "policy_source": "unresolved",
            "prototype_enabled": False,
        }
    return {
        "policy_name": spec.name,
        "policy_asset_class": spec.asset_class,
        "policy_source": "asset_class",
        "prototype_enabled": bool(spec.prototype_enabled),
        "control_surfaces": {
            "universe_policy": spec.universe_policy,
            "feature_policy": spec.feature_policy,
            "target_policy": spec.target_policy,
            "validation_policy": spec.validation_policy,
            "model_policy": spec.model_policy,
            "tuning_policy": spec.tuning_policy,
            "gate_overlay_policy": spec.gate_overlay_policy,
            "altdata_policy": spec.altdata_policy,
            "packaging_provenance_policy": spec.packaging_provenance_policy,
        },
    }


def resolve_active_prototype_policy(cfg: Any) -> Optional[TrainingPolicySpec]:
    raw_training_policy = getattr(cfg, "training_policy", None)
    if hasattr(raw_training_policy, "dict"):
        raw_training_policy = raw_training_policy.dict()
    if not isinstance(raw_training_policy, dict):
        return None
    prototype = raw_training_policy.get("prototype") or {}
    if hasattr(prototype, "dict"):
        prototype = prototype.dict()
    if not isinstance(prototype, dict):
        return None
    if not bool(prototype.get("enabled", False)):
        return None
    active_name = normalize_policy_asset_class(prototype.get("active_policy"))
    return _REGISTRY.get(active_name)


def prototype_allowed_asset_classes(cfg: Any) -> Optional[Tuple[str, ...]]:
    active = resolve_active_prototype_policy(cfg)
    if active is None:
        return None
    policy_asset = str(active.asset_class)
    aliases = {
        "stock": ("stock", "equities"),
        "etf": ("etf", "etfs"),
        "fx": ("fx", "forex"),
        "future": ("future", "futures"),
        "index": ("index", "indices"),
        "crypto": ("crypto",),
        "option": ("option", "options"),
    }
    return tuple(aliases.get(policy_asset, (policy_asset,)))
