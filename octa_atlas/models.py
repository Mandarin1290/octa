from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict


@dataclass
class ArtifactMetadata:
    asset_id: str
    artifact_type: str
    version: str
    created_at: str
    dataset_hash: str
    training_window: str
    feature_spec_hash: str
    hyperparams: Dict[str, Any]
    metrics: Dict[str, float]
    code_fingerprint: str
    gate_status: str = "PENDING"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FeatureBundle:
    features: Dict[str, Any]


@dataclass
class ModelBundle:
    model_obj: Any


@dataclass
class RiskProfile:
    profile: Dict[str, Any]
