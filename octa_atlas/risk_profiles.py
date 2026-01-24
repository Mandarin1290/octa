from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from octa_fabric.fingerprint import sha256_hexdigest

from .models import ArtifactMetadata, RiskProfile
from .registry import AtlasRegistry


def save_risk_profile(
    registry: AtlasRegistry,
    portfolio_id: str,
    version: str,
    result_bundle: Dict[str, Any],
) -> None:
    metadata = ArtifactMetadata(
        asset_id=portfolio_id,
        artifact_type="risk_profile",
        version=version,
        created_at=datetime.now(timezone.utc).isoformat(),
        dataset_hash="",
        training_window=result_bundle.get("window", ""),
        feature_spec_hash="",
        hyperparams={},
        metrics={"total_pnl": float(result_bundle.get("total_pnl", 0.0))},
        code_fingerprint=sha256_hexdigest(result_bundle),
        gate_status="COMPLETE",
    )
    rp = RiskProfile(profile=result_bundle)
    registry.save_artifact(portfolio_id, "risk_profile", version, rp, metadata)


__all__ = ["save_risk_profile"]
