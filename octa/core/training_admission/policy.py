from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import read_yaml


@dataclass(frozen=True)
class AdmissionPolicy:
    recycling_run_dir: str
    output_root: str
    evidence_root: str
    approval_registry_path: str | None
    prior_decisions_registry_path: str | None
    admitted_candidates_output: str
    rejected_candidates_output: str
    waiting_candidates_output: str
    quarantined_candidates_output: str
    require_explicit_approval: bool
    approval_scope_required: str
    allowed_asset_classes: list[str]
    allowed_source_families: list[str]
    blocked_providers: list[str]
    allowed_symbols: list[str]
    allowed_symbol_patterns: list[str]
    allowed_frequencies: list[str]
    min_coverage_rows: int
    min_confidence: float
    min_quality: float
    min_interpretability: float
    min_coverage_value: float
    max_open_risk_flags: int
    offline_only: bool
    decision_ttl_days: int
    conflict_behavior: str
    evidence_requirements: list[str]


def _list(raw: dict[str, Any], name: str, default: list[str]) -> list[str]:
    value = raw.get(name, default)
    return [str(item) for item in value]


def load_policy(path: Path) -> AdmissionPolicy:
    raw = read_yaml(path)
    return AdmissionPolicy(
        recycling_run_dir=str(raw.get("recycling_run_dir", "artifacts/parquet_recycling/latest")),
        output_root=str(raw.get("output_root", "artifacts/training_admission")),
        evidence_root=str(raw.get("evidence_root", "octa/var/evidence/training_admission")),
        approval_registry_path=str(raw["approval_registry_path"]) if raw.get("approval_registry_path") else None,
        prior_decisions_registry_path=str(raw["prior_decisions_registry_path"]) if raw.get("prior_decisions_registry_path") else None,
        admitted_candidates_output=str(raw.get("admitted_candidates_output", "admitted_offline_training_candidates.json")),
        rejected_candidates_output=str(raw.get("rejected_candidates_output", "rejected_training_candidates.json")),
        waiting_candidates_output=str(raw.get("waiting_candidates_output", "waiting_for_approval_candidates.json")),
        quarantined_candidates_output=str(raw.get("quarantined_candidates_output", "quarantined_training_candidates.json")),
        require_explicit_approval=bool(raw.get("require_explicit_approval", True)),
        approval_scope_required=str(raw.get("approval_scope_required", "offline_training_only")),
        allowed_asset_classes=_list(raw, "allowed_asset_classes", ["equities"]),
        allowed_source_families=_list(raw, "allowed_source_families", ["market_ohlcv"]),
        blocked_providers=_list(raw, "blocked_providers", []),
        allowed_symbols=_list(raw, "allowed_symbols", []),
        allowed_symbol_patterns=_list(raw, "allowed_symbol_patterns", []),
        allowed_frequencies=_list(raw, "allowed_frequencies", ["1D"]),
        min_coverage_rows=int(raw.get("min_coverage_rows", 256)),
        min_confidence=float(raw.get("min_confidence", 0.9)),
        min_quality=float(raw.get("min_quality", 0.9)),
        min_interpretability=float(raw.get("min_interpretability", 0.7)),
        min_coverage_value=float(raw.get("min_coverage_value", 0.02)),
        max_open_risk_flags=int(raw.get("max_open_risk_flags", 0)),
        offline_only=bool(raw.get("offline_only", True)),
        decision_ttl_days=int(raw.get("decision_ttl_days", 30)),
        conflict_behavior=str(raw.get("conflict_behavior", "quarantine")),
        evidence_requirements=_list(
            raw,
            "evidence_requirements",
            [
                "dataset_catalog.json",
                "classification_report.json",
                "validation_report.json",
                "routing_report.json",
                "roi_report.json",
            ],
        ),
    )
