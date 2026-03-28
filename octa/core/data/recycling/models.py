from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ScanIssue:
    code: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetRecord:
    file_path: str
    logical_dataset_name: str
    source_family: str
    provider: str
    asset_mapping_status: str
    inferred_asset_classes: list[str]
    inferred_symbols: list[str]
    inferred_regions: list[str]
    inferred_frequency: str
    time_coverage_start: str | None
    time_coverage_end: str | None
    row_count: int | None
    column_count: int | None
    schema_fingerprint: str
    file_hash_sha256: str
    partitioning_info: dict[str, Any]
    null_profile: dict[str, float]
    duplicate_profile: dict[str, Any]
    monotonicity: dict[str, Any]
    timezone_semantics: dict[str, Any]
    event_time_semantics: dict[str, Any]
    lookahead_risk_flags: list[str]
    leakage_risk_flags: list[str]
    quality_score: float
    freshness_score: float
    utility_score_preliminary: float
    governance_status: str
    lineage_parent_refs: list[str]
    confidence_score: float
    quarantine_reason: str | None
    issues: list[ScanIssue] = field(default_factory=list)


@dataclass(frozen=True)
class ClassificationDecision:
    file_path: str
    primary_role: str
    secondary_roles: list[str]
    classification_reasoning: list[str]
    classification_confidence: float
    blocking_flags: list[str]
    promotion_eligibility: str


@dataclass(frozen=True)
class RecycledFeatureArtifact:
    dataset_ref: str
    transform_id: str
    feature_name: str
    target_role: str
    config_hash: str
    lineage_hash: str
    feature_hash: str
    economic_rationale: str
    leakage_safe: bool
    values_preview: list[dict[str, Any]]


@dataclass(frozen=True)
class UtilityAssessment:
    file_path: str
    alpha_utility: float
    risk_utility: float
    regime_utility: float
    monitoring_utility: float
    simulation_utility: float
    research_utility: float
    compute_cost: float
    maintenance_cost: float
    coverage_value: float
    robustness_value: float
    interpretability: float
    governance_burden: float
    roi_composite_score: float
    roi_classification: str
    recommended_role: str
    recommended_zone: str
    recommendation: str


@dataclass(frozen=True)
class RoutingDecision:
    file_path: str
    route: str
    allowed: bool
    reason: str
    governance_status: str
    model_ready_whitelisted: bool
