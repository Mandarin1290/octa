from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AdmissionApproval:
    dataset_identifier: str
    action: str
    scope: str
    actor: str
    rationale: str
    evidence_ref: str
    approved_at: str
    expires_at: str | None = None


@dataclass(frozen=True)
class PriorDecision:
    dataset_identifier: str
    decision: str
    upstream_recycling_run: str | None
    upstream_dataset_ref: str | None
    decided_at: str
    expires_at: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class AdmissionCandidate:
    file_path: str
    dataset_identifier: str
    logical_dataset_name: str
    symbol: str
    asset_class: str
    source_family: str
    provider: str
    frequency: str
    governance_status: str
    time_coverage_start: str | None
    time_coverage_end: str | None
    row_count: int | None
    quality_score: float
    confidence_score: float
    lookahead_risk_flags: list[str]
    leakage_risk_flags: list[str]
    issue_codes: list[str]
    max_severity: str
    blocking_flags: list[str]
    promotion_eligibility: str
    routing_decision: str
    routing_allowed: bool
    routing_reason: str
    roi_recommendation: str
    interpretability: float
    coverage_value: float
    upstream_run_dir: str
    upstream_references: dict[str, Any]


@dataclass(frozen=True)
class AdmissionDecision:
    dataset_identifier: str
    symbol: str
    asset_class: str
    source_family: str
    provider: str
    frequency: str
    upstream_recycling_decision_reference: dict[str, Any]
    admission_decision: str
    reasons: list[str]
    blocking_flags: list[str]
    policy_checks: dict[str, dict[str, Any]]
    approval_status: dict[str, Any]
    evidence_references: list[str]
    timestamp: str
    run_id: str
    confidence_snapshot: dict[str, float]
    no_auto_promotion: bool
    offline_scope_only: bool
    decision_expires_at: str | None


@dataclass(frozen=True)
class AdmissionQuarantineRow:
    dataset_identifier: str
    symbol: str
    reasons: list[str]
    blocking_flags: list[str]
    evidence_references: list[str]


@dataclass
class DecisionContext:
    candidate: AdmissionCandidate
    policy_checks: dict[str, dict[str, Any]] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    blocking_flags: list[str] = field(default_factory=list)
