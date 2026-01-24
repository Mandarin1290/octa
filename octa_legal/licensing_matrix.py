from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List


@dataclass(frozen=True)
class LicensingMode:
    key: str
    title: str
    description: str
    source_access: str  # none | limited | full
    allow_forks: bool
    commercial_use: bool
    redistribution: str  # none | internal | licensed
    support_model: str
    restrictions: List[str]


def get_modes() -> Dict[str, LicensingMode]:
    """Return canonical licensing/deployment modes with constraints.

    Hard rules enforced by design:
    - Core system integrity preserved: every mode enforces evidence, audit, and signed manifests where needed.
    - No uncontrolled forks: modes that permit source access require contractual controls and code escrow/signing.
    """
    modes: Dict[str, LicensingMode] = {}

    modes["internal_fund_only"] = LicensingMode(
        key="internal_fund_only",
        title="Internal Fund Only",
        description="Deploy within a single internal fund/organization. No external redistribution.",
        source_access="limited",
        allow_forks=False,
        commercial_use=False,
        redistribution="none",
        support_model="internal",
        restrictions=[
            "no external redistribution",
            "audit_evidence_must_be_retained",
            "internal_governance_controls",
        ],
    )

    modes["licensed_engine"] = LicensingMode(
        key="licensed_engine",
        title="Licensed Engine",
        description="License the engine to third parties under contract. Source access limited to obfuscated or audited bundles.",
        source_access="limited",
        allow_forks=False,
        commercial_use=True,
        redistribution="licensed",
        support_model="paid_support",
        restrictions=[
            "contractual_no_forks",
            "signed_release_artifacts",
            "evidence_hashes_published",
        ],
    )

    modes["managed_service"] = LicensingMode(
        key="managed_service",
        title="Managed Service",
        description="Operator runs the service; customers access via API. No customer code drop-in.",
        source_access="none",
        allow_forks=False,
        commercial_use=True,
        redistribution="none",
        support_model="svc_sla",
        restrictions=[
            "no_customer_deployment",
            "api_access_only",
            "monitoring_and_audit_sharing_options",
        ],
    )

    modes["white_label_restricted"] = LicensingMode(
        key="white_label_restricted",
        title="White‑Label (Restricted)",
        description="Customer receives deployed or containerized product with branding options but under strict controls.",
        source_access="none",
        allow_forks=False,
        commercial_use=True,
        redistribution="licensed",
        support_model="managed_or_shared",
        restrictions=[
            "no_source_redistribution",
            "watermarking_and_evidence_reporting",
            "escrow_for_critical_components",
        ],
    )

    return modes


def licensing_matrix() -> Dict[str, Dict]:
    """Return a serializable matrix describing permitted operations per mode."""
    modes = get_modes()
    matrix: Dict[str, Dict] = {}
    for k, m in modes.items():
        matrix[k] = asdict(m)
    # Add enforcement recommendations
    matrix["_enforcement"] = {
        "core_integrity": [
            "distribute only signed release artifacts (signed evidence_hash)",
            "retain append-only audit logs and publish snapshots to auditors",
            "include runtime attestation and remote integrity checks for managed deployments",
        ],
        "no_uncontrolled_forks": [
            "contractual no-fork clauses for licensed deployments",
            "code escrow with release triggers under dispute",
            "technical watermarking and provenance metadata embedded in artifacts",
        ],
    }
    return matrix


def assess_request(mode_key: str, requested_actions: List[str]) -> Dict[str, object]:
    """Assess whether a set of requested actions is compatible with a deployment mode.

    Returns a dict with `allowed` bool and `reasons` list explaining rejections.
    This function is a guidance helper — contractual/legal review still required.
    """
    modes = get_modes()
    if mode_key not in modes:
        return {"allowed": False, "reasons": [f"Unknown mode: {mode_key}"]}
    mode = modes[mode_key]
    reasons: List[str] = []
    allowed = True

    for action in requested_actions:
        if action == "redistribute_source" and mode.source_access != "full":
            allowed = False
            reasons.append("source redistribution not permitted under this mode")
        if action == "run_as_service" and mode.key == "internal_fund_only":
            # internal fund may run as service internally but not externally
            reasons.append("service operation allowed only internally")
        if action == "fork" and not mode.allow_forks:
            allowed = False
            reasons.append("forks are disallowed to prevent uncontrolled forks")

    return {"allowed": allowed, "reasons": reasons, "mode": asdict(mode)}
