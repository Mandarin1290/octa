# Incident Classification & Severity Model

This document describes the formal incident taxonomy for OCTA operations.

Severity levels
- S0: informational
- S1: degraded performance
- S2: trading impairment
- S3: capital at risk
- S4: existential threat

Rules
- Every incident must have a `Severity` assigned. Recording an incident without severity is rejected.
- Severity deterministically maps to escalation and permission sets. No subjective scoring is used.
- No silent failures: operations must acknowledge and escalate according to severity.

Escalation and permissions
- Escalation and permission mappings are auditable and deterministic; see `IncidentManager.ESCALATION_RULES` and `IncidentManager.PERMISSIONS` in code.

Classification
- `IncidentManager.classify_from_impact(impact_score)` provides a deterministic mapping from measured impact to severity.

Usage
- Use `IncidentManager.record_incident(...)` to create incidents; inspect `escalation_for()` and `permissions_for()` for next steps.
