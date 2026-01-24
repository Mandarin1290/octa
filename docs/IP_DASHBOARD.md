# IP Governance Dashboard

The `IPDashboard` aggregates deterministic, auditable metrics about project IP.

Outputs
- `ip_categories`: mapping module -> classification string (deterministic sorted keys).
- `policy_violations`: list of structured violations from `PolicyEnforcer.check_policies()`.
- `valuation_metadata`: per-module valuation metadata from `ValuationEngine`.
- `risk_critical_components`: modules ordered by `risk_criticality` descending (deterministic tie-breaker).
- `externalizable_modules`: list of modules that the `ExternalizationScanner` marks `ready`.

Design
- No subjective scoring; all metrics are derived from observable inputs (module map, usage counts, revenue map).
- Deterministic ordering is enforced by sorting keys and tie-breaking by module name.
- For auditability, preserve the underlying inputs and use `ip_lineage` to snapshot metric outputs.

Usage
```
from octa_reports.ip_dashboard import IPDashboard
dashboard = IPDashboard(module_map, classifier, enforcer, scanner, valuation_engine)
summary = dashboard.summary(usage_counts=my_usage, revenue_map=my_revenue)
```
