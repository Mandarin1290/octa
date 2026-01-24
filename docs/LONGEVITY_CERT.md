# Operational Longevity Certification

Purpose
-------
Generate an evidence-based certificate summarizing long-term operational readiness, stability, drift history, retired strategies and unresolved structural risks.

Report Contents
---------------
- stability metrics: short- and long-horizon stability summaries where available
- drift history: recent alerts and audit events relevant to operational degradation
- retired strategies: list of strategies retired with timestamps and evidence
- unresolved structural risks: pending retrain requests, critical warnings and other open items

Usage
-----
Call `generate_longevity_cert(...)` with available subsystem instances (optional):

```python
from octa_reports.longevity_cert import generate_longevity_cert
cert = generate_longevity_cert(stability_monitor=longrun_monitor, audit_engine=continuous_audit, sunset_engine=sunset_engine, cost_monitor=cost_monitor, regime_system=regime_system, model_refresh=model_refresh)
```

The function is defensive: missing components are tolerated and corresponding sections will be empty.

Validation
----------
Use `validate_cert(cert)` to recompute the canonical evidence hash and verify integrity.

Evidence
--------
Certificates include `evidence_hash` computed using canonical JSON (sorted keys, compact separators) and SHA‑256.
