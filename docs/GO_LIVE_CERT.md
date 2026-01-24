Go‑Live Certification (Evidence‑based)
===================================

Purpose
-------
Generate a final, auditable go‑live certification composed only from evidence sources. The certificate is deterministic and suitable for regulatory/operational sign‑off.

Hard Rules
----------
- Evidence‑based only: inputs should include an `evidence_hash` where available. The generator will fall back to hashing raw inputs but will note missing evidence.
- No marketing language: the output is a factual record of checks, state and open risks.

Contents
--------
- `capital_status`: snapshot of capital systems (liquidity, pending settlements, locks). Prefer dict with `evidence_hash`.
- `nav_validation`: results of NAV reconciliation and validation (include `evidence_hash`).
- `fee_validation`: validation of fee engine outputs and reconciled fee totals (include `evidence_hash`).
- `risk_governance`: summary of governance confirmations and control checks (include `evidence_hash`).
- `open_risks`: explicit list of residual risks, each with a short factual description and mitigating actions.

Output
------
The generated `GoLiveCert` contains a canonical `evidence_hash` that deterministically ties the certificate to all provided sources. Store this hash alongside audit evidence.

Usage
-----
1. Collect authoritative evidence from the systems (`octa_capital`, `octa_accounting`, `octa_reports`, governance checklists).
2. Call `generate_go_live_cert(system, capital_status, nav_validation, fee_validation, risk_governance, open_risks)`.
3. Persist the returned certificate JSON and evidence_hash in your immutable audit store.

Validation
----------
Use `validate_certificate(cert)` to verify structural completeness and that the certificate's `evidence_hash` matches the inputs.
