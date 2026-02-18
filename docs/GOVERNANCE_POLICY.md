# OCTA Governance Policy

Version: 1.0.0
Effective: 2026-02-18
Last reviewed: 2026-02-18

---

## 1. Scope

This policy applies to all operations of the OCTA quantitative trading platform, including model training, execution, capital management, and key management.

## 2. Personal vs Corporate Funds

- All capital tracked in the deterministic capital ledger (`octa/accounting/capital_ledger.py`).
- Every capital movement is classified as one of: `shareholder_loan_in`, `shareholder_equity_in`, `broker_funding_in`, `broker_funding_out`, `fees`, `pnl_realized`, `pnl_unrealized_snapshot`.
- Personal funds injected as shareholder loans MUST be recorded with explicit loan terms in metadata.
- Corporate equity contributions MUST be recorded as `shareholder_equity_in`.
- The ledger is append-only and hash-chained; tampering is detectable via `CapitalLedger.verify()`.
- Reconciliation is available via: `python -m octa.accounting.ops.reconcile --asof YYYY-MM-DD`.

## 3. LEI / EMIR Enforcement

### 3.1 Legal Entity Identifier (LEI)
- A valid, ACTIVE LEI is **required** for all derivatives trading (options, futures, swaps, FX forwards, CFDs).
- The LEI registry is maintained at a configured JSON path and checked at execution preflight.
- Missing, expired, or lapsed LEI status results in **BLOCK** for derivatives.
- Equities are exempt from LEI requirements.

### 3.2 EMIR Compliance
- Derivatives trading requires documented EMIR delegation status for:
  - Clearing obligation (delegated or self-cleared)
  - Reporting obligation (delegated or self-reported)
  - Risk mitigation techniques
- Unknown or missing delegation status results in **BLOCK** for derivatives.
- EMIR configuration is maintained at a configured JSON path.

### 3.3 Combined Gate
- Both LEI **and** EMIR checks must pass for derivatives.
- The gate is implemented in `octa/core/governance/derivatives_gate.py`.

## 4. Model Promotion Policy

### 4.1 Approved-Only Execution
- Execution loads models **only** from `octa/var/models/approved/<SYMBOL>/<TF>/`.
- Every approved model must have:
  - `model.cbm` (or equivalent artifact)
  - `model.cbm.sha256` (hex digest sidecar)
  - `model.cbm.sig` (Ed25519 signature sidecar)
  - `manifest.json` (promotion metadata)

### 4.2 Promotion Process
- Candidate models are promoted via: `python -m octa.models.ops.promote`.
- Promotion requires a valid Ed25519 signing key.
- A `MODEL_PROMOTED` governance event is emitted to the hash chain.
- Gate thresholds (Sharpe, drawdown, trade count, CV, MC) must be met before promotion.

### 4.3 Verification at Load
- The execution loader verifies SHA-256 and Ed25519 signature on every model load.
- **Fail-closed**: invalid signature or missing sidecar results in load rejection.

## 5. Portfolio Risk Overlay

### 5.1 Pre-Flight Checks
All portfolio positions pass through a deterministic pre-flight overlay before order submission:

| Check | Default Threshold | Fail Behaviour |
|-------|------------------|----------------|
| Per-symbol exposure | 10% of NAV | BLOCK symbol |
| Gross exposure | 150% of NAV | BLOCK all |
| Net exposure | 100% of NAV | BLOCK all |
| Pairwise correlation | 0.85 | BLOCK all |
| Tail risk (CVaR 95%) | 5% | BLOCK all |

### 5.2 Fail-Closed Conditions
- Unknown exposure (NAV invalid): **BLOCK**
- Unknown correlation (missing return data for multi-position): **BLOCK**
- Unknown tail risk (insufficient data for multi-position): **BLOCK**

### 5.3 Governance Event
- A `PORTFOLIO_PREFLIGHT` event is emitted after each pre-flight check.

## 6. Key Rotation Policy

### 6.1 Keystore Layout
```
octa/var/keys/
  active_signing_key     # current Ed25519 private key
  active_verify_key      # current Ed25519 public key
  previous_keys/         # archived keys
    <key_id>.key         # private (deleted on revocation)
    <key_id>.pub         # public (kept for audit)
  revocation_list.json   # revoked key IDs
```

### 6.2 Rotation Schedule
- Keys SHOULD be rotated at minimum every 90 days.
- Rotation is performed via `Keystore.rotate()`.
- Each rotation archives the current key and generates a new one.
- A `KEY_ROTATED` governance event is emitted.

### 6.3 Revocation
- Compromised or deprecated keys are revoked via `Keystore.revoke(key_id)`.
- Revocation deletes the private key and adds to `revocation_list.json`.
- A `KEY_REVOKED` governance event is emitted.
- Signatures made with revoked keys are considered **invalid** for new verifications.
- Execution must check `Keystore.verify_not_revoked()` before trusting a signature.

## 7. Retention Rules

### 7.1 Evidence
- All evidence under `octa/var/evidence/` is **append-only**.
- Evidence directories MUST NOT be deleted or modified.
- Minimum retention: 7 years (regulatory requirement for financial records).

### 7.2 Hash Chains
- Governance hash chains under `octa/var/evidence/governance_hash_chain/` are immutable.
- Chain integrity can be verified via `GovernanceAudit.verify()`.

### 7.3 Capital Ledger
- The capital ledger is append-only and hash-chained.
- Journal entries MUST NOT be deleted or modified.
- Corrections are made by appending a new entry with the correction details.

### 7.4 Model Artifacts
- Approved models are retained as long as they may be referenced.
- Archived signing keys are retained (pub only after revocation) for verification.

## 8. Incident Handling

### 8.1 Risk Incidents
- Any exception or invalid return from the risk engine triggers a fail-closed block.
- Incidents are written to `octa/var/evidence/<run>/risk_incidents/` with SHA-256 sidecars.
- Incident records include: timestamp, strategy, symbol, cycle, error details.

### 8.2 Governance Incidents
- All governance events are recorded in the hash-chained audit trail.
- The following events are tracked:
  - `EXECUTION_PREFLIGHT` — execution startup checks
  - `SIGNING_CONFIGURED` — signing key initialization
  - `MODEL_PROMOTED` — model promotion to approved
  - `MODEL_LOAD_VERIFIED` / `MODEL_LOAD_REJECTED` — load-time verification
  - `PORTFOLIO_PREFLIGHT` — portfolio risk check results
  - `GOVERNANCE_ENFORCED` — LEI/EMIR enforcement actions
  - `LEDGER_UPDATED` — capital ledger reconciliation
  - `KEY_ROTATED` / `KEY_REVOKED` — key lifecycle events
  - `POLICY_UPDATED` — policy document changes

### 8.3 Escalation
- SEVERE data sanitization flags require manual review before trading.
- Failed model signature verification requires key audit and potential re-signing.
- LEI expiry within 30 days triggers a WARNING and escalation to compliance.

## 9. Audit Trail

All governance actions are recorded in a deterministic, hash-chained audit trail.
The chain is stored under `octa/var/evidence/governance_hash_chain/<run_id>/chain.jsonl`.
Each record contains:
- Sequential index
- UTC timestamp
- Previous record hash (GENESIS for first)
- Event type and payload
- SHA-256 hash of the complete record

Chain integrity can be verified at any time via `GovernanceAudit.verify()`.

---

*This document is version-controlled and any changes emit a `POLICY_UPDATED` governance event.*
