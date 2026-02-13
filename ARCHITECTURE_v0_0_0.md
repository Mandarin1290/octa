# OCTA v0.0.0 Architecture

## Boundaries
- Training path: `octa_training/**`
- Execution path: `octa/execution/**`, `octa_vertex/**`, `octa_nexus/**`
- Boundary policy: training must not import execution/broker adapters; execution must not import training modules.

## Determinism
- Fixed seeds (`42`) and fixed `global_end` anchor.
- Deterministic symbol ordering and tier selection.
- Deterministic gate calibration loops with hard iteration caps.

## Cost Model
- Canonical model: `octa/core/execution/costs/model.py`
- One fingerprint shared by training evaluation, shadow simulation, and certification.

## Risk and Execution Safety
- Fail-closed rule: any risk ERROR blocks order flow.
- Default mode is SHADOW/DRY-RUN.
- Shadow session records decision logs and hash-chain evidence.

## Calibration and Promotion
- Tier1/Tier2/Tier3 produce HF_NEAR intermediate profiles.
- Deterministic tightening yields HF_LEVEL final profiles.
- Certification always references HF_LEVEL.
- If any hard criterion fails, promotion is blocked with explicit reasons.

## Artifact Hygiene
- Calibration outputs are sandboxed under `octa/var/calibration/<RUN_ID>/`.
- Production guard rejects calibration-origin artifacts.
- Evidence under `octa/var/evidence/<RUN_ID>/` is append-only.
