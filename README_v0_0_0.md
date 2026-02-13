# OCTA v0.0.0 End-to-End

## 1) Preflight + Eligibility
```bash
python -m octa.support.ops.v000_foundation_e2e
```

Artifacts:
- `octa/var/evidence/<RUN_ID>/preflight/preflight_report.json`
- `octa/var/evidence/<RUN_ID>/eligibility/*`

## 2) Deterministic Repro Check
Run the same command; the flow performs two deterministic runs and writes:
- `octa/var/evidence/<RUN_ID>/repro/repro_run_1/*`
- `octa/var/evidence/<RUN_ID>/repro/repro_run_2/*`
- `octa/var/evidence/<RUN_ID>/repro/repro_diff.json`

## 3) Calibration Ladder (Tier1 -> Tier3 HF_NEAR)
Executed inside the same command under:
- `octa/var/evidence/<RUN_ID>/calibration/<asset_class>/tier1`
- `octa/var/evidence/<RUN_ID>/calibration/<asset_class>/tier2`
- `octa/var/evidence/<RUN_ID>/calibration/<asset_class>/tier3`

## 4) HF_LEVEL Tightening + Certification
Executed inside the same command under:
- `octa/var/evidence/<RUN_ID>/calibration/<asset_class>/hf_level_iter*`
- `octa/var/evidence/<RUN_ID>/certification/*`

## 5) Shadow Session End-to-End
Executed inside the same command under:
- `octa/var/evidence/<RUN_ID>/shadow/shadow_session.jsonl`
- `octa/var/evidence/<RUN_ID>/shadow/shadow_summary.json`
- `octa/var/evidence/<RUN_ID>/shadow/shadow_hash_chain.json`

Default execution behavior is SHADOW/DRY-RUN and no live order transmission.
