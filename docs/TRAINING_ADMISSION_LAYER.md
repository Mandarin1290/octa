# Training Admission Layer

## Meaning

The Training Admission Layer is an offline-only governance gate in front of OCTA offline training.
It consumes existing parquet recycling outputs and emits audited admission decisions.

It does:

- read recycling artifacts
- apply a stricter admission policy
- require explicit approval when configured
- write admission evidence and optional offline-training candidate lists

It does not:

- start training
- write model artifacts
- write paper or live registries
- promote anything to shadow, paper, or live
- modify execution or risk behavior

## Inputs

Required upstream artifacts from a recycling run directory:

- `dataset_catalog.json`
- `classification_report.json`
- `validation_report.json`
- `routing_report.json`
- `roi_report.json`

Optional governance inputs:

- `configs/training_admission_approvals.json`
- `configs/training_admission_prior_decisions.json`

## Decisions

- `admitted_for_offline_training`
- `rejected_for_training`
- `quarantined_for_manual_review`
- `waiting_for_explicit_approval`

The layer is fail-closed. If time semantics, duplicate scope, or prior decision history are unclear, the result is quarantine or rejection instead of admission.

## Approval Mechanism

Approvals are explicit, versioned JSON entries keyed by `dataset_identifier`.
An approval must match the required scope `offline_training_only`.
Good recycling scores never imply approval.

Example entry:

```json
{
  "dataset_identifier": "…",
  "action": "approve",
  "scope": "offline_training_only",
  "actor": "ops_user",
  "rationale": "ticket-approved",
  "evidence_ref": "CHG-1234",
  "approved_at": "2026-03-20T00:00:00Z"
}
```

## CLI

```bash
python -m octa training-admission --policy configs/training_admission_policy.yaml admission-full-run
```

Available subcommands:

- `admission-scan`
- `admission-decide`
- `admission-report`
- `admission-full-run`

## Outputs

Per run the layer writes:

- `admission_run_manifest.json`
- `admission_config_snapshot.json`
- `admission_input_manifest.json`
- `admission_decisions.json`
- `admission_quarantine_report.json`
- `admission_summary.md`
- `hashes.sha256`

Convenience outputs:

- `admitted_offline_training_candidates.json`
- `rejected_training_candidates.json`
- `waiting_for_approval_candidates.json`

## Safe Offline Reuse

The intended integration is manual and explicit:

1. run recycling
2. run training admission
3. inspect `admitted_offline_training_candidates.json`
4. manually derive a symbol file if offline training should be constrained to admitted symbols
5. launch the existing offline training path with that explicit symbol file

This preserves the separation between recycling, admission, training, and promotion.
