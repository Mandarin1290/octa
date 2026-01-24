# OCTA Foundation (monorepo)

This repository is a production-oriented Python monorepo skeleton for OCTA.

Quickstart (local):

1. Create venv and activate

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

CI Canary and PR Checklist
-------------------------
We gate promotions with an Evidently canary run in CI. The container workflow is `.github/workflows/container_train_and_canary.yml`.
CI: build and push image
------------------------
To publish the trainer image on CI, set the following repository secrets: `REGISTRY_URL`, `REGISTRY_USERNAME`, `REGISTRY_PASSWORD`.
The workflow `.github/workflows/build_and_push_image.yml` will build `octa-trainer` and push tags `latest` and `${{ github.sha }}`.

Distributed training / autoscaling
---------------------------------
We provide a simple Ray scaffold (`scripts/ray_train.py`) and Kubernetes manifests (`k8s/trainer_deployment.yaml`, `k8s/trainer_hpa.yaml`) to run and autoscale training workloads. Deploy the `Deployment` and `HPA` into your cluster and replace the image with your registry image.


Checklist for PRs that introduce or change models:

- Include a reproducible sample Parquet under `tests/data` or point to an existing `artifacts/datasets/<asset>/...`.
- Ensure `scripts/train_and_save.py` run completes locally and produces artifacts.
- Run the canary locally with `scripts/evidently_canary_wrapper` and include the generated `artifacts/canary/canary_report.json` if you are opening a manual promotion request.
- If the canary fails, investigate feature drift, data schema changes, or retrain with adjusted preprocessing before promoting.


2. Run tests and linters

```bash
pytest -q
ruff check .
black --check .
mypy .
bandit -r .
```

This starter implements strict module boundaries, typed contracts, a central risk gate, and deterministic tooling.

FX sweep tooling
---------------

The script `scripts/global_gate_diagnose.py` can run an FX two-stage sweep:

- G0 (1D): risk/regime overlay only
- G1 (1H): alpha gate (only attempted if an explicit `*_1H.parquet` exists and is hourly-like)

Quarantine registry (FX G1)
--------------------------

When FX G1 data is detected as structurally invalid (e.g. non-hourly 1H parquet), the sweep writes a quarantine registry and emits NDJSON `type="quarantine"` events:

- `reports/fx_g1_quarantine_symbols.txt`
- `reports/fx_g1_quarantine_symbols.json`

To append to an existing quarantine registry (instead of overwrite), pass `--append-quarantine`.

Pass-finder mode (FX)
---------------------

`--fx-pass-finder` runs the FX two-stage sweep across multiple horizon-sets (without weakening gates) and emits pass-candidate events.

Structural-fail diagnostics
---------------------------

When FX fails with `FAIL_STRUCTURAL`, the sweep writes per-symbol diagnostic bundles:

- `reports/fx_structural_fail_diag/<run_id>/<SYMBOL>.json`

Summarize them with:

```bash
python scripts/summarize_fx_structural_fails.py --reports-dir reports
```

![coverage](https://img.shields.io/badge/coverage-80%25-yellow)
