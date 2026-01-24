# Production Runbook (concise)

This runbook describes operator steps to train, validate, and promote models.

1) Local quick training

```bash
PYTHONPATH=. python3 scripts/train_and_save.py --parquet my_sample.parquet --target target --version v1 --seed 42 --cv-folds 5 --backtest
```

2) Run hyperparameter search (automated with `--hyperopt` flag).

3) CI: push branch to trigger `.github/workflows/train_and_ci.yml`. The workflow:
   - builds sample data, runs trainer, uploads artifacts
   - runs the Evidently canary gate to compare the candidate dataset/model against a reference and fail the job on drift.
     The workflow file used for containerized runs is `.github/workflows/container_train_and_canary.yml`.

CI Canary behavior
------------------
- The CI job builds `octa-trainer:latest` and runs the trainer producing artifacts under `artifacts/models/demo_model/regression/<run-id>`.
- After training, CI runs the canary wrapper `scripts/evidently_canary_wrapper` which writes `artifacts/canary/canary_report.json`.
- The workflow asserts `passed == true` in the report; if false, the job fails and promotion is blocked.

Local reproducer
-----------------
Run the steps locally to reproduce the CI behavior:

```bash
docker build -t octa-trainer:local .
docker run --rm -v $PWD:/workspace -w /workspace octa-trainer:local --parquet tests/data/sample_parquet.parquet --target target --version local-ci --cv-folds 3 --hyperopt --backtest
docker run --rm --entrypoint python3 -v $PWD:/workspace -w /workspace octa-trainer:local -m scripts.evidently_canary_wrapper --reference artifacts/models/demo_model/regression/test-run-1 --candidate artifacts/models/demo_model/regression/local-ci --outdir artifacts/canary
cat artifacts/canary/canary_report.json
```

4) To publish to Hugging Face (manual):

```bash
export HF_TOKEN=...  # set secret
python3 scripts/hf_upload.py --model-file artifacts/models/demo_model/regression/v1/model.pkl --repo-id username/demo_model
```

5) Promotion: run the canary promotion script when gate passes (CI or manually):

```bash
python3 scripts/canary_eval_and_promote.py --model-name demo_model --to-stage Production
```

6) Rolling back: use MLflow to transition previous version back to Production.

Notes:
- GPG signing is optional; set `GPG_KEY` env var and ensure gpg private key available if you want signatures.
- The repo contains scaffolds for pyfunc logging, CV, backtesting and model cards.
