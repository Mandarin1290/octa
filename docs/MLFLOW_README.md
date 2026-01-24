MLflow Quickstart for Octa
-------------------------

Start the MLflow UI (runs stored in `mlruns/` by default):

```bash
# from repo root
mlflow ui --port 5000
```

Open http://localhost:5000 to view experiments, runs and registered models.

Notes:
- Scripts instrumented: `scripts/train_and_save.py`, `scripts/batch_train_assets.py`.
- The helper `scripts/mlflow_helper.py` is optional: if MLflow is not installed, scripts still run as no-ops.
- Registered model entries are best-effort; for production you'd want to log MLflow native model formats (e.g., `mlflow.sklearn.log_model` or `mlflow.pyfunc.log_model`) before registering.
