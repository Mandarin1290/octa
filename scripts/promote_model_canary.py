"""Canary promotion helper: evaluate latest `feast_validation` runs and promote a registered MLflow model.

Usage:
    python3 scripts/promote_model_canary.py --model-name <MODEL_NAME> [--to-stage Production]

This script is conservative: it will promote only if recent validation runs report no detected drift.
"""
from __future__ import annotations

import argparse

from mlflow.tracking import MlflowClient


def latest_validation_metrics(experiment_name: str = "feast_validation", client: MlflowClient | None = None) -> list[dict]:
    client = client or MlflowClient()
    if hasattr(client, "search_experiments"):
        exps = [e for e in client.search_experiments() if e.name == experiment_name]
    else:
        exps = [e for e in client.list_experiments() if e.name == experiment_name]
    if not exps:
        print(f"Experiment not found: {experiment_name}")
        return []
    exp = exps[0]
    print(f"Experiment found: {experiment_name} ({exp.experiment_id})")
    runs = client.search_runs([exp.experiment_id], order_by=["attributes.start_time DESC"], max_results=50)
    print(f"Validation runs found: {len(runs)}")
    res = []
    for r in runs:
        metrics = r.data.metrics
        res.append(metrics)
    return res


def evaluate_and_promote(model_name: str, to_stage: str = "Staging"):
    client = MlflowClient()
    try:
        metrics = latest_validation_metrics(client=client)
    except Exception as e:
        print("Failed to read MLflow validation metrics:", e)
        return 6
    if not metrics:
        print("No validation runs found; skipping promotion.")
        return 1

    # Look for drift_detected metric across recent runs; require all to be 0.0
    drift_vals = []
    for m in metrics:
        if "drift_detected" in m:
            drift_vals.append(float(m["drift_detected"]))

    if not drift_vals:
        print("No drift_detected metrics found in recent validation runs; skipping promotion.")
        return 1

    print(f"Recent drift_detected values: {drift_vals}")
    if any(value != 0.0 for value in drift_vals):
        print("Drift detected in recent runs — not promoting.")
        return 2

    # promote latest model versions for the given model name
    try:
        versions = client.get_latest_versions(model_name)
    except Exception as e:
        print("Could not find registered model:", e)
        return 3

    if not versions:
        print("No registered versions for model", model_name)
        return 4

    for v in versions:
        print(f"Transitioning model {model_name} version {v.version} -> {to_stage}")
        try:
            client.transition_model_version_stage(name=model_name, version=v.version, stage=to_stage, archive_existing_versions=False)
        except Exception as e:
            print("Failed to transition version:", e)
            return 5
    print("Promotion complete.")
    return 0


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model-name", required=True)
    p.add_argument("--to-stage", default="Staging")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(evaluate_and_promote(args.model_name, args.to_stage))
