from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import mlflow
import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset


def _load_sample(sample_file: str | None) -> pd.DataFrame:
    if sample_file:
        p = Path(sample_file)
        if not p.exists():
            raise FileNotFoundError(sample_file)
        if p.suffix in {".parquet", ".pq"}:
            return pd.read_parquet(p)
        else:
            return pd.read_csv(p)
    # fallback: pick first parquet from feast_repo/data
    p = Path("feast_repo/data")
    files = sorted(p.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("no sample file found and feast_repo/data empty")
    return pd.read_parquet(files[0])


def evaluate_and_maybe_promote(model_name: str, sample_file: str | None = None, promote: bool = True) -> int:
    # candidate = Staging, production = Production
    cand_stage = "Staging"
    prod_stage = "Production"

    # load models
    try:
        cand = mlflow.pyfunc.load_model(f"models:/{model_name}/{cand_stage}")
    except Exception as e:
        print("Could not load candidate model (Staging):", e)
        return 2
    try:
        prod = mlflow.pyfunc.load_model(f"models:/{model_name}/{prod_stage}")
    except Exception as e:
        print("Could not load production model (Production):", e)
        return 3

    df = _load_sample(sample_file)
    # try to select numeric columns only
    X = df.select_dtypes(include=["number"]).copy()
    if X.empty:
        # if no numeric, try all columns
        X = df.copy()

    # predict
    try:
        p_prod = prod.predict(X)
        p_cand = cand.predict(X)
    except Exception as e:
        print("Prediction failed:", e)
        return 4

    ref = pd.DataFrame({"prediction": p_prod})
    cur = pd.DataFrame({"prediction": p_cand})

    report = Report(metrics=[DataDriftPreset()])
    try:
        report.run(reference_data=ref, current_data=cur)
        rdict = report.as_dict()
        drift_detected = False
        for m in rdict.get("metrics", []):
            if m.get("metric") == "dataset_drift" and m.get("result", {}).get("dataset_drift", False):
                drift_detected = True
                break
    except Exception as e:
        print("Evidently report failed:", e)
        return 5

    print("Drift detected:", drift_detected)

    if drift_detected:
        print("Canary failed due to detected drift. Not promoting.")
        return 6

    if promote:
        # call promote script
        try:
            res = subprocess.run(["python3", "scripts/promote_model_canary.py", "--model-name", model_name, "--to-stage", prod_stage], check=False)
            return res.returncode or 0
        except Exception as e:
            print("Promotion call failed:", e)
            return 7

    return 0


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model-name", required=True)
    p.add_argument("--sample-file", default=None)
    p.add_argument("--no-promote", dest="promote", action="store_false")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(evaluate_and_maybe_promote(args.model_name, args.sample_file, args.promote))
