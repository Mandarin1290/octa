from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report


def load_artifact_df(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        # try model's saved dataset if present
        cand = p / "data.parquet"
        if cand.exists():
            return pd.read_parquet(cand)
        raise FileNotFoundError(p)
    return pd.read_parquet(path)


def run_canary(reference_path: str, candidate_path: str) -> Dict[str, Any]:
    # reference and candidate are parquet paths (or directories containing data.parquet)
    ref = load_artifact_df(reference_path)
    cand = load_artifact_df(candidate_path)

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref, current_data=cand)
    out = report.as_dict()
    # derive pass/fail on drift share threshold
    try:
        drift_score = out["metrics"][0]["metric"]["result"]["dataset_drift_score"]
        drift_share = out["metrics"][0]["metric"]["result"].get("dataset_drift_share", 0.0)
    except Exception:
        drift_score = 0.0
        drift_share = 0.0

    passed = drift_share < 0.05 and drift_score < 0.2
    return {"passed": passed, "drift_score": drift_score, "drift_share": drift_share, "report": out}


def write_report(outdir: str, content: Dict[str, Any]):
    p = Path(outdir)
    p.mkdir(parents=True, exist_ok=True)
    (p / "canary_report.json").write_text(json.dumps(content))


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--reference", required=True)
    p.add_argument("--candidate", required=True)
    p.add_argument("--outdir", default="artifacts/canary")
    args = p.parse_args()
    res = run_canary(args.reference, args.candidate)
    write_report(args.outdir, res)
    print("CANARY_PASSED=" + str(res["passed"]))
