from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset


def load_artifact_df(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        cand = p / "data.parquet"
        if cand.exists():
            return pd.read_parquet(cand)
        raise FileNotFoundError(p)
    return pd.read_parquet(path)


def run_canary(reference_path: str, candidate_path: str) -> Dict[str, Any]:
    ref = load_artifact_df(reference_path)
    cand = load_artifact_df(candidate_path)
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref, current_data=cand)
    items = report.items()
    # Report.items() returns a list of snapshot objects; use dict() to serialize
    out = items[0].dict() if items else {}
    drift_share = float(out.get("drift_share", 0.0))
    # older/newer Evidently versions may not provide a single dataset_drift_score
    drift_score = float(out.get("dataset_drift_score", 0.0))

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
