#!/usr/bin/env python3
"""Run Evidently data drift report between two Parquet datasets.

Usage: python3 scripts/evidently_drift.py --reference path/to/ref.parquet --current path/to/curr.parquet --out-dir artifacts/drift_reports/<asset>/<version>
Produces `report.html` and `report.json` in the out dir and prints a small summary.
"""
import argparse
import json
from pathlib import Path

import pandas as pd
from evidently.legacy.metric_preset import DataDriftPreset
from evidently.legacy.report.report import Report


def run_drift(ref_path: Path, curr_path: Path, out_dir: Path) -> dict:
    ref_df = pd.read_parquet(ref_path)
    curr_df = pd.read_parquet(curr_path)

    report = Report(metrics=[DataDriftPreset()])
    try:
        report.run(reference_data=ref_df, current_data=curr_df)
    except Exception as e:
        return {"error": str(e)}

    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "report.html"
    json_path = out_dir / "report.json"
    report.save_html(str(html_path))
    # export metrics dict
    try:
        content = report.as_dict()
        json_path.write_text(json.dumps(content, indent=2))
    except Exception:
        content = {"note": "could not serialize report.as_dict()"}

    # derive a simple pass/fail: if any drift_share > 0.2 for numerical features -> fail
    drift_summary = {"drift_detected": False, "num_features_with_drift": 0}
    try:
        for stat in content.get("metrics", []):
            if stat.get("metric", {}).get("name") == "DatasetDriftTable":
                # the DatasetDriftTable contains drift_by_columns table under result
                res = stat.get("result", {})
                cols = res.get("data", {}).get("columns", [])
                count = 0
                for c in cols:
                    if c.get("drift_share") and c.get("drift_share") > 0.2:
                        count += 1
                drift_summary["num_features_with_drift"] = count
                drift_summary["drift_detected"] = count > 0
                break
    except Exception:
        pass

    out = {"report_html": str(html_path), "report_json": str(json_path), "drift_summary": drift_summary}
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--reference", required=True)
    p.add_argument("--current", required=True)
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()
    res = run_drift(Path(args.reference), Path(args.current), Path(args.out_dir))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
