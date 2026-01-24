#!/usr/bin/env python3
"""Register saved artifacts in ModelRefreshManager and run a smoke retrain flow.

This script scans `artifacts/models/*/*/*/meta.json`, registers the model
with `ModelRefreshManager`, creates a retrain request, approves it and
executes a smoke retrain using the metrics from `meta.json`.
"""
import json
from pathlib import Path

from octa_ml.model_refresh import ModelRefreshManager


def load_summary_map(root: Path):
    p = root / "batch_training_summary.json"
    if not p.exists():
        return {}
    try:
        arr = json.loads(p.read_text())
    except Exception:
        return {}
    out = {}
    for item in arr:
        a = item.get("asset")
        if a:
            out[a] = item
    return out


def find_artifacts(root: Path):
    for meta in root.glob("*/**/meta.json"):
        try:
            meta_data = json.loads(meta.read_text())
        except Exception:
            continue
        # model_id is the immediate parent of the type directory (asset name)
        # path: artifacts/models/<asset>/<type>/<version>/meta.json
        parts = meta.parts
        # ensure at least: artifacts, models, asset, type, version, meta.json
        if len(parts) < 6:
            continue
        asset = parts[-4]
        version = parts[-2]
        yield asset, version, meta_data, meta.parent


def main():
    root = Path("artifacts/models")
    mgr = ModelRefreshManager()
    summary = []
    # prefer batch summary entries for MAE when meta.json lacks metrics
    batch_map = load_summary_map(Path("artifacts"))

    for asset, version, meta, _folder in find_artifacts(root):
        model_id = asset
        try:
            mgr.add_model(model_id, version)
        except Exception:
            # model might already be present; ignore
            pass

        # Create a retrain lifecycle: request -> approve -> execute
        try:
            mgr.request_retrain(model_id, trigger="auto-register", proposer="auto-script")
            mgr.approve_retrain(model_id, approver="auto-ci")
            # use metrics from meta if present; fallback to batch summary MAE if available
            metrics = meta.get("metrics", {}) if isinstance(meta, dict) else {}
            metrics = dict(metrics)
            if "mae" not in metrics:
                bm = batch_map.get(asset)
                if bm and bm.get("mae") is not None:
                    metrics["mae"] = bm.get("mae")

            new_version = f"{version}-smoke"
            evidence = mgr.execute_retrain(model_id, new_version, validate_metrics=metrics)
            summary.append({"asset": asset, "from": version, "to": new_version, "evidence": evidence, "metrics": metrics})
            print(f"Registered and executed retrain for {asset}: {version} -> {new_version}")
        except Exception as exc:
            print(f"Failed retrain for {asset}: {exc}")
            summary.append({"asset": asset, "error": str(exc)})

    out = Path("artifacts/retrain_run_summary.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2))
    print(f"Summary written to {out}")


if __name__ == "__main__":
    main()
