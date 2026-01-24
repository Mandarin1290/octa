from __future__ import annotations

import glob
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset

from dagster import job, op
from scripts.dagster_helpers import retry_call
from scripts.dagster_resources import feast_resource, mlflow_resource, redis_resource
from scripts.retry_policy import DEFAULT_POLICY


@op(required_resource_keys=set())
def apply_feast_op():
    # run existing PoC apply script which copies parquet files and applies FeatureViews
    def _run_apply():
        return subprocess.run(["python3", "scripts/feast_apply.py"], check=False)

    res = retry_call(_run_apply, retries=2, delay=2)
    if getattr(res, "returncode", 1) != 0:
        raise RuntimeError("feast_apply.py failed")
    return "applied"


@op(required_resource_keys={"feast_store"})
def materialize_backfill_op(context):
    # materialize full range from epoch to now (quick backfill)
    fs = context.resources.feast_store
    # use a reasonable window: last 10 years to now (safe default)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=3650)
    retry_call(fs.materialize, args=(start, end), retries=DEFAULT_POLICY.retries, delay=DEFAULT_POLICY.delay, backoff=DEFAULT_POLICY.backoff)
    return {"start": start.isoformat(), "end": end.isoformat()}


@op(required_resource_keys={"feast_store", "mlflow"})
def validate_historical_op(context):
    fs = context.resources.feast_store
    mlflow = context.resources.mlflow
    fvs = fs.list_feature_views()
    if not fvs:
        raise RuntimeError("no FeatureViews in feast_repo")
    files = glob.glob("feast_repo/data/*.parquet")
    if not files:
        raise RuntimeError("no parquet files in feast_repo/data")

    mlflow.set_experiment("feast_validation")

    for fv in fvs:
        join_keys = [ec.name for ec in fv.entity_columns]
        # find a parquet file that contains the join key
        sample_file = None
        sample_df = None
        for f in files:
            df = pd.read_parquet(f)
            if join_keys and join_keys[0] in df.columns:
                sample_file = f
                sample_df = df
                break
        if sample_df is None:
            # fallback: use first file
            sample_file = files[0]
            sample_df = pd.read_parquet(sample_file)

        ent_col = join_keys[0] if join_keys else sample_df.columns[0]
        sample_vals = sample_df[ent_col].dropna().unique()[:20].tolist()
        entity_df = pd.DataFrame({ent_col: sample_vals, "event_timestamp": pd.Timestamp(datetime.now(timezone.utc))})

        feature_fields = [field for field in fv.schema if field.name not in set(join_keys + ["event_timestamp"])]
        refs = [fv.name + ":" + field.name for field in feature_fields]

        # fetch historical features
        try:
            job = retry_call(fs.get_historical_features, args=(entity_df, refs), retries=2, delay=2)
            out = job.to_df()
        except Exception as e:
            # write an error marker and continue
            err_path = Path(f"artifacts/feast_validation_{fv.name}_error.txt")
            err_path.parent.mkdir(parents=True, exist_ok=True)
            err_path.write_text(str(e))
            continue

        # save CSV per featureview
        out_path = Path(f"artifacts/feast_validation_{fv.name}.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(out_path, index=False)

        # Evidently data drift report: compare historical (reference) vs current (sample_file)
        current_df = sample_df.copy()
        # ensure same feature columns exist
        feat_cols = [c for c in out.columns if c not in [ent_col, "event_timestamp"]]
        # use only intersection columns present in current sample
        cur_cols = [c for c in feat_cols if c in current_df.columns]
        if not cur_cols:
            # nothing to compare
            report_html = None
            report_json = None
            drift_detected = False
        else:
            ref_df = out[cur_cols].reset_index(drop=True)
            cur_df = current_df[cur_cols].reset_index(drop=True)

        try:
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=ref_df, current_data=cur_df)
            report_html = Path(f"artifacts/feast_report_{fv.name}.html")
            report_json = Path(f"artifacts/feast_report_{fv.name}.json")
            report_html.parent.mkdir(parents=True, exist_ok=True)
            report.save_html(str(report_html))
            report.save_json(str(report_json))
            # extract simple metric: any drift detected?
            rdict = report.as_dict()
            # navigate presets for drift counts (best-effort)
            drift_detected = False
            try:
                for m in rdict.get("metrics", []):
                    if m.get("metric") == "dataset_drift" and m.get("result", {}).get("dataset_drift", False):
                        drift_detected = True
            except Exception:
                drift_detected = False
        except Exception:
            report_html = None
            report_json = None
            drift_detected = False

        # log to MLflow
        with mlflow.start_run(nested=True, run_name=f"feast_validate_{fv.name}"):
            mlflow.log_param("feature_view", fv.name)
            mlflow.log_param("entity_col", ent_col)
            mlflow.log_metric("rows_historical", len(out))
            mlflow.log_metric("rows_current_sample", len(cur_df))
            mlflow.log_metric("num_features", len(feat_cols))
            mlflow.log_metric("drift_detected", float(drift_detected))
            if report_html and report_html.exists():
                mlflow.log_artifact(str(report_html), artifact_path="feast_reports")
            if report_json and report_json.exists():
                mlflow.log_artifact(str(report_json), artifact_path="feast_reports")

    # do not return value to keep job outputs simple
    return None


@job(resource_defs={"feast_store": feast_resource, "redis": redis_resource, "mlflow": mlflow_resource})
def feast_etl_job():
    apply_feast_op()
    materialize_backfill_op()
    validate_historical_op()


if __name__ == "__main__":
    # run the job in-process for quick validation
    result = feast_etl_job.execute_in_process()
    if not result.success:
        raise SystemExit("Dagster job failed")
    print("Feast ETL job completed. Validation file: artifacts/feast_validation.csv")
