from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from octa.core.data.recycling.cli import main
from octa.core.data.recycling.engine import inventory_datasets
from octa.core.data.recycling.policy import load_policy


def _write_good_market_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    idx = pd.date_range("2024-01-01", periods=240, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": idx,
            "open": range(240),
            "high": [x + 1 for x in range(240)],
            "low": range(240),
            "close": [x + 0.5 for x in range(240)],
            "volume": [1000.0] * 240,
        }
    )
    df.to_parquet(path, index=False)


def _write_bad_no_time_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"close": [1.0, 2.0, 3.0], "value": [10, 11, 12]})
    df.to_parquet(path, index=False)


def _write_altdata_publish_leak_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    idx = pd.date_range("2024-01-01", periods=180, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": idx,
            "publish_time": idx - pd.Timedelta(days=2),
            "event_time": idx,
            "sentiment_score": [0.1] * 180,
        }
    )
    df.to_parquet(path, index=False)


def _write_policy(path: Path, root: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "raw_roots:",
                f"  - {root / 'raw'}",
                "output_root: artifacts/parquet_recycling",
                f"evidence_root: {root / 'evidence'}",
                "curated_zone_root: artifacts/parquet_recycling/curated",
                "feature_zone_root: artifacts/parquet_recycling/recycled_features",
                "risk_zone_root: artifacts/parquet_recycling/risk_monitoring",
                "simulation_zone_root: artifacts/parquet_recycling/simulation",
                "quarantine_zone_root: artifacts/parquet_recycling/quarantine",
                "quality_minimum: 0.55",
                "confidence_minimum: 0.60",
                "model_ready_minimum: 0.85",
                "coverage_minimum_rows: 64",
                "freshness_days_soft_limit: 5000",
                "near_duplicate_ratio: 0.98",
                "max_null_fraction: 0.25",
                "max_duplicate_fraction: 0.02",
                "fail_closed_on_unknown_asset: true",
                "default_region: global",
                "default_provider: unknown",
                "default_source_family: unknown",
            ]
        ),
        encoding="utf-8",
    )


def test_full_run_writes_required_evidence_and_routes_fail_closed(tmp_path: Path) -> None:
    _write_good_market_parquet(tmp_path / "raw" / "Stock_parquet" / "AAPL_1D.parquet")
    _write_bad_no_time_parquet(tmp_path / "raw" / "Mystery_parquet" / "BAD_1D.parquet")
    _write_altdata_publish_leak_parquet(tmp_path / "raw" / "data_vendor" / "macro_sentiment_1D.parquet")
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path)

    rc = main(["--policy", str(policy), "--run-id", "test_run_001", "full-run"])
    assert rc == 0

    run_dir = tmp_path / "evidence" / "test_run_001"
    required = {
        "run_manifest.json",
        "config_snapshot.json",
        "environment_snapshot.json",
        "git_snapshot.txt",
        "input_manifest.json",
        "dataset_catalog.json",
        "classification_report.json",
        "validation_report.json",
        "recycling_report.json",
        "routing_report.json",
        "roi_report.json",
        "quarantine_report.json",
        "summary.md",
        "hashes.sha256",
    }
    assert required.issubset({p.name for p in run_dir.iterdir()})

    catalog = json.loads((run_dir / "dataset_catalog.json").read_text(encoding="utf-8"))
    by_name = {Path(row["file_path"]).name: row for row in catalog}
    assert by_name["AAPL_1D.parquet"]["governance_status"] == "candidate"
    assert by_name["BAD_1D.parquet"]["governance_status"] == "quarantined"

    classifications = json.loads((run_dir / "classification_report.json").read_text(encoding="utf-8"))
    class_by_name = {Path(row["file_path"]).name: row for row in classifications}
    assert class_by_name["AAPL_1D.parquet"]["primary_role"] == "simulation_candidate"
    assert class_by_name["BAD_1D.parquet"]["primary_role"] == "quarantine_candidate"

    routing = json.loads((run_dir / "routing_report.json").read_text(encoding="utf-8"))
    route_by_name = {Path(row["file_path"]).name: row for row in routing}
    assert route_by_name["BAD_1D.parquet"]["route"] == "quarantine_only"
    assert route_by_name["BAD_1D.parquet"]["allowed"] is False


def test_inventory_stage_is_deterministic_for_same_run_inputs(tmp_path: Path) -> None:
    _write_good_market_parquet(tmp_path / "raw" / "Stock_parquet" / "MSFT_1D.parquet")
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path)

    rc1 = main(["--policy", str(policy), "--run-id", "determinism_run_1", "inventory"])
    rc2 = main(["--policy", str(policy), "--run-id", "determinism_run_2", "inventory"])
    assert rc1 == 0
    assert rc2 == 0

    run1 = json.loads((tmp_path / "evidence" / "determinism_run_1" / "dataset_catalog.json").read_text(encoding="utf-8"))
    run2 = json.loads((tmp_path / "evidence" / "determinism_run_2" / "dataset_catalog.json").read_text(encoding="utf-8"))
    assert run1 == run2


def test_quarantine_report_contains_inventory_failure_reasons(tmp_path: Path) -> None:
    _write_bad_no_time_parquet(tmp_path / "raw" / "Stock_parquet" / "BROKEN_1D.parquet")
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path)

    rc = main(["--policy", str(policy), "--run-id", "quarantine_run", "validate"])
    assert rc == 0

    report = json.loads((tmp_path / "evidence" / "quarantine_run" / "quarantine_report.json").read_text(encoding="utf-8"))
    assert len(report) == 1
    assert report[0]["reason"] == "missing_time_axis"
    issue_codes = [row["code"] for row in report[0]["issues"]]
    assert "missing_time_axis" in issue_codes


# ── S0-1: dry-run must write nothing ────────────────────────────────────────

def test_dry_run_writes_no_artifacts(tmp_path: Path) -> None:
    _write_good_market_parquet(tmp_path / "raw" / "Stock_parquet" / "AAPL_1D.parquet")
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path)

    rc = main(["--policy", str(policy), "--run-id", "dry_run_test", "--dry-run", "full-run"])
    assert rc == 0

    run_dir = tmp_path / "evidence" / "dry_run_test"
    assert not run_dir.exists(), (
        "--dry-run must not create the evidence directory or write any artifacts"
    )


# ── S0-2: validation_report is semantically distinct from dataset_catalog ───

def test_validation_report_is_issue_centric_and_distinct_from_catalog(tmp_path: Path) -> None:
    _write_good_market_parquet(tmp_path / "raw" / "Stock_parquet" / "AAPL_1D.parquet")
    _write_bad_no_time_parquet(tmp_path / "raw" / "Mystery_parquet" / "BAD_1D.parquet")
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path)

    rc = main(["--policy", str(policy), "--run-id", "val_distinct_run", "full-run"])
    assert rc == 0

    run_dir = tmp_path / "evidence" / "val_distinct_run"
    catalog = json.loads((run_dir / "dataset_catalog.json").read_text(encoding="utf-8"))
    validation = json.loads((run_dir / "validation_report.json").read_text(encoding="utf-8"))

    # Must not be identical payloads
    assert catalog != validation, "validation_report must differ from dataset_catalog"

    # validation_report must contain issue-centric keys
    assert len(validation) > 0
    sample = validation[0]
    assert "max_severity" in sample
    assert "issue_count" in sample
    assert "issue_codes" in sample
    assert "issues" in sample

    # catalog must NOT have max_severity (it has richer structural fields)
    assert "max_severity" not in catalog[0]

    # validation_report must NOT contain catalog-only fields
    assert "source_family" not in sample
    assert "partitioning_info" not in sample


# ── S0-3: publish before event_time triggers leakage flag ───────────────────

def test_altdata_publish_precedes_event_time_is_flagged_as_leakage(tmp_path: Path) -> None:
    """publish_time < event_time column must produce publish_precedes_event leakage flag."""
    _write_altdata_publish_leak_parquet(tmp_path / "raw" / "data_vendor" / "macro_1D.parquet")
    policy_path = tmp_path / "policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_policy(policy_path)

    ref_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    records = inventory_datasets(
        [tmp_path / "raw" / "data_vendor" / "macro_1D.parquet"],
        policy,
        tmp_path / "quarantine",
        reference_time=ref_time,
    )
    assert len(records) == 1
    record = records[0]
    assert "publish_precedes_event" in record.leakage_risk_flags, (
        f"expected publish_precedes_event in leakage_risk_flags, got {record.leakage_risk_flags}"
    )
    # event_time_col must be detected separately from effective_time
    assert record.event_time_semantics.get("event_time_col") is not None
    assert record.event_time_semantics.get("effective_time") is None


# ── S1-1: max_null_fraction and max_duplicate_fraction are enforced ─────────

def _write_high_null_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import numpy as np
    idx = pd.date_range("2024-01-01", periods=200, freq="D", tz="UTC")
    # 80% nulls in the 'value' column — exceeds policy max_null_fraction=0.25
    values = [float("nan")] * 160 + [1.0] * 40
    df = pd.DataFrame({"timestamp": idx, "value": values})
    df.to_parquet(path, index=False)


def _write_high_duplicate_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    idx = pd.date_range("2024-01-01", periods=130, freq="D", tz="UTC")
    # 50 duplicate rows (38%) — exceeds policy max_duplicate_fraction=0.02
    base = pd.DataFrame({"timestamp": idx, "close": [1.0] * 130})
    dup = pd.DataFrame({"timestamp": idx[:50], "close": [1.0] * 50})
    df = pd.concat([base, dup], ignore_index=True)
    df.to_parquet(path, index=False)


def test_excess_null_fraction_triggers_quarantine(tmp_path: Path) -> None:
    _write_high_null_parquet(tmp_path / "raw" / "altdata" / "HIGH_NULL_1D.parquet")
    policy_path = tmp_path / "policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_policy(policy_path)

    ref_time = datetime(2025, 6, 1, tzinfo=timezone.utc)
    records = inventory_datasets(
        [tmp_path / "raw" / "altdata" / "HIGH_NULL_1D.parquet"],
        policy,
        tmp_path / "quarantine",
        reference_time=ref_time,
    )
    assert len(records) == 1
    record = records[0]
    issue_codes = [i.code for i in record.issues]
    assert "excess_null_fraction" in issue_codes, (
        f"expected excess_null_fraction issue, got {issue_codes}"
    )
    assert record.governance_status == "quarantined"


def test_excess_duplicate_fraction_triggers_quarantine(tmp_path: Path) -> None:
    _write_high_duplicate_parquet(tmp_path / "raw" / "altdata" / "HIGH_DUP_1D.parquet")
    policy_path = tmp_path / "policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_policy(policy_path)

    ref_time = datetime(2025, 6, 1, tzinfo=timezone.utc)
    records = inventory_datasets(
        [tmp_path / "raw" / "altdata" / "HIGH_DUP_1D.parquet"],
        policy,
        tmp_path / "quarantine",
        reference_time=ref_time,
    )
    assert len(records) == 1
    record = records[0]
    issue_codes = [i.code for i in record.issues]
    assert "excess_duplicate_fraction" in issue_codes, (
        f"expected excess_duplicate_fraction issue, got {issue_codes}"
    )
    assert record.governance_status == "quarantined"


# ── S1-2: freshness_score is deterministic with injected reference_time ──────

def test_inventory_deterministic_with_injected_reference_time(tmp_path: Path) -> None:
    """Same reference_time must yield bit-identical catalog entries."""
    _write_good_market_parquet(tmp_path / "raw" / "Stock_parquet" / "MSFT_1D.parquet")
    policy_path = tmp_path / "policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_policy(policy_path)

    ref_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    files = [tmp_path / "raw" / "Stock_parquet" / "MSFT_1D.parquet"]
    quarantine_dir = tmp_path / "quarantine"

    records_a = inventory_datasets(files, policy, quarantine_dir, reference_time=ref_time)
    records_b = inventory_datasets(files, policy, quarantine_dir, reference_time=ref_time)

    assert len(records_a) == 1
    assert len(records_b) == 1
    assert asdict(records_a[0]) == asdict(records_b[0]), (
        "inventory_datasets with identical inputs and reference_time must be deterministic"
    )


# ── S1-4: recommendation must not use the word "promote" ────────────────────

def test_recommendation_is_not_promote(tmp_path: Path) -> None:
    """roi_report must not emit recommendation='promote' for any dataset."""
    _write_good_market_parquet(tmp_path / "raw" / "Stock_parquet" / "TSLA_1D.parquet")
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path)

    rc = main(["--policy", str(policy), "--run-id", "roi_label_run", "score"])
    assert rc == 0

    roi = json.loads(
        (tmp_path / "evidence" / "roi_label_run" / "roi_report.json").read_text(encoding="utf-8")
    )
    for row in roi:
        assert row["recommendation"] != "promote", (
            f"recommendation='promote' found for {row['file_path']} — "
            "must use 'review_for_governance_promotion' instead"
        )
