from __future__ import annotations

import json
from pathlib import Path

from octa.core.training_admission.cli import main


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _upstream_rows(*, file_path: str, symbol: str, asset_class: str = "equities", provider: str = "stock_parquet", source_family: str = "market_ohlcv", frequency: str = "1D", quality: float = 0.95, confidence: float = 0.96, coverage_rows: int = 500, coverage_value: float = 0.2, interpretability: float = 0.75, governance_status: str = "candidate", blocking_flags: list[str] | None = None, promotion_eligibility: str = "model_ready_candidate", route: str = "model_ready_candidate", route_allowed: bool = True, max_severity: str = "INFO", lookahead_flags: list[str] | None = None, leakage_flags: list[str] | None = None) -> dict[str, list[dict]]:
    catalog = [
        {
            "file_path": file_path,
            "logical_dataset_name": f"{symbol}_{frequency}",
            "source_family": source_family,
            "provider": provider,
            "inferred_asset_classes": [asset_class] if asset_class != "unknown" else [],
            "inferred_symbols": [symbol],
            "inferred_frequency": frequency,
            "governance_status": governance_status,
            "time_coverage_start": "2024-01-01T00:00:00Z",
            "time_coverage_end": "2025-01-01T00:00:00Z",
            "row_count": coverage_rows,
            "quality_score": quality,
            "confidence_score": confidence,
            "lookahead_risk_flags": lookahead_flags or [],
            "leakage_risk_flags": leakage_flags or [],
            "file_hash_sha256": f"hash-{symbol}-{frequency}",
        }
    ]
    classification = [
        {
            "file_path": file_path,
            "blocking_flags": blocking_flags or [],
            "promotion_eligibility": promotion_eligibility,
        }
    ]
    validation = [
        {
            "file_path": file_path,
            "issue_codes": [],
            "max_severity": max_severity,
        }
    ]
    routing = [
        {
            "file_path": file_path,
            "route": route,
            "allowed": route_allowed,
            "reason": "eligible_but_not_promoted",
        }
    ]
    roi = [
        {
            "file_path": file_path,
            "recommendation": "review_for_governance_promotion",
            "interpretability": interpretability,
            "coverage_value": coverage_value,
        }
    ]
    return {
        "dataset_catalog.json": catalog,
        "classification_report.json": classification,
        "validation_report.json": validation,
        "routing_report.json": routing,
        "roi_report.json": roi,
    }


def _write_recycling_run(run_dir: Path, payloads: dict[str, list[dict]]) -> None:
    for name, rows in payloads.items():
        _write_json(run_dir / name, rows)


def _write_policy(
    path: Path,
    root: Path,
    recycling_run_dir: Path,
    *,
    min_confidence: float = 0.9,
    max_open_risk_flags: int = 0,
    allowed_source_families: tuple[str, ...] = ("market_ohlcv",),
) -> None:
    path.write_text(
        "\n".join(
            [
                f"recycling_run_dir: {recycling_run_dir}",
                f"output_root: {root / 'outputs'}",
                f"evidence_root: {root / 'evidence'}",
                f"approval_registry_path: {root / 'approvals.json'}",
                f"prior_decisions_registry_path: {root / 'prior.json'}",
                "require_explicit_approval: true",
                "approval_scope_required: offline_training_only",
                "allowed_asset_classes:",
                "  - equities",
                "allowed_source_families:",
                *[f"  - {item}" for item in allowed_source_families],
                "blocked_providers:",
                "  - blocked_vendor",
                "allowed_symbol_patterns:",
                "  - '*'",
                "allowed_frequencies:",
                "  - 1D",
                "min_coverage_rows: 256",
                f"min_confidence: {min_confidence}",
                "min_quality: 0.9",
                "min_interpretability: 0.7",
                "min_coverage_value: 0.02",
                f"max_open_risk_flags: {max_open_risk_flags}",
                "offline_only: true",
                "decision_ttl_days: 30",
                "conflict_behavior: quarantine",
            ]
        ),
        encoding="utf-8",
    )


def _write_approvals(path: Path, dataset_identifier: str | None = None) -> None:
    approvals = []
    if dataset_identifier:
        approvals.append(
            {
                "dataset_identifier": dataset_identifier,
                "action": "approve",
                "scope": "offline_training_only",
                "actor": "ops",
                "rationale": "approved",
                "evidence_ref": "TICKET-1",
                "approved_at": "2026-03-20T00:00:00Z",
            }
        )
    _write_json(path, {"schema_version": 1, "approvals": approvals})


def _write_prior(path: Path, decisions: list[dict] | None = None) -> None:
    _write_json(path, {"schema_version": 1, "decisions": decisions or []})


def _dataset_identifier(symbol: str, frequency: str = "1D") -> str:
    from octa.core.data.recycling.common import stable_hash

    return stable_hash(
        {
            "file_hash_sha256": f"hash-{symbol}-{frequency}",
            "logical_dataset_name": f"{symbol}_{frequency}",
            "symbol": symbol,
            "frequency": frequency,
        }
    )


def _read_decisions(run_dir: Path) -> list[dict]:
    return json.loads((run_dir / "admission_decisions.json").read_text(encoding="utf-8"))


def _normalized_decisions(run_dir: Path) -> list[dict]:
    rows = _read_decisions(run_dir)
    for row in rows:
        row["run_id"] = "normalized"
    return rows


def test_model_ready_candidate_with_approval_is_admitted(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/AAPL_1D.parquet", symbol="AAPL"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("AAPL"))
    _write_prior(tmp_path / "prior.json")

    rc = main(["--policy", str(policy), "--run-id", "admit_run", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    assert rc == 0
    decisions = _read_decisions(tmp_path / "evidence" / "admit_run")
    assert decisions[0]["admission_decision"] == "admitted_for_offline_training"
    assert decisions[0]["no_auto_promotion"] is True


def test_model_ready_candidate_without_approval_waits(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/MSFT_1D.parquet", symbol="MSFT"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json")
    _write_prior(tmp_path / "prior.json")

    rc = main(["--policy", str(policy), "--run-id", "wait_run", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    assert rc == 0
    decisions = _read_decisions(tmp_path / "evidence" / "wait_run")
    assert decisions[0]["admission_decision"] == "waiting_for_explicit_approval"


def test_leakage_risk_rejects(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(file_path="/tmp/NVDA_1D.parquet", symbol="NVDA", leakage_flags=["publish_precedes_event"])
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("NVDA"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "reject_risk", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "reject_risk")
    assert decisions[0]["admission_decision"] == "rejected_for_training"


def test_market_ohlcv_missing_publish_effective_semantics_does_not_block_by_itself(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(
        file_path="/tmp/ARCB_1D.parquet",
        symbol="ARCB",
        confidence=0.8,
        lookahead_flags=["missing_publish_effective_semantics"],
    )
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run, min_confidence=0.8)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("ARCB"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "ohlcv_missing_publish_ok", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "ohlcv_missing_publish_ok")
    assert decisions[0]["admission_decision"] == "admitted_for_offline_training"
    assert decisions[0]["policy_checks"]["no_open_risk_flags"]["passed"] is True
    assert "ignored_risk_flags=['missing_publish_effective_semantics']" in decisions[0]["policy_checks"]["no_open_risk_flags"]["detail"]


def test_publish_semantics_flag_remains_material_for_non_market_ohlcv(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(
        file_path="/tmp/MACRO_1D.parquet",
        symbol="MACRO",
        source_family="altdata",
        confidence=0.8,
        lookahead_flags=["missing_publish_effective_semantics"],
    )
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(
        policy,
        tmp_path,
        recycling_run,
        min_confidence=0.8,
        allowed_source_families=("altdata",),
    )
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("MACRO"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "altdata_missing_publish_blocked", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "altdata_missing_publish_blocked")
    assert decisions[0]["admission_decision"] == "rejected_for_training"
    assert decisions[0]["policy_checks"]["no_open_risk_flags"]["passed"] is False


def test_confidence_threshold_matches_real_prototype_candidate_range(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(file_path="/tmp/AWK_1D.parquet", symbol="AWK", confidence=0.8)
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run, min_confidence=0.8)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("AWK"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "confidence_080", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "confidence_080")
    assert decisions[0]["admission_decision"] == "admitted_for_offline_training"

    payloads_low = _upstream_rows(file_path="/tmp/AGNC_1D.parquet", symbol="AGNC", confidence=0.79)
    _write_recycling_run(recycling_run, payloads_low)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("AGNC"))
    main(["--policy", str(policy), "--run-id", "confidence_079", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions_low = _read_decisions(tmp_path / "evidence" / "confidence_079")
    assert decisions_low[0]["admission_decision"] == "rejected_for_training"
    assert decisions_low[0]["policy_checks"]["confidence_threshold"]["passed"] is False


def test_unknown_asset_or_provider_quarantines(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(file_path="/tmp/ALT_1D.parquet", symbol="ALT", asset_class="unknown", provider="mystery_vendor")
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("ALT"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "quarantine_unknown", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "quarantine_unknown")
    assert decisions[0]["admission_decision"] == "quarantined_for_manual_review"


def test_not_model_ready_is_rejected(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(
        file_path="/tmp/IBM_1D.parquet",
        symbol="IBM",
        promotion_eligibility="curated_only",
        route="curated_only",
    )
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("IBM"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "reject_not_ready", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "reject_not_ready")
    assert decisions[0]["admission_decision"] == "rejected_for_training"


def test_policy_asset_class_block_rejects(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    payloads = _upstream_rows(file_path="/tmp/ES_1D.parquet", symbol="ES", asset_class="futures")
    _write_recycling_run(recycling_run, payloads)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("ES"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "reject_asset", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "reject_asset")
    assert decisions[0]["admission_decision"] == "rejected_for_training"


def test_decision_artifacts_are_deterministic_given_same_inputs(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/ORCL_1D.parquet", symbol="ORCL"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("ORCL"))
    _write_prior(tmp_path / "prior.json")

    args = ["--policy", str(policy), "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"]
    main(["--run-id", "deterministic_1", *args])
    main(["--run-id", "deterministic_2", *args])
    run1 = tmp_path / "evidence" / "deterministic_1"
    run2 = tmp_path / "evidence" / "deterministic_2"
    for name in (
        "admission_config_snapshot.json",
        "admission_input_manifest.json",
        "admission_quarantine_report.json",
    ):
        assert (run1 / name).read_text(encoding="utf-8") == (run2 / name).read_text(encoding="utf-8")
    assert _normalized_decisions(run1) == _normalized_decisions(run2)


def test_no_auto_promotion_marker_present(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/META_1D.parquet", symbol="META"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("META"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "no_auto", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "no_auto")
    assert decisions[0]["no_auto_promotion"] is True


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/AMD_1D.parquet", symbol="AMD"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("AMD"))
    _write_prior(tmp_path / "prior.json")

    rc = main(["--policy", str(policy), "--run-id", "dry_run_admission", "--reference-time", "2026-03-20T00:00:00Z", "--dry-run", "admission-full-run"])
    assert rc == 0
    assert not (tmp_path / "evidence" / "dry_run_admission").exists()


def test_admission_does_not_call_execution_or_write_paper_outputs(tmp_path: Path, monkeypatch) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/SHOP_1D.parquet", symbol="SHOP"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("SHOP"))
    _write_prior(tmp_path / "prior.json")

    def _boom(*args, **kwargs):
        raise AssertionError("execution path must remain untouched")

    monkeypatch.setattr("octa.execution.runner.run_execution", _boom)
    main(["--policy", str(policy), "--run-id", "offline_only", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    assert not (tmp_path / "paper_ready").exists()


def test_duplicate_scope_conflict_fails_closed(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    rows = {}
    rows.update(_upstream_rows(file_path="/tmp/BABA_1D_a.parquet", symbol="BABA"))
    second = _upstream_rows(file_path="/tmp/BABA_1D_b.parquet", symbol="BABA")
    for name, payload in second.items():
        rows[name].extend(payload)
    _write_recycling_run(recycling_run, rows)
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(tmp_path / "approvals.json", _dataset_identifier("BABA"))
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "dup_conflict", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "dup_conflict")
    assert {row["admission_decision"] for row in decisions} == {"quarantined_for_manual_review"}


def test_approval_registry_is_evaluated_correctly(tmp_path: Path) -> None:
    recycling_run = tmp_path / "recycling"
    _write_recycling_run(recycling_run, _upstream_rows(file_path="/tmp/QQQ_1D.parquet", symbol="QQQ"))
    policy = tmp_path / "policy.yaml"
    _write_policy(policy, tmp_path, recycling_run)
    _write_approvals(
        tmp_path / "approvals.json",
        None,
    )
    _write_json(
        tmp_path / "approvals.json",
        {
            "schema_version": 1,
            "approvals": [
                {
                    "dataset_identifier": _dataset_identifier("QQQ"),
                    "action": "approve",
                    "scope": "wrong_scope",
                    "actor": "ops",
                    "rationale": "wrong scope",
                    "evidence_ref": "TICKET-2",
                    "approved_at": "2026-03-20T00:00:00Z",
                }
            ],
        },
    )
    _write_prior(tmp_path / "prior.json")

    main(["--policy", str(policy), "--run-id", "approval_scope", "--reference-time", "2026-03-20T00:00:00Z", "admission-full-run"])
    decisions = _read_decisions(tmp_path / "evidence" / "approval_scope")
    assert decisions[0]["admission_decision"] == "waiting_for_explicit_approval"
