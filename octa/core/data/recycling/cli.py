from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .common import flatten_dict
from .engine import (
    build_input_manifest,
    build_recycled_features,
    build_validation_report,
    classify_datasets,
    discover_files,
    inventory_datasets,
    route_datasets,
    score_datasets,
    summarize_run,
)
from .evidence import EvidencePack
from .policy import load_policy


def _default_policy_path() -> Path:
    return Path("configs/parquet_recycling_policy.yaml")


def _write_stage_outputs(evidence: EvidencePack, payloads: dict[str, Any]) -> None:
    for name, payload in payloads.items():
        evidence.write_json(name, payload)


def _run_pipeline(args: argparse.Namespace, stage: str) -> int:
    policy = load_policy(Path(args.policy))
    files = discover_files(policy)
    input_manifest = build_input_manifest(files, policy)
    evidence = EvidencePack(
        Path(policy.evidence_root),
        Path(policy.output_root),
        run_id=args.run_id,
        dry_run=bool(args.dry_run),
    )
    evidence.bootstrap(config_snapshot=flatten_dict("", asdict(policy)), input_manifest=input_manifest)
    records = inventory_datasets(files, policy, evidence.ctx.run_dir / "quarantine")
    record_payload = [asdict(r) for r in records]
    validation_payload = build_validation_report(records)
    decisions = classify_datasets(records, policy)
    decision_payload = [asdict(d) for d in decisions]
    utility = score_datasets(records, decisions)
    utility_payload = [asdict(u) for u in utility]
    artifacts = build_recycled_features(records, decisions, policy)
    artifact_payload = [asdict(a) for a in artifacts]
    routes = route_datasets(records, decisions, utility)
    route_payload = [asdict(r) for r in routes]
    quarantine_payload = [
        {
            "file_path": r.file_path,
            "reason": r.quarantine_reason,
            "issues": [asdict(issue) for issue in r.issues],
        }
        for r in records
        if r.governance_status == "quarantined"
    ]
    stage_payloads: dict[str, Any] = {
        "dataset_catalog.json": record_payload,
        "classification_report.json": decision_payload,
        "validation_report.json": validation_payload,
        "recycling_report.json": artifact_payload,
        "routing_report.json": route_payload,
        "roi_report.json": utility_payload,
        "quarantine_report.json": quarantine_payload,
    }
    if stage == "inventory":
        stage_payloads = {"dataset_catalog.json": record_payload, "quarantine_report.json": quarantine_payload}
    elif stage == "catalog":
        stage_payloads = {"dataset_catalog.json": record_payload}
    elif stage == "validate":
        stage_payloads = {"validation_report.json": validation_payload, "quarantine_report.json": quarantine_payload}
    elif stage == "classify":
        stage_payloads = {"classification_report.json": decision_payload}
    elif stage == "recycle":
        stage_payloads = {"recycling_report.json": artifact_payload}
    elif stage == "score":
        stage_payloads = {"roi_report.json": utility_payload}
    elif stage == "route":
        stage_payloads = {"routing_report.json": route_payload}
    elif stage == "quarantine-report":
        stage_payloads = {"quarantine_report.json": quarantine_payload}
    _write_stage_outputs(evidence, stage_payloads)
    summary = summarize_run(records, decisions, utility, routes, artifacts)
    evidence.finalize(summary)
    if args.dry_run:
        print(f"[DRY-RUN] run_id={evidence.ctx.run_id!r} — no evidence artifacts written")
    else:
        print(evidence.ctx.run_dir)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="octa-parquet-recycling")
    parser.add_argument("--policy", default=str(_default_policy_path()))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    for cmd in (
        "inventory",
        "catalog",
        "validate",
        "classify",
        "recycle",
        "score",
        "route",
        "full-run",
        "evidence-report",
        "quarantine-report",
    ):
        sub.add_parser(cmd)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    command = str(args.command)
    if command == "evidence-report":
        return _run_pipeline(args, "full-run")
    if command == "full-run":
        return _run_pipeline(args, "full-run")
    return _run_pipeline(args, command)


if __name__ == "__main__":
    raise SystemExit(main())
