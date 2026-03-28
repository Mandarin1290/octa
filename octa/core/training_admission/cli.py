from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from .engine import (
    build_candidates,
    build_input_manifest,
    config_snapshot,
    decide_candidates,
    load_approvals,
    load_prior_decisions,
    load_recycling_inputs,
    parse_reference_time,
    summarize_decisions,
)
from .evidence import EvidencePack
from .policy import load_policy


EXIT_OK = 0
EXIT_INVALID_INPUT = 3
EXIT_INVALID_REGISTRY = 4


def _default_policy_path() -> Path:
    return Path("configs/training_admission_policy.yaml")


def _write_outputs(
    *,
    evidence: EvidencePack,
    decisions_payload: list[dict],
    quarantine_payload: list[dict],
    admitted_payload: list[dict],
    rejected_payload: list[dict],
    waiting_payload: list[dict],
    quarantined_payload: list[dict],
    command: str,
) -> None:
    if command in {"admission-decide", "admission-full-run"}:
        evidence.write_json("admission_decisions.json", decisions_payload, output_copy=True)
        evidence.write_json("admission_quarantine_report.json", quarantine_payload, output_copy=True)
        evidence.write_json("admitted_offline_training_candidates.json", admitted_payload, output_copy=True)
        evidence.write_json("rejected_training_candidates.json", rejected_payload, output_copy=True)
        evidence.write_json("waiting_for_approval_candidates.json", waiting_payload, output_copy=True)
        evidence.write_json("quarantined_training_candidates.json", quarantined_payload, output_copy=True)
    if command == "admission-scan":
        evidence.write_json("admission_decisions.json", [], output_copy=True)
        evidence.write_json("admission_quarantine_report.json", [], output_copy=True)


def _run(args: argparse.Namespace) -> int:
    policy = load_policy(Path(args.policy))
    recycling_run_dir = Path(args.recycling_run_dir or policy.recycling_run_dir)
    approvals_path = Path(args.approval_registry) if args.approval_registry else (Path(policy.approval_registry_path) if policy.approval_registry_path else None)
    prior_path = Path(args.prior_decisions_registry) if args.prior_decisions_registry else (Path(policy.prior_decisions_registry_path) if policy.prior_decisions_registry_path else None)
    reference_time = parse_reference_time(args.reference_time)
    try:
        payloads, materialized = load_recycling_inputs(recycling_run_dir)
    except FileNotFoundError as exc:
        print(str(exc))
        return EXIT_INVALID_INPUT
    try:
        approvals = load_approvals(approvals_path)
        prior_decisions = load_prior_decisions(prior_path)
    except ValueError as exc:
        print(str(exc))
        return EXIT_INVALID_REGISTRY

    input_manifest = build_input_manifest(
        recycling_run_dir=recycling_run_dir,
        policy=policy,
        approvals_path=approvals_path,
        prior_registry_path=prior_path,
    )
    evidence = EvidencePack(
        Path(policy.evidence_root),
        Path(policy.output_root),
        run_id=args.run_id,
        dry_run=bool(args.dry_run),
    )
    evidence.bootstrap(
        config_snapshot=config_snapshot(policy),
        input_manifest=input_manifest,
        reference_time=reference_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    candidates = build_candidates(materialized, recycling_run_dir)
    decisions, quarantines = decide_candidates(
        candidates,
        policy=policy,
        approvals=approvals,
        prior_decisions=prior_decisions,
        run_id=evidence.ctx.run_id,
        reference_time=reference_time,
    )
    decisions_payload = [asdict(row) for row in decisions]
    quarantine_payload = [asdict(row) for row in quarantines]
    admitted_payload = [row for row in decisions_payload if row["admission_decision"] == "admitted_for_offline_training"]
    rejected_payload = [row for row in decisions_payload if row["admission_decision"] == "rejected_for_training"]
    waiting_payload = [row for row in decisions_payload if row["admission_decision"] == "waiting_for_explicit_approval"]
    quarantined_payload = [row for row in decisions_payload if row["admission_decision"] == "quarantined_for_manual_review"]
    summary = summarize_decisions(decisions)

    _write_outputs(
        evidence=evidence,
        decisions_payload=decisions_payload,
        quarantine_payload=quarantine_payload,
        admitted_payload=admitted_payload,
        rejected_payload=rejected_payload,
        waiting_payload=waiting_payload,
        quarantined_payload=quarantined_payload,
        command=str(args.command),
    )
    evidence.finalize(summary)
    if args.dry_run:
        print(f"[DRY-RUN] run_id={evidence.ctx.run_id!r} — no admission artifacts written")
    else:
        print(evidence.ctx.run_dir)
    return EXIT_OK


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="octa-training-admission")
    parser.add_argument("--policy", default=str(_default_policy_path()))
    parser.add_argument("--recycling-run-dir", default=None)
    parser.add_argument("--approval-registry", default=None)
    parser.add_argument("--prior-decisions-registry", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--reference-time", default=None)
    parser.add_argument("--dry-run", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("admission-scan", "admission-decide", "admission-report", "admission-full-run"):
        sub.add_parser(command)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _run(args)


if __name__ == "__main__":
    raise SystemExit(main())
