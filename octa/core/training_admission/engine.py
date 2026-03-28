from __future__ import annotations

import fnmatch
import json
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import flatten_dict, sha256_file, stable_hash

from .models import (
    AdmissionApproval,
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionQuarantineRow,
    DecisionContext,
    PriorDecision,
)
from .policy import AdmissionPolicy


REQUIRED_INPUT_FILES = (
    "dataset_catalog.json",
    "classification_report.json",
    "validation_report.json",
    "routing_report.json",
    "roi_report.json",
)


def parse_reference_time(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    value = datetime.fromisoformat(text)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_registry_entries(path: Path | None, key: str) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    raw = _read_json(path)
    if isinstance(raw, dict):
        entries = raw.get(key, [])
    elif isinstance(raw, list):
        entries = raw
    else:
        raise ValueError(f"unsupported registry payload: {path}")
    if not isinstance(entries, list):
        raise ValueError(f"registry key must be a list: {path}:{key}")
    out: list[dict[str, Any]] = []
    for row in entries:
        if isinstance(row, dict):
            out.append(row)
    return out


def load_approvals(path: Path | None) -> dict[str, list[AdmissionApproval]]:
    entries = _read_registry_entries(path, "approvals")
    by_dataset: dict[str, list[AdmissionApproval]] = defaultdict(list)
    for row in entries:
        approval = AdmissionApproval(
            dataset_identifier=str(row.get("dataset_identifier", "")).strip(),
            action=str(row.get("action", "")).strip().lower(),
            scope=str(row.get("scope", "")).strip(),
            actor=str(row.get("actor", "")).strip(),
            rationale=str(row.get("rationale", "")).strip(),
            evidence_ref=str(row.get("evidence_ref", "")).strip(),
            approved_at=str(row.get("approved_at", "")).strip(),
            expires_at=str(row.get("expires_at", "")).strip() or None,
        )
        if approval.dataset_identifier:
            by_dataset[approval.dataset_identifier].append(approval)
    return by_dataset


def load_prior_decisions(path: Path | None) -> dict[str, list[PriorDecision]]:
    entries = _read_registry_entries(path, "decisions")
    by_dataset: dict[str, list[PriorDecision]] = defaultdict(list)
    for row in entries:
        prior = PriorDecision(
            dataset_identifier=str(row.get("dataset_identifier", "")).strip(),
            decision=str(row.get("admission_decision", row.get("decision", ""))).strip(),
            upstream_recycling_run=str(row.get("upstream_recycling_run", "")).strip() or None,
            upstream_dataset_ref=str(row.get("upstream_dataset_ref", "")).strip() or None,
            decided_at=str(row.get("timestamp", row.get("decided_at", ""))).strip(),
            expires_at=str(row.get("decision_expires_at", row.get("expires_at", ""))).strip() or None,
            reason=str(row.get("reason", "")).strip() or None,
        )
        if prior.dataset_identifier:
            by_dataset[prior.dataset_identifier].append(prior)
    return by_dataset


def load_recycling_inputs(run_dir: Path) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    payloads: dict[str, Any] = {}
    for name in REQUIRED_INPUT_FILES:
        path = run_dir / name
        if not path.exists():
            raise FileNotFoundError(f"missing_required_recycling_artifact:{path}")
        payloads[name] = _read_json(path)
    return payloads, {name: payloads[name] for name in REQUIRED_INPUT_FILES}


def build_input_manifest(
    *,
    recycling_run_dir: Path,
    policy: AdmissionPolicy,
    approvals_path: Path | None,
    prior_registry_path: Path | None,
) -> dict[str, Any]:
    files = []
    for name in REQUIRED_INPUT_FILES:
        path = recycling_run_dir / name
        files.append(
            {
                "name": name,
                "path": str(path),
                "sha256": sha256_file(path),
            }
        )
    registries = []
    for label, path in (("approval_registry", approvals_path), ("prior_decisions_registry", prior_registry_path)):
        if path is None:
            registries.append({"name": label, "path": None, "present": False})
            continue
        registries.append(
            {
                "name": label,
                "path": str(path),
                "present": path.exists(),
                "sha256": sha256_file(path) if path.exists() else None,
            }
        )
    return {
        "recycling_run_dir": str(recycling_run_dir),
        "required_artifacts": files,
        "registries": registries,
        "policy_requirements": {
            "require_explicit_approval": policy.require_explicit_approval,
            "approval_scope_required": policy.approval_scope_required,
            "offline_only": policy.offline_only,
            "conflict_behavior": policy.conflict_behavior,
            "evidence_requirements": list(policy.evidence_requirements),
        },
    }


def build_candidates(payloads: dict[str, list[dict[str, Any]]], recycling_run_dir: Path) -> list[AdmissionCandidate]:
    catalog = {str(row["file_path"]): row for row in payloads["dataset_catalog.json"]}
    classification = {str(row["file_path"]): row for row in payloads["classification_report.json"]}
    validation = {str(row["file_path"]): row for row in payloads["validation_report.json"]}
    routing = {str(row["file_path"]): row for row in payloads["routing_report.json"]}
    roi = {str(row["file_path"]): row for row in payloads["roi_report.json"]}

    candidates: list[AdmissionCandidate] = []
    for file_path in sorted(catalog):
        row = catalog[file_path]
        class_row = classification.get(file_path, {})
        validation_row = validation.get(file_path, {})
        routing_row = routing.get(file_path, {})
        roi_row = roi.get(file_path, {})
        symbol = ""
        inferred_symbols = row.get("inferred_symbols") or []
        if inferred_symbols:
            symbol = str(inferred_symbols[0]).upper()
        asset_classes = row.get("inferred_asset_classes") or []
        asset_class = str(asset_classes[0]).lower() if asset_classes else "unknown"
        dataset_identifier = stable_hash(
            {
                "file_hash_sha256": row.get("file_hash_sha256"),
                "logical_dataset_name": row.get("logical_dataset_name"),
                "symbol": symbol,
                "frequency": row.get("inferred_frequency"),
            }
        )
        candidates.append(
            AdmissionCandidate(
                file_path=file_path,
                dataset_identifier=dataset_identifier,
                logical_dataset_name=str(row.get("logical_dataset_name", "")),
                symbol=symbol,
                asset_class=asset_class,
                source_family=str(row.get("source_family", "unknown")),
                provider=str(row.get("provider", "unknown")),
                frequency=str(row.get("inferred_frequency", "unknown")),
                governance_status=str(row.get("governance_status", "unknown")),
                time_coverage_start=row.get("time_coverage_start"),
                time_coverage_end=row.get("time_coverage_end"),
                row_count=row.get("row_count"),
                quality_score=float(row.get("quality_score", 0.0) or 0.0),
                confidence_score=float(row.get("confidence_score", 0.0) or 0.0),
                lookahead_risk_flags=[str(item) for item in row.get("lookahead_risk_flags", [])],
                leakage_risk_flags=[str(item) for item in row.get("leakage_risk_flags", [])],
                issue_codes=[str(item) for item in validation_row.get("issue_codes", [])],
                max_severity=str(validation_row.get("max_severity", "UNKNOWN")),
                blocking_flags=[str(item) for item in class_row.get("blocking_flags", [])],
                promotion_eligibility=str(class_row.get("promotion_eligibility", "blocked")),
                routing_decision=str(routing_row.get("route", "quarantine_only")),
                routing_allowed=bool(routing_row.get("allowed", False)),
                routing_reason=str(routing_row.get("reason", "missing_route")),
                roi_recommendation=str(roi_row.get("recommendation", "hold")),
                interpretability=float(roi_row.get("interpretability", 0.0) or 0.0),
                coverage_value=float(roi_row.get("coverage_value", 0.0) or 0.0),
                upstream_run_dir=str(recycling_run_dir),
                upstream_references={
                    "dataset_catalog": str(recycling_run_dir / "dataset_catalog.json"),
                    "classification_report": str(recycling_run_dir / "classification_report.json"),
                    "validation_report": str(recycling_run_dir / "validation_report.json"),
                    "routing_report": str(recycling_run_dir / "routing_report.json"),
                    "roi_report": str(recycling_run_dir / "roi_report.json"),
                    "file_hash_sha256": row.get("file_hash_sha256"),
                },
            )
        )
    return candidates


def _is_active(expiry_text: str | None, now: datetime) -> bool:
    if not expiry_text:
        return True
    try:
        expiry = parse_reference_time(expiry_text)
    except Exception:
        return False
    return expiry >= now


def _find_matching_approval(
    approvals: dict[str, list[AdmissionApproval]],
    dataset_identifier: str,
    scope_required: str,
    now: datetime,
) -> AdmissionApproval | None:
    for row in approvals.get(dataset_identifier, []):
        if row.action != "approve":
            continue
        if row.scope != scope_required:
            continue
        if not _is_active(row.expires_at, now):
            continue
        return row
    return None


def _policy_check(ctx: DecisionContext, name: str, passed: bool, detail: str) -> None:
    ctx.policy_checks[name] = {"passed": bool(passed), "detail": detail}
    if not passed:
        ctx.reasons.append(f"{name}:{detail}")


def _matches_symbol_scope(candidate: AdmissionCandidate, policy: AdmissionPolicy) -> bool:
    symbol = candidate.symbol
    if policy.allowed_symbols and symbol in {item.upper() for item in policy.allowed_symbols}:
        return True
    if policy.allowed_symbol_patterns:
        for pattern in policy.allowed_symbol_patterns:
            if fnmatch.fnmatch(symbol, pattern.upper()):
                return True
        return False
    if policy.allowed_symbols:
        return False
    return bool(symbol)


def _effective_decision_expiry(reference_time: datetime, ttl_days: int) -> str | None:
    if ttl_days <= 0:
        return None
    return _iso_utc(reference_time + timedelta(days=ttl_days))


def _effective_risk_flags(candidate: AdmissionCandidate) -> tuple[list[str], list[str]]:
    raw_flags = sorted(set(candidate.lookahead_risk_flags) | set(candidate.leakage_risk_flags))
    ignored_flags: list[str] = []
    effective_flags = list(raw_flags)

    # Plain market OHLCV price series do not carry publish/effective semantics.
    # Ignore only this specific flag for this specific dataset class.
    if candidate.source_family == "market_ohlcv":
        ignored_flags = [flag for flag in effective_flags if flag == "missing_publish_effective_semantics"]
        if ignored_flags:
            effective_flags = [flag for flag in effective_flags if flag not in set(ignored_flags)]

    return effective_flags, ignored_flags


def decide_candidates(
    candidates: list[AdmissionCandidate],
    *,
    policy: AdmissionPolicy,
    approvals: dict[str, list[AdmissionApproval]],
    prior_decisions: dict[str, list[PriorDecision]],
    run_id: str,
    reference_time: datetime,
) -> tuple[list[AdmissionDecision], list[AdmissionQuarantineRow]]:
    decisions: list[AdmissionDecision] = []
    quarantines: list[AdmissionQuarantineRow] = []

    by_scope_key: dict[tuple[str, str, str, str], list[AdmissionCandidate]] = defaultdict(list)
    for candidate in candidates:
        by_scope_key[(candidate.symbol, candidate.asset_class, candidate.provider, candidate.frequency)].append(candidate)

    duplicate_keys = {key for key, rows in by_scope_key.items() if len(rows) > 1}

    for candidate in sorted(candidates, key=lambda row: (row.symbol, row.frequency, row.file_path)):
        ctx = DecisionContext(candidate=candidate)
        risk_flags, ignored_risk_flags = _effective_risk_flags(candidate)
        evidence_refs = [candidate.upstream_references[name] for name in sorted(candidate.upstream_references) if name.endswith(".json")]
        decision_expires_at = _effective_decision_expiry(reference_time, policy.decision_ttl_days)
        approval = _find_matching_approval(
            approvals,
            candidate.dataset_identifier,
            policy.approval_scope_required,
            reference_time,
        )

        _policy_check(
            ctx,
            "recycling_model_ready_candidate",
            candidate.promotion_eligibility == "model_ready_candidate" and candidate.routing_decision == "model_ready_candidate",
            f"promotion_eligibility={candidate.promotion_eligibility} route={candidate.routing_decision}",
        )
        _policy_check(
            ctx,
            "no_blocking_flags",
            not candidate.blocking_flags,
            f"blocking_flags={candidate.blocking_flags}",
        )
        _policy_check(
            ctx,
            "not_quarantined_upstream",
            candidate.governance_status == "candidate",
            f"governance_status={candidate.governance_status}",
        )
        _policy_check(
            ctx,
            "no_open_risk_flags",
            len(risk_flags) <= policy.max_open_risk_flags,
            (
                f"effective_risk_flags={risk_flags}"
                f" ignored_risk_flags={ignored_risk_flags}"
                f" source_family={candidate.source_family}"
            ),
        )
        _policy_check(
            ctx,
            "time_semantics_clear",
            candidate.time_coverage_start is not None and candidate.time_coverage_end is not None and candidate.max_severity != "SEVERE",
            f"time_coverage_start={candidate.time_coverage_start} time_coverage_end={candidate.time_coverage_end} max_severity={candidate.max_severity}",
        )
        _policy_check(
            ctx,
            "asset_provider_semantics_clear",
            candidate.asset_class != "unknown" and candidate.provider not in {"", "unknown"},
            f"asset_class={candidate.asset_class} provider={candidate.provider}",
        )
        _policy_check(
            ctx,
            "asset_class_allowed",
            candidate.asset_class in {item.lower() for item in policy.allowed_asset_classes},
            f"asset_class={candidate.asset_class}",
        )
        _policy_check(
            ctx,
            "source_family_allowed",
            candidate.source_family in {item for item in policy.allowed_source_families},
            f"source_family={candidate.source_family}",
        )
        _policy_check(
            ctx,
            "provider_allowed",
            candidate.provider not in {item for item in policy.blocked_providers},
            f"provider={candidate.provider}",
        )
        _policy_check(
            ctx,
            "symbol_in_scope",
            _matches_symbol_scope(candidate, policy),
            f"symbol={candidate.symbol}",
        )
        _policy_check(
            ctx,
            "frequency_allowed",
            candidate.frequency in {item for item in policy.allowed_frequencies},
            f"frequency={candidate.frequency}",
        )
        _policy_check(
            ctx,
            "coverage_sufficient",
            int(candidate.row_count or 0) >= policy.min_coverage_rows and candidate.coverage_value >= policy.min_coverage_value,
            f"row_count={candidate.row_count} coverage_value={candidate.coverage_value}",
        )
        _policy_check(
            ctx,
            "quality_threshold",
            candidate.quality_score >= policy.min_quality,
            f"quality_score={candidate.quality_score}",
        )
        _policy_check(
            ctx,
            "confidence_threshold",
            candidate.confidence_score >= policy.min_confidence,
            f"confidence_score={candidate.confidence_score}",
        )
        _policy_check(
            ctx,
            "interpretability_threshold",
            candidate.interpretability >= policy.min_interpretability,
            f"interpretability={candidate.interpretability}",
        )
        _policy_check(
            ctx,
            "offline_scope_only",
            bool(policy.offline_only),
            f"offline_only={policy.offline_only}",
        )

        duplicate_key = (candidate.symbol, candidate.asset_class, candidate.provider, candidate.frequency)
        conflict_duplicate = duplicate_key in duplicate_keys
        _policy_check(
            ctx,
            "no_duplicate_scope_conflict",
            not conflict_duplicate,
            f"scope_key={duplicate_key}",
        )

        prior_rows = prior_decisions.get(candidate.dataset_identifier, [])
        prior_states = {row.decision for row in prior_rows if _is_active(row.expires_at, reference_time)}
        prior_conflict = len(prior_states) > 1 or any(
            state in {"quarantined_for_manual_review", "rejected_for_training"} for state in prior_states
        )
        _policy_check(
            ctx,
            "no_prior_conflict",
            not prior_conflict,
            f"prior_states={sorted(prior_states)}",
        )
        already_admitted = "admitted_for_offline_training" in prior_states

        approval_status: dict[str, Any]
        if approval is None:
            approval_status = {
                "required": bool(policy.require_explicit_approval),
                "present": False,
                "status": "missing" if policy.require_explicit_approval else "not_required",
                "actor": None,
                "evidence_ref": None,
            }
        else:
            approval_status = {
                "required": bool(policy.require_explicit_approval),
                "present": True,
                "status": "approved",
                "actor": approval.actor,
                "evidence_ref": approval.evidence_ref,
                "approved_at": approval.approved_at,
                "scope": approval.scope,
            }
            evidence_refs.append(str(approval.evidence_ref))

        hard_reject = any(
            not ctx.policy_checks[name]["passed"]
            for name in (
                "recycling_model_ready_candidate",
                "no_blocking_flags",
                "not_quarantined_upstream",
                "no_open_risk_flags",
                "asset_class_allowed",
                "source_family_allowed",
                "provider_allowed",
                "symbol_in_scope",
                "frequency_allowed",
                "coverage_sufficient",
                "quality_threshold",
                "confidence_threshold",
                "interpretability_threshold",
                "offline_scope_only",
            )
        )
        quarantine = any(
            not ctx.policy_checks[name]["passed"]
            for name in (
                "asset_provider_semantics_clear",
                "time_semantics_clear",
                "no_duplicate_scope_conflict",
                "no_prior_conflict",
            )
        )
        waiting = bool(policy.require_explicit_approval and approval is None and not hard_reject and not quarantine)

        if already_admitted and not quarantine:
            final_decision = "rejected_for_training"
            ctx.reasons.append("already_admitted_active")
            ctx.blocking_flags.append("already_admitted_active")
        elif quarantine:
            final_decision = "quarantined_for_manual_review"
            ctx.blocking_flags.extend(
                [
                    flag
                    for flag in (
                        "time_semantics_unclear" if not ctx.policy_checks["time_semantics_clear"]["passed"] else "",
                        "asset_or_provider_semantics_unclear" if not ctx.policy_checks["asset_provider_semantics_clear"]["passed"] else "",
                        "duplicate_scope_conflict" if not ctx.policy_checks["no_duplicate_scope_conflict"]["passed"] else "",
                        "prior_decision_conflict" if not ctx.policy_checks["no_prior_conflict"]["passed"] else "",
                    )
                    if flag
                ]
            )
        elif hard_reject:
            final_decision = "rejected_for_training"
            ctx.blocking_flags.extend(risk_flags)
        elif waiting:
            final_decision = "waiting_for_explicit_approval"
        else:
            final_decision = "admitted_for_offline_training"

        ctx.blocking_flags.extend(candidate.blocking_flags)
        ctx.blocking_flags = sorted(dict.fromkeys(flag for flag in ctx.blocking_flags if flag))
        reason_list = sorted(dict.fromkeys(reason for reason in ctx.reasons if reason))
        decision = AdmissionDecision(
            dataset_identifier=candidate.dataset_identifier,
            symbol=candidate.symbol,
            asset_class=candidate.asset_class,
            source_family=candidate.source_family,
            provider=candidate.provider,
            frequency=candidate.frequency,
            upstream_recycling_decision_reference={
                "run_dir": candidate.upstream_run_dir,
                "upstream_dataset_ref": candidate.upstream_references.get("file_hash_sha256"),
                "promotion_eligibility": candidate.promotion_eligibility,
                "routing_decision": candidate.routing_decision,
                "routing_allowed": candidate.routing_allowed,
                "routing_reason": candidate.routing_reason,
                "roi_recommendation": candidate.roi_recommendation,
            },
            admission_decision=final_decision,
            reasons=reason_list,
            blocking_flags=ctx.blocking_flags,
            policy_checks=ctx.policy_checks,
            approval_status=approval_status,
            evidence_references=sorted(dict.fromkeys(evidence_refs)),
            timestamp=_iso_utc(reference_time) or "",
            run_id=run_id,
            confidence_snapshot={
                "quality_score": round(candidate.quality_score, 6),
                "confidence_score": round(candidate.confidence_score, 6),
                "interpretability": round(candidate.interpretability, 6),
                "coverage_value": round(candidate.coverage_value, 6),
            },
            no_auto_promotion=True,
            offline_scope_only=True,
            decision_expires_at=decision_expires_at,
        )
        decisions.append(decision)
        if final_decision == "quarantined_for_manual_review":
            quarantines.append(
                AdmissionQuarantineRow(
                    dataset_identifier=candidate.dataset_identifier,
                    symbol=candidate.symbol,
                    reasons=reason_list,
                    blocking_flags=ctx.blocking_flags,
                    evidence_references=decision.evidence_references,
                )
            )
    return decisions, quarantines


def summarize_decisions(decisions: list[AdmissionDecision]) -> str:
    counts = Counter(decision.admission_decision for decision in decisions)
    lines = [
        "# Training Admission Summary",
        "",
        f"- total_candidates_evaluated: {len(decisions)}",
        f"- decisions: {dict(sorted(counts.items()))}",
        "- offline_scope_only: true",
        "- no_auto_promotion: true",
        "",
        "Admission is a pre-training governance gate only. It does not start training, does not write models, does not touch paper/live/execution paths, and does not imply promotion.",
    ]
    return "\n".join(lines) + "\n"


def config_snapshot(policy: AdmissionPolicy) -> dict[str, Any]:
    return flatten_dict("", asdict(policy))
