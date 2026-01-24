from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from octa_audit.continuous_audit import ContinuousAudit
from octa_governance.continuous_review import ContinuousReviewLoop

try:
    from octa_compliance.regulatory_adapt import RegulatoryAdaptation, Rule
except Exception:  # pragma: no cover
    RegulatoryAdaptation = None  # type: ignore
    Rule = None  # type: ignore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(obj: Any) -> Any:
    """Best-effort JSON sanitization (no heavy deps, no recursion bombs)."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (list, tuple)):
        return [_safe_json(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _safe_json(v) for k, v in obj.items()}
    # pydantic v2
    if hasattr(obj, "model_dump"):
        try:
            return _safe_json(obj.model_dump())
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict"):
        try:
            return _safe_json(obj.dict())
        except Exception:
            pass
    return str(obj)


def _governance_trigger(
    cycle: str,
    *,
    symbol: str,
    run_id: str,
    passed: bool,
    participants: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    if not passed:
        return None

    loop = ContinuousReviewLoop()
    notes = f"symbol={symbol} run_id={run_id} event=tradeable_artifact_created"
    participants = participants or ["auto"]

    cycle_norm = str(cycle or "").strip().lower()
    if cycle_norm == "daily_risk":
        rec = loop.trigger_daily_risk_review(participants=participants, notes=notes)
    elif cycle_norm == "monthly_committee":
        rec = loop.trigger_monthly_committee(participants=participants, notes=notes)
    else:
        rec = loop.trigger_weekly_strategy_review(participants=participants, notes=notes)

    return {
        "cycle": rec.cycle,
        "ts": rec.ts,
        "evidence_hash": rec.evidence_hash,
        "notes": notes,
        "participants": participants,
    }


def _regulatory_check(cfg) -> Optional[Dict[str, Any]]:
    """Optional wiring for RegulatoryAdaptation.

    If enabled and rules provided, we ingest them into a fresh in-memory RA instance
    and return its verified hash-chain status + evolution log.

    This is intentionally non-authoritative unless caller provides real rules.
    """

    a = getattr(cfg, "assurance", None)
    if a is None:
        return None
    if not bool(getattr(a, "regulatory_enabled", False)):
        return None
    rules = getattr(a, "regulatory_rules", None) or []

    if RegulatoryAdaptation is None or Rule is None:
        return {"enabled": True, "available": False, "reason": "octa_compliance unavailable"}

    ra = RegulatoryAdaptation()
    ingested = 0
    errors: List[str] = []

    mode = str(getattr(a, "regulatory_compatibility_mode", "strict") or "strict")

    for r in rules:
        try:
            if not isinstance(r, dict):
                raise ValueError("rule must be a dict")
            rule = Rule(
                rule_id=str(r.get("rule_id")),
                version=str(r.get("version")),
                jurisdiction=str(r.get("jurisdiction", "")),
                effective_date=str(r.get("effective_date", "")),
                content=dict(r.get("content") or {}),
                metadata=dict(r.get("metadata") or {}),
                parent=r.get("parent"),
            )
            if rule.parent:
                ra.add_rule_version(
                    user=str(r.get("user", "compliance")),
                    rule_id=rule.rule_id,
                    new_rule=rule,
                    compatibility_mode=mode,
                )
            else:
                ra.add_rule(user=str(r.get("user", "compliance")), rule=rule)
            ingested += 1
        except Exception as e:
            errors.append(str(e))

    return {
        "enabled": True,
        "available": True,
        "rules_ingested": ingested,
        "errors": errors,
        "evolution_log_ok": bool(ra.verify_evolution_log()),
        "evolution_log": _safe_json(ra.evolution_log()),
    }


def emit_assurance_report(
    *,
    cfg: Any,
    symbol: str,
    run_id: str,
    passed: bool,
    reasons: Optional[List[str]] = None,
    safe_mode: bool = False,
    asset_class: Optional[str] = None,
    parquet_path: Optional[str] = None,
    parquet_stat: Optional[Dict[str, Any]] = None,
    metrics_summary: Optional[Dict[str, Any]] = None,
    pack_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create and persist run-level evidence.

    Returns a small dict meant to be embedded in PipelineResult.pack_result.
    """

    a = getattr(cfg, "assurance", None)
    if a is None or not bool(getattr(a, "enabled", False)):
        return {"enabled": False}

    ts = _now_iso()

    ca = ContinuousAudit()

    # Compact config snapshot (avoid dumping entire object graph)
    cfg_snapshot: Optional[Dict[str, Any]] = None
    if bool(getattr(a, "include_config", True)):
        cfg_snapshot = {
            "features": _safe_json(getattr(cfg, "features", None)),
            "signal": _safe_json(getattr(cfg, "signal", None)),
            "broker": _safe_json(getattr(cfg, "broker", None)),
            "splits": _safe_json(getattr(cfg, "splits", None)),
            "gates": _safe_json(getattr(cfg, "gates", None)),
            "packaging": _safe_json(getattr(cfg, "packaging", None)),
        }

    components = {
        "ts": ts,
        "symbol": symbol,
        "run_id": run_id,
        "passed": bool(passed),
        "safe_mode": bool(safe_mode),
        "asset_class": asset_class,
        "parquet": {
            "path": parquet_path,
            "stat": _safe_json(parquet_stat or {}),
        },
        "metrics_summary": _safe_json(metrics_summary or {}),
        "pack_result": _safe_json(pack_result or {}),
        "reasons": list(reasons or []),
        "cfg": cfg_snapshot,
    }

    snapshot_id = ca.take_snapshot("training_run", components)

    attestation_hash = None
    if bool(getattr(a, "compliance_attestation", True)):
        statement = (
            "Training run executed with PASS-only tradeable packaging; "
            f"safe_mode={bool(safe_mode)}; "
            f"debug_on_fail={bool(getattr(getattr(cfg, 'packaging', None), 'save_debug_on_fail', False))}"
        )
        attestation_hash = ca.attest_compliance(
            "training_pipeline_policy",
            attestor="octa_training.pipeline",
            statement=statement,
        )

    ca.record_control_effectiveness(
        "training.gates",
        "PASS" if passed else "FAIL",
        notes=";".join((reasons or [])[:10]) or None,
    )

    # Governance wiring: trigger a review record when a *tradeable* artifact was created.
    governance_record = None
    tradeable_saved = bool(pack_result and pack_result.get("saved") and pack_result.get("artifact_kind") != "debug")
    if tradeable_saved and bool(getattr(a, "governance_review_on_tradeable", True)):
        governance_record = _governance_trigger(
            str(getattr(a, "governance_cycle", "weekly_strategy")),
            symbol=symbol,
            run_id=run_id,
            passed=True,
        )

    regulatory = _regulatory_check(cfg)

    report = {
        "ts": ts,
        "symbol": symbol,
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "attestation_hash": attestation_hash,
        "governance": governance_record,
        "regulatory": regulatory,
        "audit_log": ca.get_audit_log(),
    }

    out_dir = Path(cfg.paths.reports_dir) / str(getattr(a, "report_subdir", "assurance"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{symbol}__{run_id}__assurance.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "enabled": True,
        "report_path": str(out_path),
        "snapshot_id": snapshot_id,
        "attestation_hash": attestation_hash,
        "governance_evidence_hash": (governance_record or {}).get("evidence_hash") if governance_record else None,
    }
