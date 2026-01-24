import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class LongevityCert:
    ts: str
    cert_id: str
    stability_metrics: Dict[str, Any]
    drift_history: List[Dict[str, Any]]
    retired_strategies: List[Dict[str, Any]]
    unresolved_structural_risks: List[Dict[str, Any]]
    evidence_hash: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_get_alerts(obj) -> List[Dict[str, Any]]:
    if obj is None:
        return []
    if hasattr(obj, "get_alerts"):
        try:
            return [
                a.__dict__ if hasattr(a, "__dict__") else a for a in obj.get_alerts()
            ]
        except Exception:
            return []
    if hasattr(obj, "alerts"):
        try:
            return [a.__dict__ if hasattr(a, "__dict__") else a for a in obj.alerts]
        except Exception:
            return []
    return []


def _safe_recent_audit(obj, limit: int = 20) -> List[Dict[str, Any]]:
    if obj is None:
        return []
    logs = []
    if hasattr(obj, "get_audit"):
        try:
            logs = obj.get_audit()
        except Exception:
            logs = []
    elif hasattr(obj, "audit_log"):
        try:
            logs = obj.audit_log
        except Exception:
            logs = []
    if not isinstance(logs, list):
        return []
    return list(logs[-limit:])


def _safe_retired_strategies(sunset_engine) -> List[Dict[str, Any]]:
    if sunset_engine is None:
        return []
    out = []
    # try history attribute first
    if hasattr(sunset_engine, "history"):
        try:
            for rec in sunset_engine.history:
                if hasattr(rec, "state") and rec.state == "retired":
                    out.append(rec.__dict__ if hasattr(rec, "__dict__") else rec)
        except Exception:
            out = []
    # fallback to audit_log search
    if not out and hasattr(sunset_engine, "get_audit"):
        try:
            for e in sunset_engine.get_audit():
                if e.get("action") in ("shutdown_complete", "sunset_executed"):
                    out.append(e)
        except Exception:
            pass
    return out


def generate_longevity_cert(
    stability_monitor: Optional[Any] = None,
    audit_engine: Optional[Any] = None,
    sunset_engine: Optional[Any] = None,
    cost_monitor: Optional[Any] = None,
    regime_system: Optional[Any] = None,
    model_refresh: Optional[Any] = None,
) -> LongevityCert:
    """Generate an evidence-based longevity certification.

    The function pulls recent metrics and alerts from provided components where available.
    It is defensive: missing components are tolerated and produce empty sections.
    """
    ts = _now_iso()

    # stability metrics: try to extract rolling stats or summary
    stability_metrics: Dict[str, Any] = {}
    try:
        if stability_monitor is not None:
            if hasattr(stability_monitor, "rolling_stats"):
                stability_metrics = (
                    stability_monitor.rolling_stats()
                    if callable(stability_monitor.rolling_stats)
                    else {}
                )
            elif hasattr(stability_monitor, "get_alerts"):
                stability_metrics = {"alerts": _safe_get_alerts(stability_monitor)}
    except Exception:
        stability_metrics = {"error": "failed to extract stability metrics"}

    # drift history: collect recent alerts from monitors
    drift_history: List[Dict[str, Any]] = []
    drift_history.extend(_safe_get_alerts(cost_monitor))
    drift_history.extend(_safe_get_alerts(regime_system))
    drift_history.extend(_safe_get_alerts(stability_monitor))

    # include recent audit events as part of drift timeline
    if audit_engine is not None:
        drift_history.extend(_safe_recent_audit(audit_engine, limit=50))

    # retired strategies
    retired = _safe_retired_strategies(sunset_engine)

    # unresolved structural risks: by default, collect open alerts marked critical or advisory
    unresolved: List[Dict[str, Any]] = []
    for src in (regime_system, cost_monitor, stability_monitor):
        for a in _safe_get_alerts(src):
            sev = (
                a.get("severity")
                if isinstance(a, dict)
                else getattr(a, "severity", None)
            )
            if sev in ("warning", "critical", "advisory"):
                unresolved.append(a if isinstance(a, dict) else a.__dict__)

    # model refresh issues: pending unapproved retrains
    if model_refresh is not None and hasattr(model_refresh, "_models"):
        try:
            for mid, info in model_refresh._models.items():
                pending = info.get("pending")
                approved = info.get("approved")
                if pending and not approved:
                    unresolved.append(
                        {"type": "pending_retrain", "model_id": mid, "pending": pending}
                    )
        except Exception:
            pass

    report = {
        "ts": ts,
        "stability_metrics": stability_metrics,
        "drift_history": drift_history,
        "retired_strategies": retired,
        "unresolved_structural_risks": unresolved,
    }

    evidence = canonical_hash(report)
    cert = LongevityCert(
        ts=ts,
        cert_id=canonical_hash({"ts": ts, "report_hash": evidence})[:16],
        stability_metrics=stability_metrics,
        drift_history=drift_history,
        retired_strategies=retired,
        unresolved_structural_risks=unresolved,
        evidence_hash=evidence,
    )
    # append to audit engine if available
    try:
        if audit_engine is not None and hasattr(audit_engine, "audit_log"):
            audit_engine.audit_log.append(
                {"ts": ts, "action": "longevity_cert", "evidence_hash": evidence}
            )
    except Exception:
        pass
    return cert


def validate_cert(cert: LongevityCert) -> bool:
    payload = {
        "ts": cert.ts,
        "stability_metrics": cert.stability_metrics,
        "drift_history": cert.drift_history,
        "retired_strategies": cert.retired_strategies,
        "unresolved_structural_risks": cert.unresolved_structural_risks,
    }
    return canonical_hash(payload) == cert.evidence_hash
