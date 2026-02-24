from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


IMMUTABLE_PROD_BLOCK = "IMMUTABLE_PROD_BLOCK"
_PROD_MODES = {"shadow", "paper", "live"}
_EXECUTION_ENTRYPOINTS = {"runner", "run_octa", "execution_service", "octa_os_start"}


@dataclass(frozen=True)
class ImmutabilityDecision:
    blocked: bool
    mode: str
    production_context: bool
    fail_closed: bool
    reason: str
    incident_path: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _ctx_get(ctx: Mapping[str, Any] | None, key: str, default: Any = None) -> Any:
    if not isinstance(ctx, Mapping):
        return default
    return ctx.get(key, default)


def _mode(ctx: Mapping[str, Any] | None) -> str:
    return str(_ctx_get(ctx, "mode", "") or "").strip().lower()


def is_production_context(ctx: Mapping[str, Any] | None) -> bool:
    mode = _mode(ctx)
    execution_active = _safe_bool(_ctx_get(ctx, "execution_active", False))
    if mode in _PROD_MODES and execution_active:
        return True

    policy_flags = _ctx_get(ctx, "policy_flags", {})
    if isinstance(policy_flags, Mapping):
        if _safe_bool(policy_flags.get("default_execution_enabled", False)):
            return True
        if _safe_bool(policy_flags.get("require_blessed_1d_1h", False)):
            return True

    entrypoint = str(_ctx_get(ctx, "entrypoint", "") or "").strip().lower()
    if entrypoint in _EXECUTION_ENTRYPOINTS:
        return True

    run_id = str(_ctx_get(ctx, "run_id", "") or "").strip().lower()
    if "execution" in run_id:
        return True
    return False


def should_fail_closed(ctx: Mapping[str, Any] | None) -> bool:
    return _mode(ctx) in {"paper", "live"}


def emit_immutable_incident(
    ctx: Mapping[str, Any] | None,
    *,
    operation: str,
    target: str,
    details: Mapping[str, Any] | None = None,
    evidence_root: str | Path | None = None,
) -> Path:
    root = Path(evidence_root) if evidence_root else Path("octa") / "var" / "evidence" / "immutable_guards"
    root.mkdir(parents=True, exist_ok=True)
    ts = _now_iso()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    mode = _mode(ctx)
    payload = {
        "timestamp": ts,
        "reason": IMMUTABLE_PROD_BLOCK,
        "mode": mode,
        "service": str(_ctx_get(ctx, "service", "") or ""),
        "run_id": str(_ctx_get(ctx, "run_id", "") or ""),
        "entrypoint": str(_ctx_get(ctx, "entrypoint", "") or ""),
        "execution_active": _safe_bool(_ctx_get(ctx, "execution_active", False)),
        "production_context": bool(is_production_context(ctx)),
        "fail_closed": bool(should_fail_closed(ctx)),
        "operation": str(operation),
        "target": str(target),
        "details": dict(details or {}),
    }
    path = root / f"{safe_ts}_{operation}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def assert_write_allowed(
    ctx: Mapping[str, Any] | None,
    *,
    operation: str,
    target: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    if not is_production_context(ctx):
        return

    incident_path = emit_immutable_incident(
        ctx,
        operation=operation,
        target=target,
        details=details,
    )
    mode = _mode(ctx)
    msg = f"{IMMUTABLE_PROD_BLOCK}:{mode}:{operation}:{target}:{incident_path}"
    raise RuntimeError(msg)


def evaluate_write_permission(
    ctx: Mapping[str, Any] | None,
    *,
    operation: str,
    target: str,
    details: Mapping[str, Any] | None = None,
) -> ImmutabilityDecision:
    production = is_production_context(ctx)
    fail_closed = should_fail_closed(ctx)
    incident_path = ""
    if production:
        incident_path = str(
            emit_immutable_incident(
                ctx,
                operation=operation,
                target=target,
                details=details,
            )
        )
    return ImmutabilityDecision(
        blocked=production,
        mode=_mode(ctx),
        production_context=production,
        fail_closed=fail_closed,
        reason=IMMUTABLE_PROD_BLOCK if production else "write_allowed",
        incident_path=incident_path,
    )
