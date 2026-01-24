from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import yaml

from octa_core.bootstrap.env_check import run_env_checks
from octa_core.security.audit import AuditLog


@dataclass(frozen=True)
class StartDecision:
    ok: bool
    warnings: List[str]
    errors: List[str]


def _count_tradeable_symbols(artifacts_root: str) -> int:
    # Conservative heuristic: count pkl files under artifacts/models or artifacts/runs promotions.
    root = Path(artifacts_root)
    if not root.exists():
        return 0

    pkl = list(root.rglob("*.pkl"))
    # De-dup by symbol filename stem
    stems = {p.stem for p in pkl}
    return len(stems)


def can_start_paper(*, features: dict, thresholds: dict, artifacts_root: str = "artifacts") -> StartDecision:
    warnings: List[str] = []
    errors: List[str] = []

    min_free = int(thresholds.get("min_free_disk_gb", 10) or 10)
    max_warn = int(thresholds.get("max_data_dir_gb_warn", 200) or 200)
    env = run_env_checks(min_free_gb=min_free, max_data_dir_gb_warn=max_warn)
    warnings.extend(env.warnings)
    errors.extend(env.errors)

    # Audit must be writable when enabled.
    audit_cfg = ((features.get("security") or {}).get("audit_log") or {}) if isinstance(features.get("security"), dict) else {}
    if bool(audit_cfg.get("enabled", True)):
        ap = str(audit_cfg.get("path", "artifacts/security/audit.jsonl"))
        try:
            AuditLog(path=ap).append(event_type="start_guard", payload={"mode": "paper"})
        except Exception as e:
            errors.append(f"audit_not_writable: {e}")

    # Require some artifacts in paper mode (fail-closed).
    min_syms = int(thresholds.get("min_tradeable_symbols_paper", 1) or 1)
    n = _count_tradeable_symbols(artifacts_root)
    if n < min_syms:
        errors.append(f"insufficient_tradeable_symbols_for_paper: {n} < {min_syms}")

    return StartDecision(ok=(len(errors) == 0), warnings=warnings, errors=errors)


def can_start_live(
    *,
    features: dict,
    modes: dict,
    thresholds: dict,
    artifacts_root: str = "artifacts",
    opengamma_health_ok: Optional[bool] = None,
    ibkr_health_ok: Optional[bool] = None,
    accounting_enabled: Optional[bool] = None,
    safe_stop_dry_run_ok: Optional[bool] = None,
    telegram_ok: Optional[bool] = None,
) -> StartDecision:
    # Live builds on paper checks.
    d = can_start_paper(features=features, thresholds=thresholds, artifacts_root=artifacts_root)
    warnings = list(d.warnings)
    errors = list(d.errors)

    allow_live = bool((modes.get("allow_live", False)) if isinstance(modes, dict) else False)
    if not allow_live:
        errors.append("live_not_allowed: set modes.allow_live=true in octa_features.yaml")

    min_syms = int(thresholds.get("min_tradeable_symbols_live", 1) or 1)
    n = _count_tradeable_symbols(artifacts_root)
    if n < min_syms:
        errors.append(f"insufficient_tradeable_symbols_for_live: {n} < {min_syms}")

    # IBKR connectivity required for live.
    if ibkr_health_ok is False:
        errors.append("ibkr_health_check_failed")
    if ibkr_health_ok is None:
        warnings.append("ibkr_health_check_not_run")

    # OpenGamma required only when configured.
    og_required = bool(((features.get("opengamma") or {}).get("required_for_live")) if isinstance(features.get("opengamma"), dict) else False)
    if og_required:
        if opengamma_health_ok is False:
            errors.append("opengamma_required_but_unhealthy")
        if opengamma_health_ok is None:
            errors.append("opengamma_required_but_not_checked")

    # Accounting requirement: allow explicit override by env if desired.
    if accounting_enabled is False and os.getenv("OCTA_LIVE_ALLOW_NO_ACCOUNTING", "").strip().lower() not in {"1", "true", "yes"}:
        errors.append("accounting_disabled_for_live: enable features.accounting.enabled or set OCTA_LIVE_ALLOW_NO_ACCOUNTING=1")

    if safe_stop_dry_run_ok is False:
        errors.append("safe_stop_dry_run_failed")

    if telegram_ok is False:
        errors.append("telegram_alerts_not_ok")

    return StartDecision(ok=(len(errors) == 0), warnings=warnings, errors=errors)


def load_features_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


__all__ = [
    "StartDecision",
    "can_start_paper",
    "can_start_live",
    "load_features_yaml",
]
