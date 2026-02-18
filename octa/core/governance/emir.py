"""EMIR (European Market Infrastructure Regulation) compliance gate.

Enforces that derivatives trading requires known EMIR delegation status.
If the delegation status is unknown or non-compliant, derivatives are BLOCKED.

Equities are not affected by EMIR checks.

Configuration file format::

    {
      "delegation": {
        "clearing_obligation": "delegated",
        "reporting_obligation": "delegated",
        "risk_mitigation": "self",
        "delegation_entity": "Acme Clearing GmbH",
        "delegation_lei": "529900ABCDEF12345678"
      },
      "asset_class_scope": ["options", "futures", "swaps", "fx_forwards"]
    }
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

_DERIVATIVES_CLASSES = frozenset({
    "options", "futures", "swaps", "fx_forwards",
    "cfd", "fx_options", "ir_swaps", "credit_default_swaps",
})

_EQUITY_CLASSES = frozenset({
    "equity", "equities", "stock", "stocks", "etf",
})

_VALID_DELEGATION = frozenset({"delegated", "self"})


@dataclass(frozen=True)
class EMIRCheckResult:
    compliant: bool
    reason: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class EMIRConfig:
    clearing_obligation: str  # "delegated" | "self" | "unknown"
    reporting_obligation: str
    risk_mitigation: str
    delegation_entity: Optional[str]
    delegation_lei: Optional[str]
    asset_class_scope: frozenset[str]


def load_emir_config(path: Path) -> Optional[EMIRConfig]:
    """Load EMIR configuration from a JSON file."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    delegation = data.get("delegation", {})
    if not isinstance(delegation, dict):
        return None
    scope_raw = data.get("asset_class_scope", [])
    scope = frozenset(str(s).lower().strip() for s in scope_raw if str(s).strip())
    return EMIRConfig(
        clearing_obligation=str(delegation.get("clearing_obligation", "unknown")).lower().strip(),
        reporting_obligation=str(delegation.get("reporting_obligation", "unknown")).lower().strip(),
        risk_mitigation=str(delegation.get("risk_mitigation", "unknown")).lower().strip(),
        delegation_entity=delegation.get("delegation_entity"),
        delegation_lei=delegation.get("delegation_lei"),
        asset_class_scope=scope or _DERIVATIVES_CLASSES,
    )


def is_derivative(asset_class: str) -> bool:
    """Return True if the asset class is a derivative."""
    return str(asset_class).lower().strip() in _DERIVATIVES_CLASSES


def is_equity(asset_class: str) -> bool:
    """Return True if the asset class is equity."""
    return str(asset_class).lower().strip() in _EQUITY_CLASSES


def check_emir_compliance(
    asset_class: str,
    emir_config: Optional[EMIRConfig],
) -> EMIRCheckResult:
    """Check EMIR compliance for a given asset class.

    Equities always pass.  Derivatives require valid EMIR delegation.

    Returns
    -------
    EMIRCheckResult
        ``compliant=True`` if allowed to trade.
    """
    ac = str(asset_class).lower().strip()

    # Equities are always allowed
    if is_equity(ac):
        return EMIRCheckResult(
            compliant=True,
            reason="EQUITY_EXEMPT",
            details={"asset_class": ac},
        )

    # Non-derivative, non-equity: pass through
    if not is_derivative(ac):
        return EMIRCheckResult(
            compliant=True,
            reason="NOT_IN_SCOPE",
            details={"asset_class": ac},
        )

    # Derivative: require EMIR config
    if emir_config is None:
        return EMIRCheckResult(
            compliant=False,
            reason="EMIR_CONFIG_MISSING",
            details={"asset_class": ac},
        )

    # Check each obligation
    issues = []
    if emir_config.clearing_obligation not in _VALID_DELEGATION:
        issues.append(f"clearing={emir_config.clearing_obligation}")
    if emir_config.reporting_obligation not in _VALID_DELEGATION:
        issues.append(f"reporting={emir_config.reporting_obligation}")
    if emir_config.risk_mitigation not in _VALID_DELEGATION:
        issues.append(f"risk_mitigation={emir_config.risk_mitigation}")

    if issues:
        return EMIRCheckResult(
            compliant=False,
            reason="EMIR_DELEGATION_UNKNOWN",
            details={
                "asset_class": ac,
                "issues": issues,
                "config": {
                    "clearing": emir_config.clearing_obligation,
                    "reporting": emir_config.reporting_obligation,
                    "risk_mitigation": emir_config.risk_mitigation,
                },
            },
        )

    return EMIRCheckResult(
        compliant=True,
        reason="EMIR_COMPLIANT",
        details={
            "asset_class": ac,
            "delegation_entity": emir_config.delegation_entity,
        },
    )
