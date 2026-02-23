"""Derivatives governance blocking gate.

Combines LEI + EMIR checks into a single fail-closed gate for derivatives.
Equities are always passed through.

Usage in execution preflight::

    gate = DerivativesGate.from_config(lei_path, emir_path)
    result = gate.check("options", lei="529900...")
    if not result.allowed:
        # BLOCK the order
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from .emir import EMIRConfig, check_emir_compliance, is_derivative, is_equity, load_emir_config
from .lei_registry import LEICheckResult, LEIRegistry


@dataclass(frozen=True)
class DerivativesGateResult:
    allowed: bool
    asset_class: str
    lei_check: Optional[LEICheckResult]
    emir_compliant: bool
    reason: str
    details: Dict[str, Any]


class DerivativesGate:
    """Combined LEI + EMIR gate for derivatives trading."""

    def __init__(
        self,
        lei_registry: LEIRegistry,
        emir_config: Optional[EMIRConfig],
    ) -> None:
        self._lei = lei_registry
        self._emir = emir_config

    @classmethod
    def from_config(
        cls,
        lei_path: Optional[Path] = None,
        emir_path: Optional[Path] = None,
    ) -> "DerivativesGate":
        lei = LEIRegistry.from_file(lei_path) if lei_path else LEIRegistry()
        emir = load_emir_config(emir_path) if emir_path else None
        return cls(lei, emir)

    def check(
        self,
        asset_class: str,
        *,
        lei: Optional[str] = None,
        as_of: Optional[date] = None,
    ) -> DerivativesGateResult:
        """Check if a trade in the given asset class is allowed.

        Equities always pass.
        Derivatives require BOTH valid LEI AND valid EMIR status.
        """
        ac = str(asset_class).lower().strip()

        # Equities pass unconditionally
        if is_equity(ac):
            return DerivativesGateResult(
                allowed=True,
                asset_class=ac,
                lei_check=None,
                emir_compliant=True,
                reason="EQUITY_EXEMPT",
                details={},
            )

        # Non-derivatives, non-equities: pass through
        if not is_derivative(ac):
            return DerivativesGateResult(
                allowed=True,
                asset_class=ac,
                lei_check=None,
                emir_compliant=True,
                reason="NOT_IN_SCOPE",
                details={},
            )

        # --- Derivative: enforce LEI ---
        lei_result = self._lei.check(lei, as_of=as_of)

        # --- Derivative: enforce EMIR ---
        emir_result = check_emir_compliance(ac, self._emir)

        allowed = lei_result.valid and emir_result.compliant

        reasons = []
        if not lei_result.valid:
            reasons.append(f"LEI:{lei_result.reason}")
        if not emir_result.compliant:
            reasons.append(f"EMIR:{emir_result.reason}")

        reason = ",".join(reasons) if reasons else "DERIVATIVES_ALLOWED"

        return DerivativesGateResult(
            allowed=allowed,
            asset_class=ac,
            lei_check=lei_result,
            emir_compliant=emir_result.compliant,
            reason=reason,
            details={
                "lei": lei_result.__dict__,
                "emir": emir_result.__dict__,
            },
        )

    def status(self) -> Dict[str, Any]:
        """Return a summary of the derivatives gate configuration."""
        return {
            "lei_entries": len(self._lei.list_entries()),
            "emir_configured": self._emir is not None,
            "emir_clearing": self._emir.clearing_obligation if self._emir else None,
            "emir_reporting": self._emir.reporting_obligation if self._emir else None,
        }
