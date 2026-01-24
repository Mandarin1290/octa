from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4


@dataclass(frozen=True)
class Hypothesis:
    hypothesis_id: str
    economic_intuition: str
    expected_regime: str
    expected_failure_modes: str
    risk_assumptions: str
    test_spec: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: str


class HypothesisRegistry:
    """Registry for immutable, testable hypotheses.

    Rules enforced:
    - No free-form: required fields must be non-empty.
    - Testable: `test_spec` must be provided in order to register.
    - Immutable: registered `Hypothesis` instances are frozen and cannot be modified.
    """

    def __init__(self):
        self._store: Dict[str, Hypothesis] = {}

    def _validate_required(
        self,
        economic_intuition: str,
        expected_regime: str,
        expected_failure_modes: str,
        risk_assumptions: str,
        test_spec: Dict[str, Any],
    ):
        if not economic_intuition:
            raise ValueError("economic_intuition is required and must be non-empty")
        if not expected_regime:
            raise ValueError("expected_regime is required and must be non-empty")
        if not expected_failure_modes:
            raise ValueError("expected_failure_modes is required and must be non-empty")
        if not risk_assumptions:
            raise ValueError("risk_assumptions is required and must be non-empty")
        if not isinstance(test_spec, dict) or not test_spec:
            raise ValueError(
                "test_spec (dict) is required to ensure hypothesis is testable"
            )

    def register(
        self,
        economic_intuition: str,
        expected_regime: str,
        expected_failure_modes: str,
        risk_assumptions: str,
        test_spec: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        hypothesis_id: Optional[str] = None,
    ) -> Hypothesis:
        self._validate_required(
            economic_intuition,
            expected_regime,
            expected_failure_modes,
            risk_assumptions,
            test_spec,
        )
        metadata = metadata or {}
        hid = hypothesis_id or uuid4().hex
        if hid in self._store:
            raise ValueError(f"hypothesis_id {hid} already registered")
        created = datetime.now(timezone.utc).isoformat()
        hyp = Hypothesis(
            hypothesis_id=hid,
            economic_intuition=economic_intuition,
            expected_regime=expected_regime,
            expected_failure_modes=expected_failure_modes,
            risk_assumptions=risk_assumptions,
            test_spec=test_spec,
            metadata=metadata,
            created_at=created,
        )
        self._store[hid] = hyp
        return hyp

    def get(self, hypothesis_id: str) -> Hypothesis:
        return self._store[hypothesis_id]

    def list(self) -> Dict[str, Hypothesis]:
        return dict(self._store)
