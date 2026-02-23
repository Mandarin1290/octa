from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping


class Capability(str, Enum):
    READ_MARKET_DATA = "READ_MARKET_DATA"
    READ_STATE = "READ_STATE"
    WRITE_STATE = "WRITE_STATE"
    WRITE_EVIDENCE = "WRITE_EVIDENCE"
    WRITE_CANDIDATE_MODEL = "WRITE_CANDIDATE_MODEL"
    WRITE_BLESSED_MODEL = "WRITE_BLESSED_MODEL"
    ISSUE_ORDER_INTENT = "ISSUE_ORDER_INTENT"
    APPROVE_ORDER = "APPROVE_ORDER"
    SEND_ORDER = "SEND_ORDER"
    BROKER_CONNECT = "BROKER_CONNECT"
    DASHBOARD_START = "DASHBOARD_START"
    ALERT_SEND = "ALERT_SEND"


class CapabilityViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class CapabilityEnforcer:
    grants: Mapping[str, set[str]]

    @classmethod
    def from_policy(cls, mapping: Mapping[str, Iterable[str]]) -> "CapabilityEnforcer":
        grants = {str(name): {str(cap) for cap in caps} for name, caps in mapping.items()}
        return cls(grants=grants)

    def require(self, service: str, capability: Capability) -> None:
        granted = self.grants.get(service, set())
        if capability.value not in granted:
            raise CapabilityViolation(f"capability_denied:{service}:{capability.value}")

    def service_caps(self, service: str) -> list[str]:
        return sorted(self.grants.get(service, set()))
