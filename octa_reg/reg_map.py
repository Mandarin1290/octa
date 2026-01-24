from pathlib import Path
from typing import Dict, List, Optional

# Logical regulatory domains
DOMAINS = [
    "market_abuse_prevention",
    "risk_management",
    "record_keeping",
    "investor_protection",
    "operational_resilience",
]


DEFAULT_CONTROLS = {
    "market_abuse_prevention": ["pre_trade_checks", "surveillance", "order_limits"],
    "risk_management": ["limit_monitoring", "stress_tests", "capital_checks"],
    "record_keeping": ["immutable_logs", "retention_policy", "audit_trail"],
    "investor_protection": [
        "disclosures",
        "suitability_checks",
        "segregation_of_assets",
    ],
    "operational_resilience": ["redundancy", "monitoring", "incident_response"],
}


def discover_components(root: Optional[str] = None) -> List[str]:
    """Discover top-level `octa_` components in the repo root.

    Returns a sorted list of directory names starting with `octa_`.
    """
    base = Path(root) if root else Path(__file__).resolve().parents[1]
    comps = [
        p.name for p in base.iterdir() if p.is_dir() and p.name.startswith("octa_")
    ]
    return sorted(comps)


def _heuristic_map(component: str) -> List[str]:
    name = component.lower()
    domains = set()

    # market abuse prevention: related to trading, strategies, alpha
    if any(
        k in name
        for k in ("strategy", "strategies", "alpha", "trading", "vertex", "atlas")
    ):
        domains.update(["market_abuse_prevention", "risk_management"])

    # risk management: capital, fund, risk, ledger
    if any(k in name for k in ("fund", "capital", "ledger", "risk")):
        domains.update(["risk_management", "record_keeping", "investor_protection"])

    # record keeping: ledger, reports, ledger-like modules
    if any(k in name for k in ("ledger", "reports", "nexus")):
        domains.add("record_keeping")

    # investor protection: fund, capital, governance
    if any(k in name for k in ("fund", "capital", "governance", "investor")):
        domains.add("investor_protection")

    # operational resilience: ops, fabric, sentinel, stream, broker
    if any(
        k in name
        for k in ("ops", "fabric", "sentinel", "stream", "broker", "core", "failover")
    ):
        domains.add("operational_resilience")

    # fallback: operational resilience + record keeping to ensure coverage
    if not domains:
        domains.update(["operational_resilience", "record_keeping"])

    return sorted(domains)


def generate_mapping(root: Optional[str] = None) -> Dict[str, List[str]]:
    comps = discover_components(root)
    mapping = {c: _heuristic_map(c) for c in comps}
    return mapping


def validate_component(component: str, mapping: Dict[str, List[str]]) -> bool:
    """Return True if component has at least one mapped domain; else raise ValueError."""
    domains = mapping.get(component)
    if not domains:
        raise ValueError(f"component not mapped: {component}")
    return True


def validate_all(
    mapping: Dict[str, List[str]], root: Optional[str] = None
) -> List[str]:
    """Validate mapping covers all discovered components.

    Returns list of unmapped components (empty if all covered). Also raises ValueError if any unmapped.
    """
    comps = discover_components(root)
    unmapped = [c for c in comps if not mapping.get(c)]
    if unmapped:
        raise ValueError(f"unmapped components: {unmapped}")
    return []


def control_requirements_for(domain: str) -> List[str]:
    return DEFAULT_CONTROLS.get(domain, [])


__all__ = [
    "DOMAINS",
    "discover_components",
    "generate_mapping",
    "validate_component",
    "validate_all",
    "control_requirements_for",
]
