from dataclasses import dataclass
from typing import Any, Dict

from octa_ip.externalization_scan import ExternalizationScanner
from octa_ip.ip_classifier import IPClassifier
from octa_ip.module_map import ModuleMap
from octa_ip.policy_enforcer import PolicyEnforcer
from octa_ip.valuation_meta import ValuationEngine


@dataclass
class IPDashboard:
    module_map: ModuleMap
    classifier: IPClassifier
    enforcer: PolicyEnforcer
    scanner: ExternalizationScanner
    valuation_engine: ValuationEngine

    def summary(
        self,
        usage_counts: Dict[str, int] | None = None,
        revenue_map: Dict[str, float] | None = None,
    ) -> Dict[str, Any]:
        usage_counts = usage_counts or {}
        revenue_map = revenue_map or {}

        # IP categories (deterministic ordering)
        categories = {
            m: self.classifier.classifications.get(m)
            for m in sorted(self.module_map.modules.keys())
        }

        # Policy violations (non-raising check)
        violations = self.enforcer.check_policies(self.module_map, self.classifier)
        violations_out = [
            {"module": v.module, "violation": v.violation, "details": v.details}
            for v in violations
        ]

        # Valuation metadata
        valuation = self.valuation_engine.compute_metadata(
            self.module_map, usage_counts, revenue_map
        )

        # Risk-critical components: sort by risk_criticality desc deterministically
        risk_sorted = sorted(
            [(m, valuation[m]["risk_criticality"]) for m in sorted(valuation.keys())],
            key=lambda x: (-x[1], x[0]),
        )
        risk_critical = [m for m, _ in risk_sorted]

        # Externalizable modules
        scans = self.scanner.scan_all(self.module_map)
        externalizable = [m for m, r in sorted(scans.items()) if r.ready]

        return {
            "ip_categories": categories,
            "policy_violations": violations_out,
            "valuation_metadata": valuation,
            "risk_critical_components": risk_critical,
            "externalizable_modules": externalizable,
        }


__all__ = ["IPDashboard"]
