from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from octa_capital.accounts import CapitalAccount, CapitalError


@dataclass
class CapitalSources:
    """Manage internal vs investor capital pools.

    Enforces strict segregation: investor (external) capital never influences strategy decision pool.
    All movements are explicit and audited.
    """

    internal: CapitalAccount = field(default_factory=lambda: CapitalAccount("internal"))
    investors: Dict[str, CapitalAccount] = field(default_factory=dict)
    audit_log: list = field(default_factory=list)

    def _record(self, actor: str, action: str, details: Dict[str, Any]):
        import datetime

        self.audit_log.append(
            {
                "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "actor": actor,
                "action": action,
                "details": details,
            }
        )

    def create_investor(self, investor_id: str, actor: str = "system") -> None:
        if investor_id in self.investors:
            raise CapitalError("investor_exists")
        self.investors[investor_id] = CapitalAccount(f"investor:{investor_id}")
        self._record(actor, "investor_created", {"investor_id": investor_id})

    def deposit_internal(self, amount: float, actor: str = "system") -> None:
        self.internal.deposit(amount, actor=actor)
        self._record(
            actor,
            "deposit_internal",
            {"amount": amount, "new_total": self.internal.total_balance},
        )

    def deposit_investor(
        self, investor_id: str, amount: float, actor: str = "system"
    ) -> None:
        if investor_id not in self.investors:
            self.create_investor(investor_id, actor=actor)
        self.investors[investor_id].deposit(amount, actor=actor)
        self._record(
            actor,
            "deposit_investor",
            {
                "investor_id": investor_id,
                "amount": amount,
                "new_total": self.investors[investor_id].total_balance,
            },
        )

    def allocate_to_strategy(
        self, subaccount: str, amount: float, actor: str = "system"
    ) -> None:
        """Reserve capital for strategy decision and execution — must use internal pool only.

        This enforces the HARD RULE: external capital NEVER influences strategy logic.
        """
        # reserve only from internal pool
        self.internal.reserve(subaccount, amount, actor=actor)
        self._record(
            actor, "allocate_to_strategy", {"subaccount": subaccount, "amount": amount}
        )

    def allocate_from_investor(
        self, investor_id: str, subaccount: str, amount: float, actor: str = "system"
    ) -> None:
        """Reserve investor capital for execution/settlement; must NOT be used for decision-making.

        Manager must ensure strategy logic only queries `strategy_deployable()` which excludes investor capital.
        """
        if investor_id not in self.investors:
            self.create_investor(investor_id, actor=actor)
        self.investors[investor_id].reserve(subaccount, amount, actor=actor)
        self._record(
            actor,
            "allocate_from_investor",
            {"investor_id": investor_id, "subaccount": subaccount, "amount": amount},
        )

    def release_from_investor(
        self, investor_id: str, subaccount: str, amount: float, actor: str = "system"
    ) -> None:
        if investor_id not in self.investors:
            raise CapitalError("investor_not_found")
        self.investors[investor_id].release(subaccount, amount, actor=actor)
        self._record(
            actor,
            "release_from_investor",
            {"investor_id": investor_id, "subaccount": subaccount, "amount": amount},
        )

    def strategy_deployable(self) -> float:
        """Return deployable capital that strategies may use for decision-making — INTERNAL ONLY."""
        return self.internal.deployable

    def aggregate_view(self) -> Dict[str, Any]:
        """Return an accounting view separating internal and external pools."""
        inv_summary = {iid: acc.get_balances() for iid, acc in self.investors.items()}
        return {
            "internal": self.internal.get_balances(),
            "investors": inv_summary,
            "strategy_deployable": self.strategy_deployable(),
        }
