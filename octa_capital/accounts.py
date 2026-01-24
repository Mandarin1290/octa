from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Dict, List


class CapitalError(Exception):
    pass


@dataclass
class CapitalAccount:
    account_id: str
    total_balance: float = 0.0
    # reserved balances per sub-account/strategy
    reserved: Dict[str, float] = field(default_factory=dict)
    audit_log: List[Dict[str, object]] = field(default_factory=list)

    def _ts(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def record_audit(self, actor: str, action: str, details: Dict[str, object]) -> None:
        self.audit_log.append(
            {"ts": self._ts(), "actor": actor, "action": action, "details": details}
        )

    @property
    def deployable(self) -> float:
        return round(self.total_balance - sum(self.reserved.values()), 8)

    def deposit(self, amount: float, actor: str = "system") -> None:
        if amount <= 0:
            raise CapitalError("deposit_amount_positive")
        self.total_balance = round(self.total_balance + float(amount), 8)
        self.record_audit(
            actor, "deposit", {"amount": amount, "new_total": self.total_balance}
        )

    def reserve(self, subaccount: str, amount: float, actor: str = "system") -> None:
        amount = float(amount)
        if amount <= 0:
            raise CapitalError("reserve_amount_positive")
        if amount > self.deployable:
            self.record_audit(
                actor,
                "reserve_failed",
                {
                    "subaccount": subaccount,
                    "amount": amount,
                    "reason": "insufficient_deployable",
                    "deployable": self.deployable,
                },
            )
            raise CapitalError("insufficient_deployable")

        self.reserved[subaccount] = round(
            self.reserved.get(subaccount, 0.0) + amount, 8
        )
        self.record_audit(
            actor,
            "reserve",
            {
                "subaccount": subaccount,
                "amount": amount,
                "reserved": self.reserved[subaccount],
                "deployable": self.deployable,
            },
        )

    def release(self, subaccount: str, amount: float, actor: str = "system") -> None:
        amount = float(amount)
        current = self.reserved.get(subaccount, 0.0)
        if amount <= 0 or amount > current:
            self.record_audit(
                actor,
                "release_failed",
                {
                    "subaccount": subaccount,
                    "amount": amount,
                    "reason": "invalid_release",
                    "reserved": current,
                },
            )
            raise CapitalError("invalid_release")
        self.reserved[subaccount] = round(current - amount, 8)
        self.record_audit(
            actor,
            "release",
            {
                "subaccount": subaccount,
                "amount": amount,
                "reserved": self.reserved[subaccount],
                "deployable": self.deployable,
            },
        )

    def consume_reserved(
        self, subaccount: str, amount: float, actor: str = "system"
    ) -> None:
        """Consume reserved capital for trading activities. This reduces both reserved and total_balance.

        Trading logic should never manipulate `total_balance` directly; use this method to represent committed capital movements.
        """
        amount = float(amount)
        current = self.reserved.get(subaccount, 0.0)
        if amount <= 0 or amount > current:
            self.record_audit(
                actor,
                "consume_failed",
                {
                    "subaccount": subaccount,
                    "amount": amount,
                    "reason": "insufficient_reserved",
                    "reserved": current,
                },
            )
            raise CapitalError("insufficient_reserved")

        self.reserved[subaccount] = round(current - amount, 8)
        self.total_balance = round(self.total_balance - amount, 8)
        self.record_audit(
            actor,
            "consume_reserved",
            {
                "subaccount": subaccount,
                "amount": amount,
                "reserved": self.reserved[subaccount],
                "new_total": self.total_balance,
            },
        )

    def transfer_between_subaccounts(
        self, from_sub: str, to_sub: str, amount: float, actor: str = "system"
    ) -> None:
        amount = float(amount)
        if amount <= 0:
            raise CapitalError("transfer_positive")
        from_current = self.reserved.get(from_sub, 0.0)
        if amount > from_current:
            self.record_audit(
                actor,
                "transfer_failed",
                {
                    "from": from_sub,
                    "to": to_sub,
                    "amount": amount,
                    "reason": "insufficient_reserved",
                    "from_reserved": from_current,
                },
            )
            raise CapitalError("insufficient_reserved_from")
        self.reserved[from_sub] = round(from_current - amount, 8)
        self.reserved[to_sub] = round(self.reserved.get(to_sub, 0.0) + amount, 8)
        self.record_audit(
            actor,
            "transfer",
            {
                "from": from_sub,
                "to": to_sub,
                "amount": amount,
                "from_reserved": self.reserved[from_sub],
                "to_reserved": self.reserved[to_sub],
            },
        )

    def get_balances(self) -> Dict[str, object]:
        return {
            "total": self.total_balance,
            "deployable": self.deployable,
            "reserved": dict(self.reserved),
        }
