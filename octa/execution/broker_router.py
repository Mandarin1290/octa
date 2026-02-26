from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter


@dataclass(frozen=True)
class BrokerRouterConfig:
    mode: str
    enable_live: bool = False
    i_understand_live_risk: bool = False
    enable_carry_live: bool = False
    i_understand_carry_risk: bool = False
    supported_instruments: tuple[str, ...] = ()


class BrokerRouter:
    def __init__(self, cfg: BrokerRouterConfig) -> None:
        self.cfg = cfg
        self.mode = str(cfg.mode).strip().lower()
        self._sandbox = IBKRContractAdapter(
            IBKRConfig(
                rate_limit_per_minute=120,
                supported_instruments=list(cfg.supported_instruments),
            )
        )

    def health_check(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "mode": self.mode,
            "live_enabled": bool(self.cfg.enable_live and self.cfg.i_understand_live_risk),
            "carry_live_enabled": bool(self.cfg.enable_carry_live and self.cfg.i_understand_carry_risk),
        }

    def account_snapshot(self) -> Dict[str, Any]:
        if self.mode == "dry-run":
            return {"positions": [], "buying_power": 0.0, "funding_rates": {}}
        return self._sandbox.account_snapshot()

    def place_order(self, *, strategy: str, order: Dict[str, Any]) -> Dict[str, Any]:
        strategy = str(strategy).strip().lower()
        if self.mode == "dry-run":
            return {
                "order_id": str(order.get("order_id", "")),
                "status": "SIMULATED",
                "mode": "dry-run",
                "strategy": strategy,
            }

        if self.mode == "live":
            if not (self.cfg.enable_live and self.cfg.i_understand_live_risk):
                return {
                    "order_id": str(order.get("order_id", "")),
                    "status": "REJECTED",
                    "reason": "live_not_explicitly_enabled",
                    "strategy": strategy,
                }
            if strategy == "carry" and not (self.cfg.enable_carry_live and self.cfg.i_understand_carry_risk):
                return {
                    "order_id": str(order.get("order_id", "")),
                    "status": "REJECTED",
                    "reason": "carry_live_not_explicitly_enabled",
                    "strategy": strategy,
                }

        # paper/live paths both use sandbox adapter unless extended explicitly.
        return self._sandbox.submit_order(order)
