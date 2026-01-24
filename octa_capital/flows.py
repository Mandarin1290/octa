import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class CapitalFlowError(Exception):
    pass


class TradingWindowActive(CapitalFlowError):
    pass


class InsufficientLiquidity(CapitalFlowError):
    pass


@dataclass
class FlowRecord:
    timestamp: str
    action: str
    details: Dict
    evidence_hash: str = ""


@dataclass
class PendingRequest:
    investor: str
    amount: float
    request_ts: str
    settle_ts: str
    kind: str  # 'subscription' or 'redemption'
    nav_per_share: Optional[float] = None


class CapitalFlows:
    """Handle subscriptions and redemptions with settlement delays and liquidity checks.

    Hard rules enforced:
    - No capital movement while `trading_window_active` is True.
    - Settlement delays applied to subscriptions and redemptions.
    - Redemptions only settled if sufficient `liquid_assets` exist.
    """

    def __init__(
        self, settlement_delay_days: int = 2, initial_liquid_assets: float = 0.0
    ):
        self.settlement_delay = timedelta(days=settlement_delay_days)
        self.liquid_assets = float(initial_liquid_assets)
        self.investor_balances: Dict[str, float] = {}
        self.pending: List[PendingRequest] = []
        self.audit_log: List[FlowRecord] = []
        self.trading_window_active = False

    def _now_iso(self, now: Optional[datetime] = None) -> str:
        if now is None:
            now = datetime.now(timezone.utc)
        return now.isoformat()

    def _record(self, action: str, details: Dict, now: Optional[datetime] = None):
        ts = self._now_iso(now)
        rec = FlowRecord(timestamp=ts, action=action, details=details)
        rec.evidence_hash = canonical_hash(
            {"ts": ts, "action": action, "details": details}
        )
        self.audit_log.append(rec)

    def set_trading_window(self, active: bool):
        self.trading_window_active = bool(active)
        self._record("set_trading_window", {"active": self.trading_window_active})

    def subscribe(
        self, investor: str, amount: float, now: Optional[datetime] = None
    ) -> PendingRequest:
        if amount <= 0:
            raise ValueError("amount must be positive")
        req_time = now or datetime.now(timezone.utc)
        settle_time = req_time + self.settlement_delay
        pr = PendingRequest(
            investor=investor,
            amount=float(amount),
            request_ts=req_time.isoformat(),
            settle_ts=settle_time.isoformat(),
            kind="subscription",
        )
        self.pending.append(pr)
        self._record(
            "subscribe_requested",
            {
                "investor": investor,
                "amount": amount,
                "request_ts": pr.request_ts,
                "settle_ts": pr.settle_ts,
            },
            now=req_time,
        )
        return pr

    def redeem(
        self,
        investor: str,
        shares: float,
        nav_per_share: float,
        now: Optional[datetime] = None,
    ) -> PendingRequest:
        if shares <= 0:
            raise ValueError("shares must be positive")
        if self.trading_window_active:
            raise TradingWindowActive(
                "No capital movement during active trading window"
            )
        req_time = now or datetime.now(timezone.utc)
        value = shares * float(nav_per_share)
        settle_time = req_time + self.settlement_delay

        # Liquidity check at request time: ensure we can potentially cover at settlement
        if value > self.liquid_assets:
            raise InsufficientLiquidity(
                f"Requested redemption {value} exceeds liquid assets {self.liquid_assets}"
            )

        pr = PendingRequest(
            investor=investor,
            amount=value,
            request_ts=req_time.isoformat(),
            settle_ts=settle_time.isoformat(),
            kind="redemption",
            nav_per_share=nav_per_share,
        )
        self.pending.append(pr)
        self._record(
            "redeem_requested",
            {
                "investor": investor,
                "shares": shares,
                "nav_per_share": nav_per_share,
                "value": value,
                "request_ts": pr.request_ts,
                "settle_ts": pr.settle_ts,
            },
            now=req_time,
        )
        return pr

    def process_settlements(
        self, now: Optional[datetime] = None
    ) -> List[PendingRequest]:
        now_dt = now or datetime.now(timezone.utc)
        settled: List[PendingRequest] = []
        remaining: List[PendingRequest] = []
        for pr in self.pending:
            settle_dt = datetime.fromisoformat(pr.settle_ts)
            if settle_dt <= now_dt:
                # Execute settlement
                if pr.kind == "subscription":
                    self.investor_balances.setdefault(pr.investor, 0.0)
                    self.investor_balances[pr.investor] += pr.amount
                    self.liquid_assets += pr.amount
                    self._record(
                        "settle_subscription",
                        {
                            "investor": pr.investor,
                            "amount": pr.amount,
                            "settle_ts": pr.settle_ts,
                        },
                    )
                    settled.append(pr)
                elif pr.kind == "redemption":
                    # final liquidity check
                    if pr.amount > self.liquid_assets:
                        # cannot settle now; keep pending
                        self._record(
                            "redeem_settlement_failed_liquidity",
                            {
                                "investor": pr.investor,
                                "amount": pr.amount,
                                "liquid_assets": self.liquid_assets,
                                "settle_ts": pr.settle_ts,
                            },
                        )
                        remaining.append(pr)
                    else:
                        # reduce investor balance and liquid assets
                        self.investor_balances.setdefault(pr.investor, 0.0)
                        if pr.amount > self.investor_balances[pr.investor] + 1e-12:
                            # Oversubscription compared to record; fail closed
                            self._record(
                                "redeem_failed_insufficient_balance",
                                {
                                    "investor": pr.investor,
                                    "requested": pr.amount,
                                    "balance": self.investor_balances[pr.investor],
                                },
                            )
                            remaining.append(pr)
                        else:
                            self.investor_balances[pr.investor] -= pr.amount
                            self.liquid_assets -= pr.amount
                            self._record(
                                "settle_redemption",
                                {
                                    "investor": pr.investor,
                                    "amount": pr.amount,
                                    "settle_ts": pr.settle_ts,
                                },
                            )
                            settled.append(pr)
                else:
                    # unknown kind — skip
                    self._record("unknown_pending_kind", {"kind": pr.kind})
            else:
                remaining.append(pr)
        self.pending = remaining
        return settled

    def get_liquidity(self) -> float:
        return float(self.liquid_assets)

    def get_balance(self, investor: str) -> float:
        return float(self.investor_balances.get(investor, 0.0))
