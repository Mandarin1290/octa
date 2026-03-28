from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class BrokerPaperPolicy:
    require_paper_gate_status: str
    require_min_completed_sessions: int
    require_min_total_trades: int
    require_min_win_rate: float
    require_min_profit_factor: float
    max_allowed_drawdown: float
    require_kill_switch_not_triggered: bool
    require_hash_integrity: bool
    require_broker_mode: str
    forbid_live_mode: bool
    max_session_age_hours: float
    paper_capital: float
    paper_fee: float
    paper_slippage: float
    max_open_positions: int
    kill_switch_drawdown: float
    allow_short: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "BrokerPaperPolicy":
        required = (
            "require_paper_gate_status",
            "require_min_completed_sessions",
            "require_min_total_trades",
            "require_min_win_rate",
            "require_min_profit_factor",
            "max_allowed_drawdown",
            "require_kill_switch_not_triggered",
            "require_hash_integrity",
            "require_broker_mode",
            "forbid_live_mode",
            "max_session_age_hours",
            "paper_capital",
            "paper_fee",
            "paper_slippage",
            "max_open_positions",
            "kill_switch_drawdown",
            "allow_short",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"broker paper policy missing required keys: {missing}")
        return cls(
            require_paper_gate_status=str(payload["require_paper_gate_status"]),
            require_min_completed_sessions=int(payload["require_min_completed_sessions"]),
            require_min_total_trades=int(payload["require_min_total_trades"]),
            require_min_win_rate=float(payload["require_min_win_rate"]),
            require_min_profit_factor=float(payload["require_min_profit_factor"]),
            max_allowed_drawdown=float(payload["max_allowed_drawdown"]),
            require_kill_switch_not_triggered=bool(payload["require_kill_switch_not_triggered"]),
            require_hash_integrity=bool(payload["require_hash_integrity"]),
            require_broker_mode=str(payload["require_broker_mode"]),
            forbid_live_mode=bool(payload["forbid_live_mode"]),
            max_session_age_hours=float(payload["max_session_age_hours"]),
            paper_capital=float(payload["paper_capital"]),
            paper_fee=float(payload["paper_fee"]),
            paper_slippage=float(payload["paper_slippage"]),
            max_open_positions=int(payload["max_open_positions"]),
            kill_switch_drawdown=float(payload["kill_switch_drawdown"]),
            allow_short=bool(payload["allow_short"]),
        )


__all__ = ["BrokerPaperPolicy"]
