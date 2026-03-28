from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class PaperSessionPolicy:
    require_gate_status: str
    max_session_minutes: int
    heartbeat_interval_sec: int
    paper_capital: float
    paper_fee: float
    paper_slippage: float
    max_open_positions: int
    kill_switch_drawdown: float
    allow_short: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "PaperSessionPolicy":
        required = (
            "require_gate_status",
            "max_session_minutes",
            "heartbeat_interval_sec",
            "paper_capital",
            "paper_fee",
            "paper_slippage",
            "max_open_positions",
            "kill_switch_drawdown",
            "allow_short",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"paper session policy missing required keys: {missing}")
        return cls(
            require_gate_status=str(payload["require_gate_status"]),
            max_session_minutes=int(payload["max_session_minutes"]),
            heartbeat_interval_sec=int(payload["heartbeat_interval_sec"]),
            paper_capital=float(payload["paper_capital"]),
            paper_fee=float(payload["paper_fee"]),
            paper_slippage=float(payload["paper_slippage"]),
            max_open_positions=int(payload["max_open_positions"]),
            kill_switch_drawdown=float(payload["kill_switch_drawdown"]),
            allow_short=bool(payload["allow_short"]),
        )


__all__ = ["PaperSessionPolicy"]
