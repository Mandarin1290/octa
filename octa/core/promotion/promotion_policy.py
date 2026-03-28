from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class PromotionPolicy:
    min_trades: int
    min_win_rate: float
    min_profit_factor: float
    max_drawdown: float
    min_total_return: float
    require_hash_integrity: bool = True
    require_validation_ok: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "PromotionPolicy":
        required = (
            "min_trades",
            "min_win_rate",
            "min_profit_factor",
            "max_drawdown",
            "min_total_return",
            "require_hash_integrity",
            "require_validation_ok",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"promotion policy missing required keys: {missing}")
        return cls(
            min_trades=int(payload["min_trades"]),
            min_win_rate=float(payload["min_win_rate"]),
            min_profit_factor=float(payload["min_profit_factor"]),
            max_drawdown=float(payload["max_drawdown"]),
            min_total_return=float(payload["min_total_return"]),
            require_hash_integrity=bool(payload["require_hash_integrity"]),
            require_validation_ok=bool(payload["require_validation_ok"]),
        )


__all__ = ["PromotionPolicy"]
