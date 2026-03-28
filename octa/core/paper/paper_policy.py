from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class PaperPolicy:
    require_promotion_status: str
    require_hash_integrity: bool
    require_recent_promotion: bool
    max_promotion_age_hours: float
    require_shadow_metrics_present: bool
    paper_capital: float
    paper_fee: float
    paper_slippage: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "PaperPolicy":
        required = (
            "require_promotion_status",
            "require_hash_integrity",
            "require_recent_promotion",
            "max_promotion_age_hours",
            "require_shadow_metrics_present",
            "paper_capital",
            "paper_fee",
            "paper_slippage",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"paper policy missing required keys: {missing}")
        return cls(
            require_promotion_status=str(payload["require_promotion_status"]),
            require_hash_integrity=bool(payload["require_hash_integrity"]),
            require_recent_promotion=bool(payload["require_recent_promotion"]),
            max_promotion_age_hours=float(payload["max_promotion_age_hours"]),
            require_shadow_metrics_present=bool(payload["require_shadow_metrics_present"]),
            paper_capital=float(payload["paper_capital"]),
            paper_fee=float(payload["paper_fee"]),
            paper_slippage=float(payload["paper_slippage"]),
        )


__all__ = ["PaperPolicy"]
