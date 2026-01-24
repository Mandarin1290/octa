from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class BondSpec:
    identifier: str
    maturity: date
    duration: Optional[float]  # Macaulay or modified depending on convention supplied
    modified_duration: Optional[float]
    convexity: Optional[float] = None


def duration_proxy_available(bond: BondSpec) -> bool:
    return bond.modified_duration is not None or bond.duration is not None
