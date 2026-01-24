from datetime import date
from typing import Dict


class SeasonalityModel:
    """Apply monthly seasonality factors to commodity exposures or prices.

    `factors` should map month ints (1-12) to multiplicative factors (e.g., 1.05 for 5% seasonal uplift).
    """

    def __init__(self, factors: Dict[int, float]):
        # ensure all months present; default to 1.0
        self.factors = {m: float(factors.get(m, 1.0)) for m in range(1, 13)}

    def factor_for(self, d: date) -> float:
        return self.factors.get(d.month, 1.0)

    def apply(self, base_value: float, d: date) -> float:
        return float(base_value) * self.factor_for(d)
