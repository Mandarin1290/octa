from typing import Union


class CarryEngine:
    """Compute funding / carry accruals for FX positions.

    We use a simple convention:
    - position is expressed as `base_qty` (positive if long base)
    - `price` is quote per base
    - rates passed are annualized decimals (e.g., 0.01 for 1%)
    - accrual returned in quote currency for given `days`

    Formula used (quote currency accrual):
      accrual_quote = base_qty * price * (rate_quote - rate_base) * (days/365)

    Rationale: when long base, you effectively borrow quote to finance base; carry equals interest differential scaled to notional in quote.
    """

    def daily_accrual(
        self,
        base_qty: Union[int, float],
        price: float,
        rate_base: float,
        rate_quote: float,
        days: int = 1,
    ) -> float:
        notional_quote = float(base_qty) * float(price)
        diff = float(rate_quote) - float(rate_base)
        return notional_quote * diff * (float(days) / 365.0)
