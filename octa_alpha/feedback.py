from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, getcontext
from typing import Dict, List, Tuple

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass
class FeedbackEngine:
    window_size: int = 20
    lag_periods: int = 2
    min_periods: int = 5
    learning_rate: Decimal = Decimal("1.0")
    significance_threshold: Decimal = Decimal("0.001")
    max_adjust_pct: Decimal = Decimal("0.2")
    # internal store: mapping alpha_id -> list of (period, return)
    returns: Dict[str, List[Tuple[int, Decimal]]] = field(default_factory=dict)

    def add_return(self, alpha_id: str, period: int, ret: float) -> None:
        r = Decimal(str(ret))
        lst = self.returns.setdefault(alpha_id, [])
        lst.append((period, _quant(r)))
        # keep bounded history
        if len(lst) > (self.window_size + self.lag_periods + 5):
            # remove oldest
            lst.sort()
            while len(lst) > (self.window_size + self.lag_periods + 5):
                lst.pop(0)

    def _eligible_window(self, alpha_id: str, current_period: int) -> List[Decimal]:
        lst = self.returns.get(alpha_id, [])
        cutoff = current_period - self.lag_periods
        lower = cutoff - self.window_size + 1
        vals = [r for (p, r) in lst if p <= cutoff and p >= lower]
        return vals

    def rolling_mean(self, alpha_id: str, current_period: int) -> Tuple[Decimal, int]:
        vals = self._eligible_window(alpha_id, current_period)
        n = len(vals)
        if n == 0:
            return Decimal("0"), 0
        s = sum(float(v) for v in vals)
        mean = Decimal(str(s / n))
        return _quant(mean), n

    def adjust_scores(
        self, base_scores: Dict[str, Decimal], current_period: int
    ) -> Tuple[Dict[str, Decimal], Dict[str, Decimal]]:
        """Return (adjusted_scores, adjustments) where adjustments are multipliers applied.

        Rules:
        - Use rolling mean excluding `lag_periods` to avoid reacting to newest data.
        - Require at least `min_periods` samples; otherwise no change.
        - Only adjust if abs(mean) >= `significance_threshold`.
        - Multiplier = 1 + clamp(learning_rate * mean, -max_adjust_pct, max_adjust_pct)
        - Adjusted score = base_score * multiplier.
        """
        adjusted: Dict[str, Decimal] = {}
        multipliers: Dict[str, Decimal] = {}
        for aid, base in base_scores.items():
            mean, n = self.rolling_mean(aid, current_period)
            if n < self.min_periods or abs(mean) < self.significance_threshold:
                multipliers[aid] = Decimal("1")
                adjusted[aid] = _quant(base)
                continue

            # compute raw multiplier
            raw = Decimal("1") + (self.learning_rate * mean)
            # cap change relative to 1
            lower = Decimal("1") - self.max_adjust_pct
            upper = Decimal("1") + self.max_adjust_pct
            if raw < lower:
                raw = lower
            if raw > upper:
                raw = upper

            multipliers[aid] = _quant(raw)
            adjusted[aid] = _quant(_quant(base) * multipliers[aid])

        return adjusted, multipliers


__all__ = ["FeedbackEngine"]
