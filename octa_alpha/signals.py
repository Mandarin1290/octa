from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import List, Optional

getcontext().prec = 12


@dataclass
class Signal:
    values: List[Decimal]
    normalized: List[Decimal]
    signals: List[Decimal]
    confidences: List[Decimal]


class SignalBuilder:
    """Build bounded signals with documented directionality and confidence weights.

    Hard rules enforced:
    - Signals are clipped to [-1, 1].
    - No implicit leverage: optionally enforce sum(abs(signal)) <= 1.
    - Requires documented transforms (passed via `transforms` arg) — caller responsibility.
    """

    def __init__(self, values: List[float]):
        self.values = [Decimal(str(v)) for v in values]
        self._normalized: List[Decimal] = []
        self._signals: List[Decimal] = []
        self._confidences: List[Decimal] = []

    def normalize_minmax(self) -> List[Decimal]:
        vals = [float(v) for v in self.values]
        vmin = min(vals)
        vmax = max(vals)
        if vmax == vmin:
            norm = [Decimal("0") for _ in vals]
        else:
            norm = [Decimal(str((v - vmin) / (vmax - vmin))) for v in vals]
        self._normalized = norm
        return norm

    def normalize_zscore(self) -> List[Decimal]:
        vals = [float(v) for v in self.values]
        m = sum(vals) / len(vals)
        sd = (sum((x - m) ** 2 for x in vals) / len(vals)) ** 0.5
        if sd == 0:
            norm = [Decimal("0") for _ in vals]
        else:
            # map +/-3 sigma -> +/-1
            norm = [Decimal(str(max(min((x - m) / (3 * sd), 1.0), -1.0))) for x in vals]
        self._normalized = norm
        return norm

    def encode_direction(self, directions: Optional[List[int]] = None) -> List[Decimal]:
        """Apply direction (+1/-1) to normalized magnitudes. If `directions` is None,
        infer by sign of original values.
        """
        if not self._normalized:
            raise ValueError("normalize() must be called before encode_direction()")
        if directions is None:
            dirs = [1 if v >= 0 else -1 for v in [float(x) for x in self.values]]
        else:
            if len(directions) != len(self._normalized):
                raise ValueError("directions length mismatch")
            dirs = [1 if int(d) >= 0 else -1 for d in directions]
        signals = [
            Decimal(d) * Decimal(str(abs(n)))
            for d, n in zip(dirs, self._normalized, strict=False)
        ]
        # clip to bounds
        self._signals = [self._clip(s) for s in signals]
        return self._signals

    def apply_confidence(self, confidences: List[float]) -> List[Decimal]:
        if not self._signals:
            raise ValueError(
                "encode_direction() must be called before apply_confidence()"
            )
        if len(confidences) != len(self._signals):
            raise ValueError("confidences length mismatch")
        conf = [Decimal(str(max(0.0, min(1.0, float(c))))) for c in confidences]
        self._confidences = conf
        self._signals = [
            (s * c).quantize(Decimal("0.00000001"))
            for s, c in zip(self._signals, conf, strict=False)
        ]
        return self._signals

    def enforce_bounds(self, allow_implicit_leverage: bool = False) -> None:
        # all signals must be within [-1,1]
        for s in self._signals:
            if abs(s) > Decimal("1"):
                raise ValueError(f"signal {s} out of bounds [-1,1]")
        if not allow_implicit_leverage:
            total_leverage = sum(abs(s) for s in self._signals)
            if total_leverage > Decimal("1"):
                raise ValueError(
                    f"implicit leverage detected sum(abs(signals))={total_leverage} > 1"
                )

    def get(self) -> Signal:
        return Signal(
            values=self.values,
            normalized=self._normalized,
            signals=self._signals,
            confidences=self._confidences,
        )

    def _clip(self, v: Decimal) -> Decimal:
        if v > Decimal("1"):
            return Decimal("1")
        if v < Decimal("-1"):
            return Decimal("-1")
        return v
