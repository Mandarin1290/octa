from typing import Any, Dict


class CurveBuckets:
    def __init__(self, rates: Dict[float, float]):
        """rates: mapping tenor_years -> rate (decimal)

        Buckets: short (<=2y), belly (>2 and <=7), long (>7)
        """
        self.rates = dict(rates)

    def bucketed(self) -> Dict[str, Dict[float, float]]:
        short = {t: r for t, r in self.rates.items() if t <= 2}
        belly = {t: r for t, r in self.rates.items() if 2 < t <= 7}
        long = {t: r for t, r in self.rates.items() if t > 7}
        return {"short": short, "belly": belly, "long": long}

    def parallel_shift(self, bp: float) -> Dict[float, float]:
        """Shift all rates by `bp` basis points (bp as decimal, e.g., 0.01 for 1bp? we expect bp expressed in percent of 1% -> use fraction of 1; here bp is in basis points: 1 bp = 0.0001)."""
        shift = float(bp)
        return {t: r + shift for t, r in self.rates.items()}

    def steepen(self, short_bp: float, long_bp: float) -> Dict[float, float]:
        new = {}
        for t, r in self.rates.items():
            if t <= 2:
                new[t] = r + float(short_bp)
            elif t > 7:
                new[t] = r + float(long_bp)
            else:
                # linear interpolation between short and long shift
                new[t] = r + (float(short_bp) + float(long_bp)) / 2.0
        return new

    def apply_stress(self, stress: Dict[str, Any]) -> Dict[float, float]:
        """Stress can be {'parallel': bp, 'steepen': (short_bp, long_bp)} etc."""
        if "parallel" in stress:
            return self.parallel_shift(stress["parallel"])
        if "steepen" in stress:
            s = stress["steepen"]
            return self.steepen(s[0], s[1])
        return dict(self.rates)
