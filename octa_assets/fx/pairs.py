from dataclasses import dataclass


@dataclass
class FXPair:
    pair: str  # e.g., "EURUSD"
    base: str  # e.g., EUR
    quote: str  # e.g., USD
    pip_size: float  # smallest price increment, e.g., 0.0001

    def pip_value(self, lot: float, price: float) -> float:
        """Return pip value in quote currency for given lot (base units) and price.

        lot: number of base units (e.g., 100000 for standard lot) or plain base quantity
        """
        # pip in price units multiplied by lot and multiplier of quote per base
        return self.pip_size * lot
