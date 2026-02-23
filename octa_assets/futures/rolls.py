from datetime import date
from typing import Dict, List, Optional


class RollManager:
    """Deterministic roll manager for futures contracts.

    - forward-linked roll: chooses next front-month based on open interest/volume or roll window
    - back-adjust helper for research use
    """

    def __init__(self, roll_window_days: int = 5, oi_multiplier_trigger: float = 1.2):
        self.roll_window_days = int(roll_window_days)
        self.oi_multiplier_trigger = float(oi_multiplier_trigger)

    def days_to_expiry(self, expiry: date, sim_date: Optional[date] = None) -> int:
        """Return calendar days from sim_date (or today) to expiry.

        Pass sim_date explicitly for deterministic backtest behaviour.
        Defaults to date.today() when not provided (non-deterministic outside tests).
        """
        return (expiry - (sim_date or date.today())).days

    def decide_roll(
        self,
        current_symbol: str,
        candidates: Dict[str, Dict],
        contracts_meta: Dict[str, Dict],
        sim_date: Optional[date] = None,
    ) -> Optional[str]:
        """Decide which contract to be the active front-month.

        candidates: mapping symbol -> {"volume": int, "open_interest": int}
        contracts_meta: mapping symbol -> {"expiry": date}
        Returns symbol to roll to (could be same as current_symbol if no roll).
        Deterministic rules:
        - If current contract days_to_expiry <= roll_window_days => roll to candidate with highest open_interest
        - Else if next contract open_interest > current_open_interest * oi_multiplier_trigger => roll
        - Otherwise, keep current
        """
        if current_symbol not in candidates or current_symbol not in contracts_meta:
            return None

        current_meta = contracts_meta[current_symbol]
        cur_expiry = current_meta["expiry"]
        dte = self.days_to_expiry(cur_expiry, sim_date=sim_date)
        current_oi = candidates[current_symbol].get("open_interest", 0)

        # choose next candidate by earliest expiry after current
        future_candidates = [
            s for s, m in contracts_meta.items() if m["expiry"] > cur_expiry
        ]
        if not future_candidates:
            return current_symbol

        # pick the nearest expiry among future candidates
        next_sym = min(future_candidates, key=lambda s: contracts_meta[s]["expiry"])
        next_oi = candidates.get(next_sym, {}).get("open_interest", 0)

        if dte <= self.roll_window_days:
            return next_sym

        if next_oi > current_oi * self.oi_multiplier_trigger:
            return next_sym

        return current_symbol

    def back_adjust(
        self, historical_prices: Dict[str, List[float]], roll_points: List[Dict]
    ) -> Dict[str, List[float]]:
        """Apply simple back-adjust by subtracting price differences at roll points.

        historical_prices: mapping symbol -> list of prices
        roll_points: list of {"from": sym1, "to": sym2, "index": idx}
        """
        adjusted = dict(historical_prices)
        cumulative_shift = 0.0
        for rp in roll_points:
            from_sym = rp["from"]
            to_sym = rp["to"]
            idx = rp["index"]
            price_from = historical_prices[from_sym][idx]
            price_to = historical_prices[to_sym][idx]
            shift = price_to - price_from
            cumulative_shift += shift
            # apply shift to all earlier prices
            for k in adjusted:
                adjusted[k] = [p + cumulative_shift for p in adjusted[k]]

        return adjusted
