from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, getcontext
from typing import Dict, Optional, Tuple

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass
class RotationEngine:
    transaction_cost_rate: Decimal = Decimal("0.0")
    max_shift_fraction: Decimal = Decimal(
        "0.2"
    )  # fraction of total capital allowed to move per period
    cooldown_periods: int = 0  # number of periods an alpha is frozen after a change

    # internal state: last_changed_period per alpha
    last_changed: Dict[str, int] = field(default_factory=dict)

    def rotate_once(
        self,
        current_allocs: Dict[str, Decimal],
        target_weights: Dict[str, Decimal],
        total_capital: Decimal,
        period: int,
        risk_gate_active: Optional[Dict[str, bool]] = None,
    ) -> Tuple[Dict[str, Decimal], Dict[str, Decimal], Decimal]:
        """Perform one rotation step.

        Returns: (new_allocs, moved_amounts, total_cost_paid)
        - `current_allocs`: mapping alpha_id -> capital currently allocated
        - `target_weights`: mapping alpha_id -> desired fraction of total_capital (sums not required)
        - `total_capital`: total capital to distribute
        - `period`: integer time bucket for cooldown logic
        - `risk_gate_active`: optional mapping alpha_id->bool; if True, target changes for that alpha are blocked
        """
        risk_gate_active = risk_gate_active or {}
        total_capital = _quant(total_capital)

        # normalize target weights into requested allocations
        total_weight = sum(float(w) for w in target_weights.values())
        if total_weight <= 0:
            desired_allocs = {k: Decimal("0") for k in target_weights}
        else:
            desired_allocs = {
                k: _quant(Decimal(float(w)) / Decimal(total_weight) * total_capital)
                for k, w in target_weights.items()
            }

        # ensure all keys present
        keys = set(current_allocs.keys()) | set(desired_allocs.keys())
        for k in keys:
            current_allocs.setdefault(k, Decimal("0"))
            desired_allocs.setdefault(k, Decimal("0"))

        # limit total movable capital per period
        max_movable = _quant(total_capital * self.max_shift_fraction)

        new_allocs = current_allocs.copy()
        moved = {k: Decimal("0") for k in keys}
        total_cost = Decimal("0")

        # compute desired deltas
        deltas = {
            k: desired_allocs[k] - current_allocs.get(k, Decimal("0")) for k in keys
        }

        # apply risk gates and cooldowns: if gated or cooling, set desired equal to current
        for k in keys:
            if risk_gate_active.get(k, False):
                deltas[k] = Decimal("0")
            last = self.last_changed.get(k, -9999)
            if period - last < self.cooldown_periods:
                deltas[k] = Decimal("0")

        # We will move capital from net-negative deltas to net-positive deltas, respecting max_movable and costs
        sources = {k: -d for k, d in deltas.items() if d < 0}
        sinks = {k: d for k, d in deltas.items() if d > 0}

        total_to_move = sum(sources.values(), Decimal("0"))

        # but limited by max_movable
        move_budget = min(_quant(total_to_move), max_movable)

        if move_budget <= Decimal("0"):
            return new_allocs, moved, total_cost

        # Proportional movement from sources and to sinks
        # Compute source proportions
        source_total = sum(float(v) for v in sources.values()) if sources else 0.0
        sink_total = sum(float(v) for v in sinks.values()) if sinks else 0.0

        for s_k, s_val in sources.items():
            prop = float(s_val) / source_total if source_total > 0 else 0.0
            provide = _quant(Decimal(str(prop)) * move_budget)
            # apply cost: cost deducted from moved amount
            cost = _quant(provide * self.transaction_cost_rate)
            _quant(provide - cost)
            new_allocs[s_k] = _quant(new_allocs.get(s_k, Decimal("0")) - provide)
            moved[s_k] = _quant(moved.get(s_k, Decimal("0")) - provide)
            total_cost += cost
            self.last_changed[s_k] = period

        # distribute to sinks proportionally from collected net amount (move_budget - total_cost)
        available_for_sinks = _quant(move_budget - total_cost)
        for t_k, t_val in sinks.items():
            prop = float(t_val) / sink_total if sink_total > 0 else 0.0
            receive = _quant(Decimal(str(prop)) * available_for_sinks)
            new_allocs[t_k] = _quant(new_allocs.get(t_k, Decimal("0")) + receive)
            moved[t_k] = _quant(moved.get(t_k, Decimal("0")) + receive)
            self.last_changed[t_k] = period

        # final quantize
        for k in new_allocs:
            new_allocs[k] = _quant(new_allocs[k])

        total_cost = _quant(total_cost)
        return new_allocs, moved, total_cost


__all__ = ["RotationEngine"]
