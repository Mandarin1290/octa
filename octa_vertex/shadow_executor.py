import hashlib
from typing import Any, Callable, Dict

from octa_sentinel.kill_switch import get_kill_switch


class ShadowExecutor:
    """Execute orders in shadow mode: full pipeline exercised, but no live broker calls.

    Expected minimal interfaces (duck-typed):
    - `broker_adapter` with `submit_order(order)` (will NOT be called in shadow mode)
    - `allocator_api` with optional `pre_trade_check(order)` returning (allowed:bool, reason:str)
    - `sentinel_api` with `set_gate(level, reason)` and `get_gate_level()` (optional)
    - `price_provider` Callable `get_price(instrument)` -> float
    - `audit_fn(event_type, payload)` to record audit events
    """

    def __init__(
        self,
        broker_adapter,
        allocator_api,
        sentinel_api,
        price_provider: Any,
        audit_fn: Callable[[str, dict], None],
        config: Dict[str, Any],
    ):
        self.broker = broker_adapter
        self.allocator = allocator_api
        self.sentinel = sentinel_api
        self.price_provider = price_provider
        self.audit = audit_fn
        self.config = dict(config or {})

        # wire kill switch singleton
        self._kill = get_kill_switch(audit_fn=audit_fn)

        self.shadow_positions: Dict[str, float] = {}
        self.shadow_pnl: Dict[str, float] = {}
        self.paper_pnl: Dict[str, float] = {}

    def _deterministic_slippage(self, order: dict, base_price: float) -> float:
        # deterministic pseudo-random slippage based on order_id
        oid = str(order.get("order_id", ""))
        h = hashlib.sha256(oid.encode()).digest()
        # take small int from hash
        small = h[0] % 11  # 0..10 bps
        bps = small / 10000.0
        side = order.get("side", "BUY").upper()
        direction = -1 if side == "BUY" else 1
        return direction * base_price * bps

    def submit_order(self, order: dict) -> dict:
        """Run full pre-trade pipeline but intercept final execution.

        Returns a simulated order result dict with keys: status, filled_qty, fill_price, reason(optional)
        """
        # Hard kill-switch enforcement: block if TRIGGERED/LOCKED
        try:
            ks = self._kill.get_state()
            if ks.name in ("TRIGGERED", "LOCKED"):
                self.audit("kill_block", {"order": order, "state": ks.name})
                return {"status": "REJECTED", "reason": "kill-switch"}
        except Exception:
            # fail-safe: if kill-switch check errors, block
            self.audit("kill_check_error", {"order": order})
            return {"status": "REJECTED", "reason": "kill-check-error"}

        # config enforcement
        if not self.config.get("shadow_mode", True):
            # fallback to live submission
            return self.broker.submit_order(order)

        # allocator pre-trade
        if self.allocator and hasattr(self.allocator, "pre_trade_check"):
            allowed, reason = self.allocator.pre_trade_check(order)
            if not allowed:
                self.audit("pre_trade_block", {"order": order, "reason": reason})
                return {"status": "REJECTED", "reason": reason}

        # sentinel gating: if kill switch active, block
        gate_level = 0
        if self.sentinel and hasattr(self.sentinel, "get_gate_level"):
            try:
                gate_level = int(self.sentinel.get_gate_level() or 0)
            except Exception:
                gate_level = 0

        kill_threshold = int(self.config.get("kill_threshold", 3))
        if gate_level >= kill_threshold:
            self.audit("shadow_block_kill", {"order": order, "gate_level": gate_level})
            return {"status": "REJECTED", "reason": "kill-switch"}

        # Use price provider to simulate fills
        instrument = order.get("instrument")
        instrument_str = str(instrument or "_unknown")
        base_price = None
        if callable(getattr(self.price_provider, "get_price", None)):
            base_price = self.price_provider.get_price(instrument)
        elif callable(self.price_provider):
            base_price = self.price_provider(instrument)

        if base_price is None:
            self.audit("shadow_no_price", {"order": order})
            return {"status": "REJECTED", "reason": "no-price"}

        slippage = self._deterministic_slippage(order, base_price)
        fill_price = base_price + slippage
        qty = float(order.get("qty", 0))
        side = order.get("side", "BUY").upper()
        signed_qty = qty if side == "BUY" else -qty

        # update shadow positions (do not touch real broker)
        prev = self.shadow_positions.get(instrument_str, 0.0)
        new = prev + signed_qty
        self.shadow_positions[instrument_str] = new

        # compute simple PnL delta: -signed_qty * (fill_price - base_price)
        pnl = -signed_qty * (fill_price - base_price)
        oid = order.get("order_id")
        oid_str = str(oid or "")
        self.shadow_pnl[oid_str] = pnl

        event = {
            "order_id": order.get("order_id"),
            "instrument": instrument,
            "qty": qty,
            "side": side,
            "fill_price": fill_price,
            "status": "FILLED",
        }
        self.audit("shadow_fill", event)

        # Shadow drawdown evaluation: if drawdown breaches, notify sentinel
        dd_threshold = float(self.config.get("shadow_drawdown_threshold", 0.2))
        total_pnl = sum(self.shadow_pnl.values())
        # assume notional exposure approximate
        notional = (
            sum(
                abs(q) * base_price
                for q, base_price in [
                    (v, base_price) for v in self.shadow_positions.values()
                ]
            )
            or 1.0
        )
        drawdown = abs(total_pnl) / notional
        if (
            drawdown >= dd_threshold
            and self.sentinel
            and hasattr(self.sentinel, "set_gate")
        ):
            self.sentinel.set_gate(2, f"shadow_drawdown:{drawdown:.3f}")

        # return simulated fill
        return {"status": "FILLED", "filled_qty": qty, "fill_price": fill_price}
