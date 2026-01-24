from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional


@dataclass
class AutoDeriskConfig:
    min_factor: float = 0.5  # strongest derisk factor (0.0 would close positions)
    cooldown_seconds: int = 3600  # minimum seconds between derisk actions per strategy
    max_attempts: int = 3  # attempts before escalation
    effectiveness_threshold: float = (
        0.01  # required fractional reduction in exposure to count as effective
    )


class AutoDerisk:
    """Automatic soft de-risking engine.

    - Derisk factor computed from health score: factor in [min_factor, 1.0].
    - Strategy-specific scaling via register_strategy.
    - Cooldown enforced between derisk actions.
    - Escalates to suspension when derisk is ineffective after `max_attempts`.
    """

    def __init__(
        self,
        audit_fn: Callable[[str, Dict], None] | None = None,
        sentinel_api=None,
        allocator_api=None,
        config: AutoDeriskConfig | None = None,
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.allocator_api = allocator_api
        self.config = config or AutoDeriskConfig()

        # per-strategy state
        self._last_action: Dict[str, datetime] = {}
        self._attempts: Dict[str, int] = {}
        self._last_exposure: Dict[str, float] = {}
        self._strategy_scale: Dict[str, float] = {}

    def register_strategy(self, strategy_id: str, scale: float = 1.0) -> None:
        self._strategy_scale[strategy_id] = float(scale)
        self._attempts.setdefault(strategy_id, 0)
        self._last_exposure.setdefault(strategy_id, 0.0)
        self._last_action.setdefault(
            strategy_id, datetime.fromtimestamp(0, tz=timezone.utc)
        )
        self.audit_fn(
            "autoderisk.register", {"strategy_id": strategy_id, "scale": scale}
        )

    def _compute_factor(self, health: float, strategy_id: str) -> float:
        # health in [0,1], higher = healthier. factor in [min_factor, 1.0]
        min_f = max(0.0, min(1.0, self.config.min_factor))
        h = max(0.0, min(1.0, float(health)))
        factor = min_f + h * (1.0 - min_f)
        # apply strategy-specific scale (if scale <1 compresses effect)
        scale = self._strategy_scale.get(strategy_id, 1.0)
        # scale closer to 1 reduces derisk strength
        factor = 1.0 - (1.0 - factor) * float(scale)
        return max(min_f, min(1.0, factor))

    def process(
        self, strategy_id: str, health_score: float, current_exposure: float
    ) -> Optional[Dict]:
        now = datetime.now(timezone.utc)
        last = self._last_action.get(
            strategy_id, datetime.fromtimestamp(0, tz=timezone.utc)
        )
        cooldown = timedelta(seconds=self.config.cooldown_seconds)
        if (now - last) < cooldown:
            # cooldown active
            self.audit_fn(
                "autoderisk.cooldown",
                {
                    "strategy_id": strategy_id,
                    "since_last_sec": (now - last).total_seconds(),
                },
            )
            return None

        factor = self._compute_factor(health_score, strategy_id)

        # perform derisk via allocator_api
        try:
            if self.allocator_api is not None:
                self.allocator_api.derisk(strategy_id, factor)
        except Exception:
            pass

        self.audit_fn(
            "autoderisk.action",
            {
                "strategy_id": strategy_id,
                "factor": factor,
                "health": health_score,
                "exposure_before": current_exposure,
            },
        )
        # record action time
        self._last_action[strategy_id] = now

        # evaluate effectiveness
        prev = self._last_exposure.get(strategy_id, current_exposure)
        # store current as baseline for next check
        self._last_exposure[strategy_id] = current_exposure

        effective = False
        if prev > 0:
            reduction = (prev - current_exposure) / prev
            effective = reduction >= self.config.effectiveness_threshold

        if effective:
            # reset attempts
            self._attempts[strategy_id] = 0
            self.audit_fn(
                "autoderisk.effective",
                {"strategy_id": strategy_id, "reduction": reduction},
            )
            return {
                "strategy_id": strategy_id,
                "action": "derisk",
                "factor": factor,
                "effective": True,
            }

        # ineffective
        self._attempts[strategy_id] = self._attempts.get(strategy_id, 0) + 1
        self.audit_fn(
            "autoderisk.ineffective",
            {"strategy_id": strategy_id, "attempts": self._attempts[strategy_id]},
        )

        if self._attempts[strategy_id] >= self.config.max_attempts:
            # escalate to suspension
            self.audit_fn(
                "autoderisk.escalate",
                {"strategy_id": strategy_id, "attempts": self._attempts[strategy_id]},
            )
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(
                        3, f"autoderisk_escalation:{strategy_id}"
                    )
            except Exception:
                pass
            try:
                if self.allocator_api is not None:
                    self.allocator_api.suspend(strategy_id)
            except Exception:
                pass
            # reset attempts after escalate
            self._attempts[strategy_id] = 0
            return {"strategy_id": strategy_id, "action": "suspend", "escalated": True}

        return {
            "strategy_id": strategy_id,
            "action": "derisk",
            "factor": factor,
            "effective": False,
            "attempts": self._attempts[strategy_id],
        }
