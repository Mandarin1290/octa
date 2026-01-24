import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _utc_now_iso():
    return datetime.utcnow().isoformat() + "Z"


def _canonical_serialize(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical_serialize(obj).encode("utf-8")).hexdigest()


@dataclass
class StrategyContext:
    id: str
    name: str
    positions: Dict[str, float]
    prices: Dict[str, float]
    cash: float
    signals: Dict[str, float]
    recent_trades: List[Dict[str, Any]] = field(default_factory=list)
    active: bool = True
    stuck: bool = False
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def log(self, actor: str, action: str, details: Optional[Dict[str, Any]] = None):
        self.audit_log.append(
            {
                "ts": _utc_now_iso(),
                "actor": actor,
                "action": action,
                "details": details or {},
            }
        )


class SabotageSimulator:
    """Introduce deliberate strategy corruption."""

    @staticmethod
    def runaway_leverage(ctx: StrategyContext, multiplier: float = 2.0) -> None:
        # multiply positions to simulate runaway leverage
        ctx.log("sabotage", "start_runaway_leverage", {"multiplier": multiplier})
        # record baseline notional so monitors can detect abrupt notional inflation
        baseline = 0.0
        for s, q in ctx.positions.items():
            baseline += abs(q) * ctx.prices.get(s, 1.0)
        ctx.metadata.setdefault("baseline_notional", baseline)
        for s in list(ctx.positions.keys()):
            ctx.positions[s] = ctx.positions[s] * multiplier
        ctx.log("sabotage", "runaway_leverage_applied", {"positions": ctx.positions})

    @staticmethod
    def invert_signals(ctx: StrategyContext) -> None:
        ctx.log("sabotage", "start_invert_signals", {})
        for s, v in list(ctx.signals.items()):
            ctx.signals[s] = -v
        ctx.log("sabotage", "signals_inverted", {"signals": ctx.signals})

    @staticmethod
    def stuck_positions(ctx: StrategyContext) -> None:
        ctx.log("sabotage", "start_stuck_positions", {})
        ctx.stuck = True
        ctx.log("sabotage", "positions_stuck", {})


class StrategyMonitor:
    """Detect sabotage and isolate affected strategies without contagion."""

    def __init__(self, leverage_threshold: float = 5.0, inactivity_ticks: int = 3):
        self.leverage_threshold = leverage_threshold
        self.inactivity_ticks = inactivity_ticks

    def _notional(self, ctx: StrategyContext) -> float:
        total = 0.0
        for s, q in ctx.positions.items():
            price = ctx.prices.get(s, 1.0)
            total += abs(q) * price
        return total

    def detect_runaway_leverage(self, ctx: StrategyContext) -> bool:
        notional = self._notional(ctx)
        # if baseline_notional exists, detect abrupt notional inflation
        baseline = ctx.metadata.get("baseline_notional")
        if baseline is not None and baseline > 0:
            ratio = notional / baseline
            ctx.log(
                "monitor",
                "notional_check",
                {"notional": notional, "baseline": baseline, "ratio": ratio},
            )
            return ratio > self.leverage_threshold

        equity = ctx.cash + sum(
            q * ctx.prices.get(s, 1.0) for s, q in ctx.positions.items()
        )
        leverage = notional / max(1e-9, equity)
        ctx.log(
            "monitor",
            "leverage_check",
            {"notional": notional, "equity": equity, "leverage": leverage},
        )
        return leverage > self.leverage_threshold

    def detect_inverted_signals(self, ctx: StrategyContext) -> bool:
        # requires signal history in metadata: list of prior signal dicts
        history = ctx.metadata.get("signal_history")
        if not history or len(history) < 1:
            return False
        # compute simple sign agreement metric across symbols
        current = ctx.signals
        avg_corr = 0.0
        count = 0
        for past in history[-3:]:
            s_corr = 0
            n = 0
            for k in current.keys():
                if k in past and current[k] != 0 and past[k] != 0:
                    s_corr += 1 if current[k] * past[k] > 0 else -1
                    n += 1
            if n:
                avg_corr += s_corr / n
                count += 1
        if count == 0:
            return False
        avg_corr = avg_corr / count
        ctx.log("monitor", "signal_correlation", {"avg_corr": avg_corr})
        # if average sign agreement is strongly negative, signals inverted
        return avg_corr < -0.5

    def detect_stuck(self, ctx: StrategyContext) -> bool:
        # stuck if explicitly flagged
        if ctx.stuck:
            ctx.log("monitor", "stuck_detected_flag", {})
            return True

        # if signals exist but no recent trades, consider inactivity only if strategy was previously trading
        nonzero_signals = any(abs(v) > 1e-9 for v in ctx.signals.values())
        was_trading = ctx.metadata.get("was_trading", False)
        if nonzero_signals and len(ctx.recent_trades) == 0:
            ticks = ctx.metadata.get("inactive_ticks", 0) + 1
            ctx.metadata["inactive_ticks"] = ticks
            ctx.log(
                "monitor", "inactive_tick", {"ticks": ticks, "was_trading": was_trading}
            )
            if was_trading:
                return ticks >= self.inactivity_ticks
            return False

        # reset inactivity counter when trades occur or no signals
        ctx.metadata["inactive_ticks"] = 0
        return False

    def assess_and_isolate(self, contexts: List[StrategyContext]) -> Dict[str, Any]:
        results = {}
        # evaluate each strategy independently to avoid contagion
        for ctx in contexts:
            findings = {
                "runaway": False,
                "inverted": False,
                "stuck": False,
                "isolated": False,
            }
            try:
                if self.detect_runaway_leverage(ctx):
                    findings["runaway"] = True
                if self.detect_inverted_signals(ctx):
                    findings["inverted"] = True
                if self.detect_stuck(ctx):
                    findings["stuck"] = True

                if any((findings["runaway"], findings["inverted"], findings["stuck"])):
                    ctx.active = False
                    findings["isolated"] = True
                    ctx.log(
                        "monitor",
                        "isolate",
                        {
                            "reasons": [
                                k for k, v in findings.items() if v and k != "isolated"
                            ]
                        },
                    )
            except Exception as e:
                ctx.log("monitor", "error", {"error": str(e)})
            results[ctx.id] = findings
        return results


__all__ = ["StrategyContext", "SabotageSimulator", "StrategyMonitor"]
