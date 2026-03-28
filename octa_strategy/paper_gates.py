from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.robustness_gate2 import evaluate_block_bootstrap
from octa_strategy.state_machine import LifecycleState


class GateFailure(Exception):
    pass


class PaperGates:
    """Quantitative gates for promoting a strategy from PAPER -> LIVE.

    Lifecycle order: IDEA -> SHADOW -> PAPER -> LIVE (shadow before paper).
    PaperGates applies when a strategy has completed paper trading and is
    being evaluated for promotion to LIVE production.

    Previous docstring incorrectly stated PAPER -> SHADOW (old wrong lifecycle order
    was fixed 2026-03-21).

    Metrics input (dict) must include the following keys (all deterministic):
      - runtime_days: float
      - max_drawdown: float (positive, e.g., 0.1 for 10%)
      - sharpe: float
      - sortino: float
      - slippage_diff: float (abs difference vs forecast)
      - incidents: int (number of critical incidents)
      - max_corr: float (max correlation vs existing strategies)

        Optional (recommended) Gate2 extensions (still deterministic):
            - replication_error: float
                    Absolute performance mismatch between an independent backtest engine
                    (e.g. Zipline/Backtrader) and the canonical signal/execution simulator.
                    Gate checks it if threshold `replication_error` is configured.
            - returns: List[float]
                    Return series (e.g. daily) used for deterministic block-bootstrap
                    robustness checks if any `bootstrap_*` thresholds are configured.

    The class is configurable via thresholds passed to constructor.
    """

    DEFAULT_THRESHOLDS = {
        "runtime_days": 7.0,
        "max_drawdown": 0.10,
        "sharpe": 0.5,
        "sortino": 0.7,
        "slippage_diff": 0.02,
        "incidents": 0,
        "max_corr": 0.6,
    }

    def __init__(
        self,
        audit_fn=None,
        sentinel_api=None,
        thresholds: Optional[Dict[str, Any]] = None,
        extra_evaluators: Optional[
            List[Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Dict[str, Any]]]]
        ] = None,
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.thresholds = dict(self.DEFAULT_THRESHOLDS)
        if thresholds:
            self.thresholds.update(thresholds)
        self.extra_evaluators = list(extra_evaluators or [])

    def _maybe_eval_replication(
        self, metrics: Dict[str, Any], t: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        if "replication_error" not in t:
            return {}
        value = float(metrics.get("replication_error", 999.0))
        thr = float(t.get("replication_error"))
        return {
            "replication_error": {
                "value": value,
                "pass": value <= thr,
                "threshold": thr,
            }
        }

    def _maybe_eval_bootstrap(
        self, metrics: Dict[str, Any], t: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        wants = any(
            k in t
            for k in (
                "bootstrap_sharpe_p05_min",
                "bootstrap_maxdd_p95_max",
                "bootstrap_prob_sharpe_below_max",
                "bootstrap_sharpe_floor",
                "bootstrap_n",
                "bootstrap_block",
                "bootstrap_seed",
            )
        )
        if not wants:
            return {}

        returns = metrics.get("returns")
        # Fail-closed: if bootstrap thresholds are configured, returns must be provided.
        if returns is None:
            return {
                "bootstrap": {
                    "value": "missing_returns",
                    "pass": False,
                    "threshold": "returns_required",
                }
            }

        stats = evaluate_block_bootstrap(
            returns=returns,
            sharpe_floor=float(t.get("bootstrap_sharpe_floor", 0.0)),
            n=int(t.get("bootstrap_n", 2000)),
            block=int(t.get("bootstrap_block", 5)),
            seed=int(t.get("bootstrap_seed", 1337)),
        )

        res: Dict[str, Dict[str, Any]] = {
            "bootstrap_sharpe_p05": {
                "value": float(stats["sharpe_p05"]),
                "pass": True,
                "threshold": None,
            },
            "bootstrap_maxdd_p95": {
                "value": float(stats["maxdd_p95"]),
                "pass": True,
                "threshold": None,
            },
            "bootstrap_prob_sharpe_below_floor": {
                "value": float(stats["prob_sharpe_below_floor"]),
                "pass": True,
                "threshold": float(t.get("bootstrap_sharpe_floor", 0.0)),
            },
        }

        if "bootstrap_sharpe_p05_min" in t:
            thr = float(t["bootstrap_sharpe_p05_min"])
            res["bootstrap_sharpe_p05"]["pass"] = float(stats["sharpe_p05"]) >= thr
            res["bootstrap_sharpe_p05"]["threshold"] = thr
        if "bootstrap_maxdd_p95_max" in t:
            thr = float(t["bootstrap_maxdd_p95_max"])
            res["bootstrap_maxdd_p95"]["pass"] = float(stats["maxdd_p95"]) <= thr
            res["bootstrap_maxdd_p95"]["threshold"] = thr
        if "bootstrap_prob_sharpe_below_max" in t:
            thr = float(t["bootstrap_prob_sharpe_below_max"])
            res["bootstrap_prob_sharpe_below_floor"]["pass"] = float(
                stats["prob_sharpe_below_floor"]
            ) <= thr
            res["bootstrap_prob_sharpe_below_floor"]["threshold"] = thr
        return res

    def evaluate(self, metrics: Dict) -> Dict[str, Dict]:
        t = self.thresholds
        results = {}

        results["runtime_days"] = {
            "value": float(metrics.get("runtime_days", 0.0)),
            "pass": float(metrics.get("runtime_days", 0.0)) >= t["runtime_days"],
            "threshold": t["runtime_days"],
        }
        results["max_drawdown"] = {
            "value": float(metrics.get("max_drawdown", 1.0)),
            "pass": float(metrics.get("max_drawdown", 1.0)) <= t["max_drawdown"],
            "threshold": t["max_drawdown"],
        }
        results["sharpe"] = {
            "value": float(metrics.get("sharpe", 0.0)),
            "pass": float(metrics.get("sharpe", 0.0)) >= t["sharpe"],
            "threshold": t["sharpe"],
        }
        results["sortino"] = {
            "value": float(metrics.get("sortino", 0.0)),
            "pass": float(metrics.get("sortino", 0.0)) >= t["sortino"],
            "threshold": t["sortino"],
        }
        results["slippage_diff"] = {
            "value": float(metrics.get("slippage_diff", 999.0)),
            "pass": float(metrics.get("slippage_diff", 999.0)) <= t["slippage_diff"],
            "threshold": t["slippage_diff"],
        }
        results["incidents"] = {
            "value": int(metrics.get("incidents", 1)),
            "pass": int(metrics.get("incidents", 1)) <= t["incidents"],
            "threshold": t["incidents"],
        }
        results["max_corr"] = {
            "value": float(metrics.get("max_corr", 1.0)),
            "pass": float(metrics.get("max_corr", 1.0)) <= t["max_corr"],
            "threshold": t["max_corr"],
        }

        # Optional Gate2 extensions (fail-closed if configured)
        results.update(self._maybe_eval_replication(metrics, t))
        results.update(self._maybe_eval_bootstrap(metrics, t))
        for evaluator in self.extra_evaluators:
            try:
                extra = evaluator(metrics, t)
                if isinstance(extra, dict):
                    results.update(extra)
            except Exception:
                # fail-closed
                results["extra_evaluator"] = {
                    "value": "exception",
                    "pass": False,
                    "threshold": "no_exceptions",
                }

        self.audit_fn("paper_gates.evaluate", {"metrics": metrics, "results": results})
        return results

    def can_promote(self, metrics: Dict) -> bool:
        res = self.evaluate(metrics)
        return all(v["pass"] for v in res.values())

    def promote_if_pass(
        self, lifecycle: StrategyLifecycle, metrics: Dict, doc: str
    ) -> None:
        # only allow promotion if strategy currently in PAPER
        if lifecycle.current_state != LifecycleState.PAPER:
            raise GateFailure(
                f"Strategy not in PAPER state (current={lifecycle.current_state})"
            )

        results = self.evaluate(metrics)
        failed = {k: v for k, v in results.items() if not v["pass"]}
        if failed:
            # signal sentinel and audit
            if self.sentinel_api is not None:
                try:
                    self.sentinel_api.set_gate(
                        2, f"paper_gate_failed:{list(failed.keys())}"
                    )
                except Exception:
                    pass
            self.audit_fn("paper_gates.failed", {"failed": failed})
            raise GateFailure(f"Paper gates failed: {list(failed.keys())}")

        # all gates passed: perform documented transition to LIVE
        lifecycle.transition_to(LifecycleState.LIVE, doc=doc)
        self.audit_fn(
            "paper_gates.promoted", {"strategy_id": lifecycle.strategy_id, "doc": doc}
        )
