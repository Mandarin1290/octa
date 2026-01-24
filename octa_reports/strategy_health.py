from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class StrategyHealthDashboard:
    """Produces a deterministic, explainable dashboard combining aging and health diagnostics.

    Inputs:
    - `registry`: StrategyRegistry
    - `ledger`: AuditChain (optional)
    - optional engines/detectors: `health_scorer`, `alpha_detector`, `regime_engine`, `stability_analyzer`, `auto_derisk`, `sunset_engine`
    - `returns_by_strategy`: optional dict mapping strategy_id -> list of returns for detectors
    - `market_indicator`: optional list for regime tagging
    """

    def __init__(
        self,
        registry,
        ledger=None,
        health_scorer=None,
        alpha_detector=None,
        regime_engine=None,
        stability_analyzer=None,
        auto_derisk=None,
        sunset_engine=None,
    ):
        self.registry = registry
        self.ledger = ledger
        self.health_scorer = health_scorer
        self.alpha_detector = alpha_detector
        self.regime_engine = regime_engine
        self.stability_analyzer = stability_analyzer
        self.auto_derisk = auto_derisk
        self.sunset_engine = sunset_engine

    def _parse_created_at(self, created_at: str) -> Optional[datetime]:
        try:
            # accept ISO format
            return datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            return None

    def _tier_from_created(self, created_at: Optional[str], config=None) -> str:
        # config optional: provide thresholds in days via config dict
        now = datetime.now(timezone.utc)
        created = self._parse_created_at(created_at) if created_at else None
        if created is None:
            return "UNKNOWN"
        days = (now - created).total_seconds() / 86400.0
        c = config or {"young": 90, "mature": 365}
        if days < c["young"]:
            return "YOUNG"
        if days < c["mature"]:
            return "MATURE"
        return "OLD"

    def build(
        self,
        returns_by_strategy: Optional[Dict[str, List[float]]] = None,
        market_indicator: Optional[List[float]] = None,
    ) -> Dict[str, Any]:
        strategies: List[Dict[str, Any]] = []
        out = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "strategies": strategies,
        }
        reg = self.registry.list()
        for sid in sorted(reg.keys()):
            meta = reg[sid]
            created_at = meta.get("created_at")
            tier = self._tier_from_created(created_at)

            # health
            health = None
            if self.health_scorer is not None:
                # prepare inputs from available detectors
                alpha_info = None
                stability_info = None
                regime_info = None
                if (
                    returns_by_strategy
                    and sid in returns_by_strategy
                    and self.alpha_detector is not None
                ):
                    try:
                        ad = self.alpha_detector.detect_decay(returns_by_strategy[sid])
                        alpha_info = {
                            "decay_score": ad.decay_score,
                            "confidence": ad.confidence,
                        }
                    except Exception:
                        alpha_info = None
                if (
                    returns_by_strategy
                    and sid in returns_by_strategy
                    and self.stability_analyzer is not None
                ):
                    try:
                        st = self.stability_analyzer.analyze(returns_by_strategy[sid])
                        stability_info = {"stability_score": st.stability_score}
                    except Exception:
                        stability_info = None
                if self.regime_engine is not None and market_indicator is not None:
                    try:
                        tags = self.regime_engine.tag_regimes(market_indicator)
                        perf = (
                            self.regime_engine.performance_by_regime(
                                returns_by_strategy.get(sid, []), tags
                            )
                            if returns_by_strategy and sid in returns_by_strategy
                            else {}
                        )
                        score, cur, conf = self.regime_engine.compatibility_score(
                            market_indicator[-1] if market_indicator else 0.0,
                            market_indicator,
                            perf,
                        )
                        regime_info = {
                            "compatibility_score": score,
                            "current_regime": cur,
                            "confidence": conf,
                        }
                    except Exception:
                        regime_info = None

                health = self.health_scorer.score(
                    alpha_decay=alpha_info,
                    regime_fit=regime_info,
                    stability=stability_info,
                    drawdown_profile=None,
                    risk_util=None,
                )

            # auto-derisk status
            auto = None
            if self.auto_derisk is not None:
                # best-effort: check last action recorded if accessible
                try:
                    state = getattr(self.auto_derisk, "_last_action", {}).get(sid)
                    attempts = getattr(self.auto_derisk, "_attempts", {}).get(sid, 0)
                    auto = {
                        "last_action": state.isoformat() if state else None,
                        "attempts": attempts,
                    }
                except Exception:
                    auto = None

            # sunset candidate
            sunset = None
            if self.sunset_engine is not None:
                try:
                    is_s = self.sunset_engine.is_sunset(sid)
                    # number of confirmers if any
                    confs = len(
                        getattr(self.sunset_engine, "_confirmers", {}).get(sid, set())
                    )
                    sunset = {"sunset": is_s, "confirmations": confs}
                except Exception:
                    sunset = None

            strategies.append(
                {
                    "strategy_id": sid,
                    "lifecycle_state": meta.get("lifecycle_state"),
                    "age_tier": tier,
                    "health_score": health.score if health is not None else None,
                    "health_explain": health.explain if health is not None else None,
                    "alpha_warning": None,
                    "regime_perf": None,
                    "auto_derisk": auto,
                    "sunset": sunset,
                }
            )

        return out
