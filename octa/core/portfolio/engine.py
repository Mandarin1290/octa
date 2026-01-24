from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from octa.core.capital.engine import CapitalDecision

from .correlation import correlation_adjusted_exposure, correlation_matrix, cluster_exposure_score
from .metrics import exposure_concentration
from .state import PortfolioState


@dataclass(frozen=True)
class PortfolioEngineConfig:
    max_net_exposure: float = 1.0
    max_gross_exposure: float = 1.5
    max_concentration: float = 0.35
    drawdown_limit: float = 0.15
    correlation_threshold: float = 0.7


@dataclass(frozen=True)
class PortfolioDecision:
    allow_trades: bool
    approved_trades: list[CapitalDecision]
    blocked_trades: list[CapitalDecision]
    exposure_after: float
    correlation_penalty: float
    portfolio_risk_flags: dict[str, Any]
    reason: str


class PortfolioEngine:
    def __init__(self, config: PortfolioEngineConfig | None = None) -> None:
        self._config = config or PortfolioEngineConfig()

    def aggregate(
        self,
        capital_decisions: Sequence[CapitalDecision],
        portfolio_state: PortfolioState,
        market_state: Mapping[str, Any],
    ) -> PortfolioDecision:
        total_equity = float(market_state.get("total_equity", portfolio_state.net_exposure or 0.0))
        if total_equity <= 0:
            total_equity = 1.0
        if portfolio_state.portfolio_drawdown > self._config.drawdown_limit:
            return PortfolioDecision(
                allow_trades=False,
                approved_trades=[],
                blocked_trades=list(capital_decisions),
                exposure_after=portfolio_state.net_exposure,
                correlation_penalty=1.0,
                portfolio_risk_flags={"drawdown": portfolio_state.portfolio_drawdown},
                reason="DRAWDOWN_LIMIT",
            )

        approved: list[CapitalDecision] = []
        blocked: list[CapitalDecision] = []

        exposure_after = portfolio_state.net_exposure
        gross_after = portfolio_state.gross_exposure

        returns = market_state.get("returns", {})
        matrix = correlation_matrix(returns) if isinstance(returns, Mapping) else {}
        cluster_score = cluster_exposure_score(matrix, self._config.correlation_threshold)
        correlation_penalty = correlation_adjusted_exposure(cluster_score)

        for decision in capital_decisions:
            if not decision.allow_trade:
                blocked.append(decision)
                continue

            next_exposure = exposure_after + decision.capital_used
            next_gross = gross_after + abs(decision.capital_used)

            if next_exposure > self._config.max_net_exposure * total_equity:
                blocked.append(decision)
                continue
            if next_gross > self._config.max_gross_exposure * total_equity:
                blocked.append(decision)
                continue
            if correlation_penalty < 0.5:
                blocked.append(decision)
                continue

            exposure_after = next_exposure
            gross_after = next_gross
            approved.append(decision)

        concentration = exposure_concentration(
            {
                (decision.symbol or str(idx)): decision.capital_used
                for idx, decision in enumerate(approved)
            }
        )
        if len(approved) > 1 and concentration > self._config.max_concentration:
            blocked.extend(approved)
            approved = []
            reason = "CONCENTRATION_LIMIT"
        else:
            reason = "OK"

        portfolio_flags = {
            "correlation_penalty": correlation_penalty,
            "cluster_score": cluster_score,
            "concentration": concentration,
        }

        return PortfolioDecision(
            allow_trades=bool(approved),
            approved_trades=approved,
            blocked_trades=blocked,
            exposure_after=exposure_after,
            correlation_penalty=correlation_penalty,
            portfolio_risk_flags=portfolio_flags,
            reason=reason,
        )
