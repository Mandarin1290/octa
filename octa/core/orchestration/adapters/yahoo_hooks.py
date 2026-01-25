from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from octa.core.data.sources.fundamentals.yahoo import (
    build_yahoo_features,
    fetch_yahoo_corporate_actions,
    fetch_yahoo_earnings_calendar,
    fetch_yahoo_fundamentals,
    load_yahoo_config,
)


def yahoo_features_for_symbol(
    symbol: str, config: Optional[Mapping[str, Any]] = None
) -> tuple[dict[str, float | int | str], dict[str, Any]]:
    cfg = dict(config or load_yahoo_config())
    if not cfg.get("enabled", False):
        return {}, {"enabled": False, "status": "disabled"}

    fundamentals = fetch_yahoo_fundamentals(symbol)
    actions = fetch_yahoo_corporate_actions(symbol)
    earnings = fetch_yahoo_earnings_calendar(symbol)
    features = build_yahoo_features(fundamentals, actions, earnings)

    health = {
        "enabled": True,
        "fundamentals_ok": fundamentals.health.ok,
        "actions_ok": actions.health.ok,
        "earnings_ok": earnings.health.ok,
        "fundamentals_cache_hit": fundamentals.health.cache_hit,
        "actions_cache_hit": actions.health.cache_hit,
        "earnings_cache_hit": earnings.health.cache_hit,
        "errors": fundamentals.health.errors + actions.health.errors + earnings.health.errors,
    }
    return features, health
