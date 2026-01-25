from __future__ import annotations

from typing import Any, Mapping, Optional

from octa.core.features.altdata import attention_features, event_features, flow_features
from octa.core.features.altdata import fundamental_features, liquidity_features, macro_features, sentiment_features
from octa.core.features.altdata.registry import FeatureRegistry


def build_features(
    *,
    run_id: str,
    timeframe: str,
    gate_layer: str,
    payloads: Mapping[str, Any],
    registry: FeatureRegistry,
    symbol: Optional[str] = None,
    asof_ts: Optional[str] = None,
) -> None:
    """Build and persist features for a specific gate/timeframe.

    Payloads is a mapping of source -> normalized payload dict.
    """

    market: dict[str, float] = {}
    symbol_features: dict[str, dict[str, float]] = {}

    if gate_layer in {"global_1d"}:
        market.update(macro_features.build(payloads, asof_ts=asof_ts))
        market.update(event_features.build(payloads, asof_ts=asof_ts))
        market.update(flow_features.build(payloads, asof_ts=asof_ts))

    if gate_layer in {"structure_30m"}:
        market.update(event_features.build(payloads, asof_ts=asof_ts))
        market.update(flow_features.build(payloads, asof_ts=asof_ts))
        if symbol:
            symbol_features[symbol] = {}
            symbol_features[symbol].update(fundamental_features.build(payloads))
            symbol_features[symbol].update(event_features.build(payloads, asof_ts=asof_ts))

    if gate_layer in {"signal_1h"}:
        market.update(flow_features.build(payloads, asof_ts=asof_ts))
        market.update(attention_features.build(payloads))
        if symbol:
            symbol_features[symbol] = symbol_features.get(symbol, {})
            symbol_features[symbol].update(sentiment_features.build(payloads))

    if gate_layer in {"execution_5m", "micro_1m"}:
        if symbol:
            symbol_features[symbol] = symbol_features.get(symbol, {})
            symbol_features[symbol].update(liquidity_features.build(payloads))

    if market:
        registry.write_market_features(
            timeframe=timeframe,
            gate_layer=gate_layer,
            features=market,
            asof_ts=asof_ts,
        )
    if symbol_features:
        registry.write_symbol_features(
            timeframe=timeframe,
            gate_layer=gate_layer,
            features_by_symbol=symbol_features,
            asof_ts=asof_ts,
        )
