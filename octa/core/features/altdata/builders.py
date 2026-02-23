from __future__ import annotations

from typing import Any, Mapping, Optional

from octa.core.features.altdata import attention_features, event_features, flow_features
from octa.core.features.altdata import fundamental_features, liquidity_features, macro_features, sentiment_features
from octa.core.features.altdata import (
    basis_features,
    cot_features,
    eco_calendar_features,
    funding_rate_features,
    greeks_features,
    iv_surface_features,
    onchain_features,
)
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
    asset_class: str = "stock",
) -> None:
    """Build and persist features for a specific gate/timeframe.

    Payloads is a mapping of source -> normalized payload dict.
    asset_class controls which additional builder modules are dispatched.
    """

    market: dict[str, float] = {}
    symbol_features: dict[str, dict[str, float]] = {}

    # ------------------------------------------------------------------
    # Default (equities / legacy) gate-layer dispatch
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Asset-class-specific dispatch (additive on top of defaults)
    # ------------------------------------------------------------------
    ac = str(asset_class or "stock").lower()

    if ac == "crypto":
        if gate_layer in {"global_1d"}:
            market.update(onchain_features.build(payloads, asof_ts=asof_ts))
        if gate_layer in {"structure_30m"} and symbol:
            symbol_features.setdefault(symbol, {})
            symbol_features[symbol].update(onchain_features.build_symbol(payloads, asof_ts=asof_ts))
        if gate_layer in {"signal_1h"}:
            market.update(funding_rate_features.build(payloads, asof_ts=asof_ts))

    elif ac in {"fx", "forex"}:
        if gate_layer in {"global_1d"}:
            market.update(eco_calendar_features.build(payloads, asof_ts=asof_ts))
        if gate_layer in {"structure_30m"}:
            market.update(cot_features.build(payloads, asof_ts=asof_ts))
        # FX has no per-symbol fundamental features → skip fundamental_features
        if gate_layer in {"structure_30m"} and symbol and symbol in symbol_features:
            # remove fundamental/event features added by default dispatch (not relevant for FX)
            symbol_features[symbol].clear()

    elif ac in {"future", "futures"}:
        if gate_layer in {"structure_30m"} and symbol:
            symbol_features.setdefault(symbol, {})
            symbol_features[symbol].update(basis_features.build(payloads, asof_ts=asof_ts))
        # Futures have no per-symbol fundamental features
        if gate_layer in {"structure_30m"} and symbol and symbol in symbol_features:
            # Remove fundamental features; basis is the relevant structural signal
            symbol_features[symbol] = {
                k: v for k, v in symbol_features[symbol].items()
                if k.startswith("basis.")
            }

    elif ac in {"option", "options"}:
        if gate_layer in {"structure_30m"} and symbol:
            symbol_features.setdefault(symbol, {})
            symbol_features[symbol].update(greeks_features.build(payloads, asof_ts=asof_ts))
        if gate_layer in {"signal_1h"} and symbol:
            symbol_features.setdefault(symbol, {})
            symbol_features[symbol].update(iv_surface_features.build(payloads, asof_ts=asof_ts))

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
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
