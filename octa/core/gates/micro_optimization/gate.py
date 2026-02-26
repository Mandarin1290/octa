from __future__ import annotations

from dataclasses import dataclass, field
from statistics import median, pstdev
from typing import Any, Mapping, Protocol, Sequence, TypedDict

from octa.core.cascade.context import CascadeContext
from octa.core.cascade.contracts import GateDecision, GateOutcome
from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider
from octa.core.types.timeframe import Timeframe


@dataclass(frozen=True)
class OHLCVSeries:
    open: list[float]
    high: list[float]
    low: list[float]
    close: list[float]
    volume: list[float]
    timestamps: list[Any] = field(default_factory=list)


class MicroDataProvider(Protocol):
    def get_ohlcv(self, symbol: str, timeframe: Timeframe) -> OHLCVSeries | None:
        ...


class MicroPlan(TypedDict):
    order_type_hint: str
    limit_offset_bps: float
    slice_count: int
    max_wait_bars: int
    cancel_if_not_filled_bars: int


@dataclass(frozen=True)
class MicroGateConfig:
    min_points: int = 120
    window: int = 30
    spread_proxy_high: float = 0.002
    spike_score_high: float = 0.01
    gap_threshold: float = 0.01
    min_median_volume: float = 1_000.0
    limit_offset_scale: float = 0.8
    min_offset_bps: float = 1.0
    max_offset_bps: float = 6.0
    max_slice_count: int = 3


class MicroGate:
    name = "micro"
    timeframe: Timeframe = "1M"

    def __init__(
        self,
        data_provider: MicroDataProvider | None = None,
        config: MicroGateConfig | None = None,
        ohlcv_provider: OHLCVProvider | None = None,
    ) -> None:
        self._provider = data_provider
        self._config = config or MicroGateConfig()
        self._ohlcv_provider = ohlcv_provider
        self._execution_map: dict[str, Mapping[str, Any]] = {}
        self._last_artifacts: dict[str, Any] = {}

    def set_context(self, context: CascadeContext) -> None:
        execution_artifacts = context.artifacts.get("execution", {})
        resolved: dict[str, Mapping[str, Any]] = {}
        for symbol, payload in execution_artifacts.items():
            if isinstance(payload, Mapping) and symbol in payload:
                resolved[symbol] = payload[symbol]
            else:
                resolved[symbol] = payload
        self._execution_map = resolved

    def fit(self, symbols: Sequence[str]) -> None:
        return None

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        eligible: list[str] = []
        rejected: list[str] = []
        artifacts: dict[str, Any] = {}

        for symbol in symbols:
            payload = self._evaluate_symbol(symbol)
            artifacts[symbol] = payload
            if payload["decision"] == GateDecision.PASS.value:
                eligible.append(symbol)
            else:
                rejected.append(symbol)

        self._last_artifacts = artifacts
        decision = GateDecision.PASS if eligible else GateDecision.FAIL
        return GateOutcome(
            decision=decision,
            eligible_symbols=eligible,
            rejected_symbols=rejected,
            artifacts=artifacts,
        )

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        return list(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        return dict(self._last_artifacts)

    def _evaluate_symbol(self, symbol: str) -> dict[str, Any]:
        series = self._get_series(symbol)
        if series is None:
            return _fail_payload("missing_data")

        exec_payload = self._execution_map.get(symbol)
        if not exec_payload:
            return _fail_payload("missing_execution_plan")

        plan = exec_payload.get("execution_plan", {})
        if plan.get("action") != "ENTER":
            return _fail_payload("non_enter_plan")

        metrics = _micro_metrics(series, self._config)
        if metrics is None:
            return _fail_payload("missing_data")

        flags = _micro_flags(series, self._config, metrics)
        micro_plan = _build_micro_plan(metrics, flags, self._config)

        decision = GateDecision.PASS if not _critical_flags(flags) else GateDecision.FAIL

        return {
            "decision": decision.value,
            "micro_plan": micro_plan,
            "micro_metrics": metrics,
            "micro_risk_flags": flags,
        }

    def _get_series(self, symbol: str) -> OHLCVSeries | None:
        if self._ohlcv_provider is not None:
            bars = self._ohlcv_provider.get_ohlcv(symbol, self.timeframe)
            return _series_from_bars(bars)
        if self._provider is None:
            return None
        return self._provider.get_ohlcv(symbol, self.timeframe)


def _fail_payload(reason: str) -> dict[str, Any]:
    return {
        "decision": GateDecision.FAIL.value,
        "micro_plan": {
            "order_type_hint": "MARKET",
            "limit_offset_bps": 0.0,
            "slice_count": 1,
            "max_wait_bars": 0,
            "cancel_if_not_filled_bars": 0,
        },
        "micro_metrics": {},
        "micro_risk_flags": {
            "spread_proxy_high": True,
            "spike_risk": True,
            "liquidity_thin": True,
            "gap_risk": True,
            "reason": reason,
        },
    }


def _micro_metrics(series: OHLCVSeries, config: MicroGateConfig) -> dict[str, float] | None:
    if len(series.close) < config.min_points:
        return None

    window = series.close[-config.window :]
    highs = series.high[-config.window :]
    lows = series.low[-config.window :]
    volumes = series.volume[-config.window :]

    spread_proxy = median([(h - lo) for h, lo in zip(highs, lows)]) / median(window)
    returns = [
        (window[idx] / window[idx - 1]) - 1.0 for idx in range(1, len(window))
    ]
    spike_score = max(abs(ret) for ret in returns) if returns else 0.0
    vol1m = pstdev(returns) if len(returns) > 1 else 0.0
    liquidity_score = median(volumes) / max(config.min_median_volume, 1.0)

    return {
        "spread_proxy": spread_proxy,
        "spike_score": spike_score,
        "vol1m": vol1m,
        "liquidity_score": liquidity_score,
    }


def _micro_flags(
    series: OHLCVSeries, config: MicroGateConfig, metrics: Mapping[str, float]
) -> dict[str, bool]:
    spread_proxy_high = metrics["spread_proxy"] >= config.spread_proxy_high
    spike_risk = metrics["spike_score"] >= config.spike_score_high
    liquidity_thin = metrics["liquidity_score"] < 1.0

    gap_risk = False
    for idx in range(1, len(series.close)):
        gap = abs(series.open[idx] / series.close[idx - 1] - 1.0)
        if gap >= config.gap_threshold:
            gap_risk = True
            break

    return {
        "spread_proxy_high": spread_proxy_high,
        "spike_risk": spike_risk,
        "liquidity_thin": liquidity_thin,
        "gap_risk": gap_risk,
    }


def _critical_flags(flags: Mapping[str, bool]) -> bool:
    return (
        flags.get("spread_proxy_high", False)
        or flags.get("spike_risk", False)
        or flags.get("liquidity_thin", False)
        or flags.get("gap_risk", False)
    )


def _build_micro_plan(
    metrics: Mapping[str, float],
    flags: Mapping[str, bool],
    config: MicroGateConfig,
) -> MicroPlan:
    if _critical_flags(flags):
        return {
            "order_type_hint": "MARKET",
            "limit_offset_bps": 0.0,
            "slice_count": 1,
            "max_wait_bars": 1,
            "cancel_if_not_filled_bars": 1,
        }

    spread_bps = metrics["spread_proxy"] * 10000.0 * config.limit_offset_scale
    offset_bps = min(config.max_offset_bps, max(config.min_offset_bps, spread_bps))

    slice_count = 2
    if metrics["liquidity_score"] >= 2.0 and metrics["spike_score"] < config.spike_score_high / 2:
        slice_count = config.max_slice_count

    return {
        "order_type_hint": "LIMIT",
        "limit_offset_bps": offset_bps,
        "slice_count": slice_count,
        "max_wait_bars": 3,
        "cancel_if_not_filled_bars": 3,
    }


def _series_from_bars(bars: Sequence[OHLCVBar]) -> OHLCVSeries | None:
    if not bars:
        return None
    return OHLCVSeries(
        open=[bar.open for bar in bars],
        high=[bar.high for bar in bars],
        low=[bar.low for bar in bars],
        close=[bar.close for bar in bars],
        volume=[bar.volume for bar in bars],
        timestamps=[bar.ts for bar in bars],
    )
