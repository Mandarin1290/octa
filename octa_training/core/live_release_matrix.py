from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional


class LiveMode(str, Enum):
    FULL_MODE = "FULL_MODE"
    REDUCED_INTRADAY_MODE = "REDUCED_INTRADAY_MODE"
    DEFENSIVE_DAILY_MODE = "DEFENSIVE_DAILY_MODE"
    NO_TRADE_MODE = "NO_TRADE_MODE"


@dataclass(frozen=True)
class TimeframeOutcome:
    timeframe: str
    status: str  # PASS | FAIL | ERR
    sharpe: Optional[float] = None
    max_drawdown: Optional[float] = None
    expectancy_net: Optional[float] = None  # avg net return per trade (after costs)
    stability_wf_std: Optional[float] = None
    cost_bps: Optional[float] = None
    spread_bps: Optional[float] = None


@dataclass(frozen=True)
class LiveReleaseDecision:
    mode: LiveMode
    position_size_cap: float
    intraday_allowed: bool
    reentries_allowed: bool
    entries_allowed: bool
    min_holding_days: int
    allow_scale_in: bool
    direction_sources: tuple[str, ...]
    timing_sources: tuple[str, ...]
    exit_sources: tuple[str, ...]
    micro_layer_weight_5m: float
    micro_layer_weight_1m: float
    reason: str


# Deterministic (risk-first) caps.
# - CASE B says 50–70% => choose 50%.
# - CASE C says 10–25% => choose 10%.
REDUCED_INTRADAY_POSITION_CAP = 0.50
DEFENSIVE_DAILY_POSITION_CAP = 0.10
FULL_POSITION_CAP = 1.00
NO_TRADE_POSITION_CAP = 0.00

MICRO_WEIGHT_5M_MAX = 0.10
MICRO_WEIGHT_1M_LIVE = 0.00


def _is_pass(status: str | None) -> bool:
    return str(status or "").upper() == "PASS"


def _is_positive_cost_model(outcome: TimeframeOutcome) -> bool:
    # Fail-closed: if expectancy is unknown, treat as not tradeable.
    if outcome.expectancy_net is None:
        return False
    try:
        return float(outcome.expectancy_net) > 0.0
    except Exception:
        return False


def determine_live_release(
    outcomes_by_tf: Mapping[str, TimeframeOutcome],
) -> LiveReleaseDecision:
    """Deterministic live-release matrix.

    Inputs:
    - outcomes_by_tf: keys must include "1D", "1H", "30m" (5m/1m optional for the decision).

    Rules:
    - Higher TFs dominate lower TFs.
    - 1D FAIL/ERR => NO_TRADE.
    - 1D PASS + 1H PASS + 30m PASS => FULL_MODE.
    - 1D PASS + 1H PASS + 30m not PASS => REDUCED_INTRADAY_MODE.
    - 1D PASS + 1H not PASS => DEFENSIVE_DAILY_MODE.
    - No trade without a positive cost model / net expectancy for required TFs.
    """

    o_1d = outcomes_by_tf.get("1D")
    o_1h = outcomes_by_tf.get("1H")
    o_30m = outcomes_by_tf.get("30m")

    # Strict cascade compatibility: only 1D is strictly required to decide.
    # Missing 1H/30m means "not available" -> treated as not PASS.
    if o_1d is None:
        return LiveReleaseDecision(
            mode=LiveMode.NO_TRADE_MODE,
            position_size_cap=NO_TRADE_POSITION_CAP,
            intraday_allowed=False,
            reentries_allowed=False,
            entries_allowed=False,
            min_holding_days=0,
            allow_scale_in=False,
            direction_sources=(),
            timing_sources=(),
            exit_sources=(),
            micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
            micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
            reason="missing_timeframe:1D",
        )

    if not _is_pass(o_1d.status):
        return LiveReleaseDecision(
            mode=LiveMode.NO_TRADE_MODE,
            position_size_cap=NO_TRADE_POSITION_CAP,
            intraday_allowed=False,
            reentries_allowed=False,
            entries_allowed=False,
            min_holding_days=0,
            allow_scale_in=False,
            direction_sources=(),
            timing_sources=(),
            exit_sources=("1H", "30m", "5m", "1m"),
            micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
            micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
            reason=f"case_d_1d_not_pass:{o_1d.status}",
        )

    # CASE C: only 1D is allowed as bias/regime if 1H is missing or not PASS.
    if o_1h is None or not _is_pass(o_1h.status):
        if not _is_positive_cost_model(o_1d):
            return LiveReleaseDecision(
                mode=LiveMode.NO_TRADE_MODE,
                position_size_cap=NO_TRADE_POSITION_CAP,
                intraday_allowed=False,
                reentries_allowed=False,
                entries_allowed=False,
                min_holding_days=0,
                allow_scale_in=False,
                direction_sources=(),
                timing_sources=(),
                exit_sources=(),
                micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
                micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
                reason="no_trade_cost_model_not_positive:1D",
            )

        return LiveReleaseDecision(
            mode=LiveMode.DEFENSIVE_DAILY_MODE,
            position_size_cap=DEFENSIVE_DAILY_POSITION_CAP,
            intraday_allowed=False,
            reentries_allowed=False,
            entries_allowed=True,
            min_holding_days=1,
            allow_scale_in=False,
            direction_sources=("1D",),
            timing_sources=(),
            exit_sources=("1H", "30m", "5m"),
            micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
            micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
            reason=f"case_c_only_1d_passed:1H={(o_1h.status if o_1h else 'MISSING')}",
        )

    # 1D PASS + 1H PASS => intraday is possible, depending on 30m.
    if not _is_positive_cost_model(o_1d):
        return LiveReleaseDecision(
            mode=LiveMode.NO_TRADE_MODE,
            position_size_cap=NO_TRADE_POSITION_CAP,
            intraday_allowed=False,
            reentries_allowed=False,
            entries_allowed=False,
            min_holding_days=0,
            allow_scale_in=False,
            direction_sources=(),
            timing_sources=(),
            exit_sources=(),
            micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
            micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
            reason="no_trade_cost_model_not_positive:1D",
        )

    if not _is_positive_cost_model(o_1h):
        return LiveReleaseDecision(
            mode=LiveMode.NO_TRADE_MODE,
            position_size_cap=NO_TRADE_POSITION_CAP,
            intraday_allowed=False,
            reentries_allowed=False,
            entries_allowed=False,
            min_holding_days=0,
            allow_scale_in=False,
            direction_sources=(),
            timing_sources=(),
            exit_sources=(),
            micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
            micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
            reason="no_trade_cost_model_not_positive:1H",
        )

    if o_30m is not None and _is_pass(o_30m.status):
        return LiveReleaseDecision(
            mode=LiveMode.FULL_MODE,
            position_size_cap=FULL_POSITION_CAP,
            intraday_allowed=True,
            reentries_allowed=True,
            entries_allowed=True,
            min_holding_days=0,
            allow_scale_in=True,
            direction_sources=("1D", "1H"),
            timing_sources=("30m",),
            exit_sources=("30m", "5m"),
            micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
            micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
            reason="case_a_1d_1h_30m_passed",
        )

    # CASE B: 1D + 1H passed, but 30m missing or not PASS.
    return LiveReleaseDecision(
        mode=LiveMode.REDUCED_INTRADAY_MODE,
        position_size_cap=REDUCED_INTRADAY_POSITION_CAP,
        intraday_allowed=True,
        reentries_allowed=False,
        entries_allowed=True,
        min_holding_days=0,
        allow_scale_in=False,
        direction_sources=("1D", "1H"),
        timing_sources=(),
        exit_sources=("30m", "5m"),
        micro_layer_weight_5m=MICRO_WEIGHT_5M_MAX,
        micro_layer_weight_1m=MICRO_WEIGHT_1M_LIVE,
        reason=f"case_b_1d_1h_passed_30m_not_pass:{(o_30m.status if o_30m else 'MISSING')}",
    )


def outcome_from_pipeline_dict(timeframe: str, layer_dict: Mapping[str, Any] | None) -> TimeframeOutcome:
    """Build TimeframeOutcome from `_pipeline_result_to_dict` output.

    Missing/unknown values are left as None (policy is fail-closed where required).
    """

    status = "ERR"
    if layer_dict is not None:
        passed = bool(layer_dict.get("passed", False))
        err = layer_dict.get("error")
        if err:
            status = "ERR"
        else:
            status = "PASS" if passed else "FAIL"

    metrics: Mapping[str, Any] = {}
    if layer_dict is not None:
        metrics = layer_dict.get("metrics") or {}

    meta: Mapping[str, Any] = {}
    if isinstance(metrics, Mapping):
        meta = metrics.get("metadata") or {}

    return TimeframeOutcome(
        timeframe=str(timeframe),
        status=status,
        sharpe=_to_float(metrics.get("sharpe")),
        max_drawdown=_to_float(metrics.get("max_drawdown")),
        expectancy_net=_to_float(metrics.get("avg_net_trade_return")),
        stability_wf_std=_to_float(metrics.get("sharpe_wf_std")),
        cost_bps=_to_float(meta.get("cost_bps")),
        spread_bps=_to_float(meta.get("spread_bps")),
    )


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None
