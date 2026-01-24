from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExposureLimits:
    max_single_position_pct: float = 0.1
    max_total_exposure_pct: float = 1.0
    max_gross_exposure_pct: float = 1.5
    correlation_proxy_penalty: float = 0.5


@dataclass(frozen=True)
class ExposureDecision:
    allow: bool
    exposure_after: float
    reason: str


def check_exposure(
    total_equity: float,
    net_exposure: float,
    gross_exposure: float,
    position_value: float,
    limits: ExposureLimits,
) -> ExposureDecision:
    if total_equity <= 0:
        return ExposureDecision(False, net_exposure, "NO_EQUITY")

    single_pct = position_value / total_equity
    if single_pct > limits.max_single_position_pct:
        return ExposureDecision(False, net_exposure, "SINGLE_POSITION_LIMIT")

    exposure_after = net_exposure + position_value
    total_pct = exposure_after / total_equity
    if total_pct > limits.max_total_exposure_pct:
        return ExposureDecision(False, exposure_after, "TOTAL_EXPOSURE_LIMIT")

    gross_after = gross_exposure + abs(position_value)
    gross_pct = gross_after / total_equity
    if gross_pct > limits.max_gross_exposure_pct:
        return ExposureDecision(False, exposure_after, "GROSS_EXPOSURE_LIMIT")

    return ExposureDecision(True, exposure_after, "OK")
