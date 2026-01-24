"""Drawdown character analysis.

Computes drawdown episodes, durations, recovery speeds, clustering and a simple
classification of drawdown profiles: `LONG_SHALLOW`, `SHARP_CRASH`,
`CLUSTERED`, `QUICK_RECOVERY`, or `NONE`.

Deterministic, no external deps.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class DrawdownEpisode:
    start: int
    trough: int
    end: int
    depth: float
    duration: int
    recovery_time: int


def equity_curve_from_returns(returns: List[float], base: float = 1.0) -> List[float]:
    eq = [base]
    for r in returns:
        eq.append(eq[-1] * (1.0 + float(r)))
    return eq[1:]


def compute_drawdown_series(equity: List[float]) -> List[float]:
    peak = -math.inf
    dd = []
    for v in equity:
        if v > peak:
            peak = v
        if peak <= 0:
            dd.append(0.0)
        else:
            dd.append((peak - v) / peak)
    return dd


def extract_episodes(dd: List[float]) -> List[DrawdownEpisode]:
    episodes: List[DrawdownEpisode] = []
    in_dd = False
    start = 0
    trough = 0
    trough_val = 0.0
    for i, v in enumerate(dd):
        if not in_dd and v > 0:
            in_dd = True
            start = i
            trough = i
            trough_val = v
        elif in_dd:
            if v > trough_val:
                trough_val = v
                trough = i
            if v == 0.0:
                # recovery
                end = i
                depth = trough_val
                duration = trough - start + 1
                recovery_time = end - trough
                episodes.append(
                    DrawdownEpisode(
                        start=start,
                        trough=trough,
                        end=end,
                        depth=depth,
                        duration=duration,
                        recovery_time=recovery_time,
                    )
                )
                in_dd = False
                trough_val = 0.0

    # if currently in drawdown at series end, close with end = last index
    if in_dd:
        end = len(dd) - 1
        depth = trough_val
        duration = trough - start + 1
        recovery_time = -1  # not recovered yet
        episodes.append(
            DrawdownEpisode(
                start=start,
                trough=trough,
                end=end,
                depth=depth,
                duration=duration,
                recovery_time=recovery_time,
            )
        )

    return episodes


def cluster_count(episodes: List[DrawdownEpisode], window: int) -> int:
    # count how many episodes start within final `window` periods
    if not episodes:
        return 0
    last_idx = episodes[-1].end
    cnt = sum(1 for e in episodes if e.start >= max(0, last_idx - window + 1))
    return cnt


def classify_profile(
    episodes: List[DrawdownEpisode], window: int = 60
) -> Dict[str, Any]:
    if not episodes:
        return {"classification": "NONE", "reason": None}

    # metrics aggregated from episodes
    depths = [e.depth for e in episodes]
    durations = [e.duration for e in episodes]
    recoveries = [e.recovery_time for e in episodes if e.recovery_time >= 0]

    avg_depth = sum(depths) / len(depths) if depths else 0.0
    avg_duration = sum(durations) / len(durations) if durations else 0.0
    avg_recovery = sum(recoveries) / len(recoveries) if recoveries else float("inf")

    # clustering: many episodes started recently
    clusters = cluster_count(episodes, window)

    # classification rules (conservative deterministic rules):
    # - LONG_SHALLOW: current (or average) drawdown lasts long and is shallow
    last_ep = episodes[-1]
    if (
        last_ep.recovery_time == -1
        and last_ep.duration >= (window / 2)
        and last_ep.depth < 0.15
    ) or (avg_duration >= (window / 2) and avg_depth < 0.15):
        return {
            "classification": "LONG_SHALLOW",
            "metrics": {"avg_depth": avg_depth, "avg_duration": avg_duration},
        }

    # - SHARP_CRASH: max depth large (>0.25) and durations small (<=5)
    if max(depths) >= 0.25 and any(d <= 5 for d in durations):
        return {"classification": "SHARP_CRASH", "metrics": {"max_depth": max(depths)}}

    # - QUICK_RECOVERY: recent recoveries are fast (avg_recovery <= 3)
    if recoveries and avg_recovery <= 3:
        return {
            "classification": "QUICK_RECOVERY",
            "metrics": {"avg_recovery": avg_recovery},
        }

    # - CLUSTERED: many episodes in window
    if clusters >= 2:
        return {"classification": "CLUSTERED", "metrics": {"cluster_count": clusters}}

    return {
        "classification": "MIXED",
        "metrics": {"avg_depth": avg_depth, "avg_duration": avg_duration},
    }


def analyze_drawdown(returns: List[float], window: int = 60) -> Dict[str, Any]:
    eq = equity_curve_from_returns(returns)
    dd = compute_drawdown_series(eq)
    episodes = extract_episodes(dd)
    profile = classify_profile(episodes, window=window)
    return {"episodes": episodes, "profile": profile, "drawdown_series": dd}
