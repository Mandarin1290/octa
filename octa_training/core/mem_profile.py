from __future__ import annotations

import os
import time
import tracemalloc
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class MemSnapshot:
    label: str
    ts_utc: float
    rss_mb: Optional[float]
    traced_current_mb: float
    traced_peak_mb: float
    top: list[dict[str, Any]]


def _env_flag(name: str, default: str = "0") -> bool:
    v = str(os.environ.get(name, default)).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def enabled() -> bool:
    return _env_flag("OCTA_MEM_PROFILE", "0")


def _rss_mb() -> Optional[float]:
    try:
        import psutil  # type: ignore

        p = psutil.Process()
        return float(p.memory_info().rss) / (1024.0 * 1024.0)
    except Exception:
        return None


def maybe_start(*, n_frames: int | None = None) -> bool:
    """Start tracemalloc if enabled. Returns True if tracing is active."""
    if not enabled():
        return False

    if tracemalloc.is_tracing():
        return True

    if n_frames is None:
        try:
            n_frames = int(os.environ.get("OCTA_MEM_PROFILE_FRAMES", "25"))
        except Exception:
            n_frames = 25

    try:
        tracemalloc.start(n_frames)
        return True
    except Exception:
        return False


def snapshot(
    *,
    label: str,
    limit: int | None = None,
    logger=None,
) -> Optional[MemSnapshot]:
    """Take a lightweight snapshot and (optionally) emit to logger.

    Note: This is intentionally summary-level (top allocations only) to avoid bloating logs.
    """
    if not enabled() or not tracemalloc.is_tracing():
        return None

    if limit is None:
        try:
            limit = int(os.environ.get("OCTA_MEM_PROFILE_TOP", "20"))
        except Exception:
            limit = 20

    t0 = time.time()
    try:
        snap = tracemalloc.take_snapshot()
        stats = snap.statistics("lineno")
    except Exception:
        return None

    current, peak = tracemalloc.get_traced_memory()
    traced_current_mb = float(current) / (1024.0 * 1024.0)
    traced_peak_mb = float(peak) / (1024.0 * 1024.0)

    top: list[dict[str, Any]] = []
    for s in stats[: max(1, int(limit))]:
        try:
            frame = s.traceback[0]
            top.append(
                {
                    "file": str(frame.filename),
                    "line": int(frame.lineno),
                    "kb": float(s.size) / 1024.0,
                    "count": int(s.count),
                }
            )
        except Exception:
            continue

    out = MemSnapshot(
        label=label,
        ts_utc=t0,
        rss_mb=_rss_mb(),
        traced_current_mb=traced_current_mb,
        traced_peak_mb=traced_peak_mb,
        top=top,
    )

    if logger is not None:
        try:
            logger.info(
                "mem_profile",
                extra={
                    "mem_profile": {
                        "label": out.label,
                        "ts_utc": out.ts_utc,
                        "rss_mb": out.rss_mb,
                        "traced_current_mb": out.traced_current_mb,
                        "traced_peak_mb": out.traced_peak_mb,
                        "top": out.top,
                    }
                },
            )
        except Exception:
            pass

    return out
