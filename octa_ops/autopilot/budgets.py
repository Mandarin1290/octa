from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional

try:  # optional
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None  # type: ignore


@dataclass
class BudgetExceeded(RuntimeError):
    reason: str


class ResourceBudgetController:
    """HF-style resource budget controller.

    - Caps threads via env vars (OMP/MKL/OpenBLAS/NUMEXPR).
    - Can checkpoint max RSS and runtime.
    - Fail-closed: if we cannot observe memory (psutil missing), we do not crash,
      but we also cannot enforce RAM budgets.
    """

    def __init__(self, max_runtime_s: int, max_ram_mb: int, max_threads: int, max_disk_mb: int = 0, disk_root: str | None = None) -> None:
        self.max_runtime_s = int(max_runtime_s)
        self.max_ram_mb = int(max_ram_mb)
        self.max_threads = int(max_threads)
        self.max_disk_mb = int(max_disk_mb or 0)
        self.disk_root = disk_root
        self._t0 = time.time()
        self._rss_peak_mb = 0.0

    def apply_thread_caps(self) -> None:
        n = str(max(1, int(self.max_threads)))
        for k in (
            "OMP_NUM_THREADS",
            "MKL_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
        ):
            os.environ.setdefault(k, n)

    def _rss_mb(self) -> Optional[float]:
        if psutil is None:
            return None
        try:
            p = psutil.Process()
            return float(p.memory_info().rss) / (1024.0 * 1024.0)
        except Exception:
            return None

    def checkpoint(self, label: str) -> None:
        dt = time.time() - self._t0
        if dt > self.max_runtime_s:
            raise BudgetExceeded(reason=f"RESOURCE_BUDGET:runtime>{self.max_runtime_s}s at {label}")
        rss = self._rss_mb()
        if rss is not None:
            self._rss_peak_mb = max(self._rss_peak_mb, rss)
            if rss > self.max_ram_mb:
                raise BudgetExceeded(reason=f"RESOURCE_BUDGET:ram>{self.max_ram_mb}MB at {label}")

        # disk budget (best-effort). If configured and we can measure usage, enforce.
        if self.max_disk_mb and self.disk_root:
            try:
                import os

                total = 0
                for root, _dirs, files in os.walk(self.disk_root):
                    for fn in files:
                        try:
                            total += os.path.getsize(os.path.join(root, fn))
                        except Exception:
                            continue
                mb = float(total) / (1024.0 * 1024.0)
                if mb > float(self.max_disk_mb):
                    raise BudgetExceeded(reason=f"RESOURCE_BUDGET:disk>{self.max_disk_mb}MB at {label}")
            except BudgetExceeded:
                raise
            except Exception:
                # If we cannot observe disk usage reliably, do not crash; we still remain fail-closed
                # at artifact promotion level (missing/invalid artifacts => no trading).
                pass

    @property
    def rss_peak_mb(self) -> float:
        return float(self._rss_peak_mb)
