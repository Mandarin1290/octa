from __future__ import annotations

import os
import platform
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class EnvReport:
    ok: bool
    warnings: List[str]
    errors: List[str]
    facts: Dict[str, str]


def _gb(bytes_: int) -> float:
    return float(bytes_) / (1024.0**3)


def check_python(min_major: int = 3, min_minor: int = 10) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []

    v = sys.version_info
    if (v.major, v.minor) < (min_major, min_minor):
        errors.append(f"python_too_old: {v.major}.{v.minor} < {min_major}.{min_minor}")

    # Soft warning: venv recommended.
    in_venv = (getattr(sys, "base_prefix", sys.prefix) != sys.prefix) or bool(os.getenv("VIRTUAL_ENV"))
    if not in_venv:
        warnings.append("venv_not_detected: recommended for reproducible installs")

    return warnings, errors


def check_disk(
    *,
    path: str = ".",
    min_free_gb: int = 10,
) -> Tuple[List[str], List[str], Dict[str, str]]:
    warnings: List[str] = []
    errors: List[str] = []
    facts: Dict[str, str] = {}

    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(str(p))
    free_gb = _gb(int(usage.free))
    total_gb = _gb(int(usage.total))

    facts.update({
        "disk_path": str(p.resolve()),
        "disk_free_gb": f"{free_gb:.2f}",
        "disk_total_gb": f"{total_gb:.2f}",
    })

    if free_gb < float(min_free_gb):
        errors.append(f"disk_free_too_low: {free_gb:.2f}GB < {min_free_gb}GB at {p}")

    return warnings, errors, facts


def dir_size_gb(path: str) -> float:
    p = Path(path)
    if not p.exists():
        return 0.0
    total = 0
    for f in p.rglob("*"):
        try:
            if f.is_file():
                total += int(f.stat().st_size)
        except Exception:
            continue
    return _gb(total)


def check_data_dirs(*, warn_over_gb: int = 200, dirs: List[str] | None = None) -> List[str]:
    warnings: List[str] = []
    dirs = dirs or ["raw", "artifacts", "logs", "reports", "mlruns"]
    for d in dirs:
        try:
            sz = dir_size_gb(d)
            if sz > float(warn_over_gb):
                warnings.append(f"data_dir_large: {d} size_gb={sz:.1f} > {warn_over_gb}")
        except Exception:
            continue
    if warnings:
        warnings.append("recommendation: consider archiving/compressing old runs (no deletion)")
    return warnings


def run_env_checks(*, min_free_gb: int, max_data_dir_gb_warn: int) -> EnvReport:
    warnings: List[str] = []
    errors: List[str] = []

    w, e = check_python()
    warnings.extend(w)
    errors.extend(e)

    w2, e2, facts = check_disk(path=".", min_free_gb=min_free_gb)
    warnings.extend(w2)
    errors.extend(e2)

    warnings.extend(check_data_dirs(warn_over_gb=max_data_dir_gb_warn))

    facts.update({
        "os": platform.platform(),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
    })

    return EnvReport(ok=(len(errors) == 0), warnings=warnings, errors=errors, facts=facts)


__all__ = ["EnvReport", "run_env_checks", "dir_size_gb"]
