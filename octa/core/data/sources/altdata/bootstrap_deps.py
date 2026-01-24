from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

DEFAULT_IMPORTS: Tuple[str, ...] = (
    "duckdb",
    "pyarrow",
    "pandas",
    "requests",
    "tenacity",
    "dateutil",
)


@dataclass
class DepsStatus:
    ok: bool
    missing: List[str]
    attempted_install: bool
    installed: List[str]
    errors: List[str]


def _auto_install_enabled() -> bool:
    return str(os.getenv("OKTA_ALTDATA_AUTO_INSTALL", "")).strip() == "1"


def check_imports(module_names: Iterable[str]) -> List[str]:
    missing: List[str] = []
    for m in module_names:
        try:
            __import__(m)
        except Exception:
            missing.append(m)
    return missing


def _pip_install(packages: List[str]) -> Tuple[List[str], List[str]]:
    installed: List[str] = []
    errors: List[str] = []
    for p in packages:
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", p],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
                text=True,
            )
            if proc.returncode == 0:
                installed.append(p)
            else:
                errors.append(f"pip install {p} failed rc={proc.returncode}: {proc.stdout[-500:]}")
        except Exception as e:
            errors.append(f"pip install {p} exception: {e}")
    return installed, errors


def ensure_deps(
    *,
    essential_imports: Optional[Iterable[str]] = None,
    auto_install: Optional[bool] = None,
) -> DepsStatus:
    """Ensure AltData dependencies are importable.

    Fail-closed semantics apply to AltData only: if deps are missing,
    the caller should disable AltData but continue training.
    """

    essential = list(essential_imports or DEFAULT_IMPORTS)
    missing = check_imports(essential)
    if not missing:
        return DepsStatus(ok=True, missing=[], attempted_install=False, installed=[], errors=[])

    allow = _auto_install_enabled() if auto_install is None else bool(auto_install)
    if not allow:
        return DepsStatus(ok=False, missing=missing, attempted_install=False, installed=[], errors=[])

    installed, errors = _pip_install(list(missing))
    # re-check
    missing2 = check_imports(essential)
    ok = len(missing2) == 0
    return DepsStatus(ok=ok, missing=missing2, attempted_install=True, installed=installed, errors=errors)


def read_requirements_file(req_path: Path) -> List[str]:
    pkgs: List[str] = []
    try:
        for line in req_path.read_text().splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            pkgs.append(s)
    except Exception:
        return []
    return pkgs


def ensure_from_requirements_file(
    *,
    req_path: Path,
    auto_install: Optional[bool] = None,
) -> DepsStatus:
    # We only check importability via DEFAULT_IMPORTS; the file is for operators.
    # This keeps the bootstrap lean and predictable.
    _ = read_requirements_file(req_path)
    return ensure_deps(auto_install=auto_install)
