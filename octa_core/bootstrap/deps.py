from __future__ import annotations

import importlib
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, Union


@dataclass(frozen=True)
class PackageSpec:
    """Represents a Python dependency.

    - name: pip package name (what to pass to pip)
    - import_name: module name used for import checks
    - version_spec: PEP440-style range, used for pip install (best-effort)
    - critical: if missing or incompatible, abort startup
    - feature_flag: dotted path in octa_features.yaml that must be true to require/install
    - heavy: optional heavyweight libs must only be installed when enabled
    """

    name: str
    import_name: str
    version_spec: str = ""
    critical: bool = True
    feature_flag: Optional[str] = None
    heavy: bool = False

    def pip_requirement(self) -> str:
        return f"{self.name}{self.version_spec}" if self.version_spec else self.name


def check_import(import_name: str) -> bool:
    try:
        importlib.import_module(import_name)
        return True
    except Exception:
        return False


def _run_pip_install(requirements: Sequence[str]) -> None:
    cmd = [sys.executable, "-m", "pip", "install", *requirements]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        out = (proc.stdout or "")[-4000:]
        err = (proc.stderr or "")[-4000:]
        raise RuntimeError(
            "pip_install_failed\n"
            f"cmd={cmd!r}\n"
            f"stdout_tail=\n{out}\n"
            f"stderr_tail=\n{err}\n"
        )


def ensure_installed(packages: Union[Sequence[PackageSpec], Sequence[str]]) -> None:
    """Ensure packages are importable.

    Accepts either:
    - a list of PackageSpec (preferred; supports import checks)
    - a list of pip requirement strings (best-effort)
    """

    if not packages:
        return

    if isinstance(packages[0], str):  # type: ignore[index]
        reqs = list(packages)  # type: ignore[assignment]
        _run_pip_install(reqs)
        return

    packages = packages  # type: ignore[assignment]
    missing: List[PackageSpec] = [p for p in packages if not check_import(p.import_name)]
    if not missing:
        return

    reqs = [p.pip_requirement() for p in missing]
    _run_pip_install(reqs)

    still_missing = [p for p in missing if not check_import(p.import_name)]
    if still_missing:
        names = ", ".join(f"{p.name} (import {p.import_name})" for p in still_missing)
        raise RuntimeError(f"dependency_install_incomplete: {names}")


_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+).*$")


def _get_version(import_name: str) -> Optional[str]:
    try:
        m = importlib.import_module(import_name)
        return getattr(m, "__version__", None)
    except Exception:
        return None


def verify_versions(packages: Sequence[PackageSpec]) -> Tuple[List[str], List[str]]:
    """Best-effort version sanity.

    Returns (warnings, errors). This is intentionally conservative and does not attempt full PEP440 parsing.
    """

    warnings: List[str] = []
    errors: List[str] = []

    for p in packages:
        if not check_import(p.import_name):
            msg = f"missing_import:{p.import_name} (pip {p.pip_requirement()})"
            (errors if p.critical else warnings).append(msg)
            continue

        v = _get_version(p.import_name)
        if not v or not p.version_spec:
            continue

        # Basic guard: if version_spec is a range like '>=x,<y', ensure major.minor isn't wildly behind.
        # Not a full resolver; it just prevents obviously ancient installs.
        m = _VERSION_RE.match(str(v))
        if not m:
            continue
        major = int(m.group(1))
        if major == 0:
            warnings.append(f"pre1.0_pkg:{p.import_name} version={v}")

    return warnings, errors


def _flag_enabled(flags: dict, dotted: str) -> bool:
    cur = flags
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return False
        cur = cur.get(part)
    return bool(cur)


def select_required_packages(feature_flags: dict) -> List[PackageSpec]:
    """Select required packages based on feature flags.

    Only installs heavy optional dependencies when explicitly enabled.
    """

    # Base runtime deps (already used by Octa broadly). Keep conservative ranges.
    specs: List[PackageSpec] = [
        PackageSpec("numpy", "numpy", ">=1.23,<3", critical=True),
        PackageSpec("pandas", "pandas", ">=1.5,<3", critical=True),
        PackageSpec("pyarrow", "pyarrow", ">=10,<30", critical=True),
        PackageSpec("fastparquet", "fastparquet", ">=2023.10,<2026", critical=False),
        PackageSpec("scipy", "scipy", ">=1.9,<2", critical=False),
        PackageSpec("pydantic", "pydantic", ">=1.10,<3", critical=True),
        PackageSpec("PyYAML", "yaml", ">=6,<7", critical=True),
        PackageSpec("requests", "requests", ">=2.28,<3", critical=True),
        PackageSpec("tenacity", "tenacity", ">=8,<10", critical=True),
        # Execution adapters
        PackageSpec("ib_insync", "ib_insync", ">=0.9.86,<1.0", critical=False, feature_flag="features.execution.ibkr_ib_insync.enabled", heavy=True),
        # Security
        PackageSpec("cryptography", "cryptography", ">=41,<44", critical=False, feature_flag="features.security.encryption_at_rest.enabled", heavy=True),
        PackageSpec("PyNaCl", "nacl", ">=1.5,<2", critical=False, feature_flag="features.security.encryption_at_rest.enabled", heavy=True),
        PackageSpec("keyring", "keyring", ">=24,<26", critical=False, feature_flag=None, heavy=False),
        # Control plane
        PackageSpec("fastapi", "fastapi", ">=0.110,<1.0", critical=False, feature_flag="features.control_plane.enabled", heavy=True),
        PackageSpec("uvicorn", "uvicorn", ">=0.23,<1.0", critical=False, feature_flag="features.control_plane.enabled", heavy=True),
        # Telegram
        PackageSpec("python-telegram-bot", "telegram", ">=20,<22", critical=False, feature_flag="features.telegram_control.enabled", heavy=True),
        # Quant/Risk
        PackageSpec("QuantLib-Python", "QuantLib", ">=1.30,<2", critical=False, feature_flag="features.quantlib.enabled", heavy=True),
        # Portfolio optim (optional)
        PackageSpec("riskfolio-lib", "riskfolio", ">=4,<6", critical=False, feature_flag="features.portfolio_optim.riskfolio.enabled", heavy=True),
        PackageSpec("cvxpy", "cvxpy", ">=1.3,<2", critical=False, feature_flag="features.portfolio_optim.cvxpy.enabled", heavy=True),
    ]

    selected: List[PackageSpec] = []
    for s in specs:
        if s.feature_flag is None:
            selected.append(s)
            continue
        if _flag_enabled(feature_flags, s.feature_flag):
            selected.append(s)

    # Never auto-install heavy libs unless explicitly enabled.
    selected = [s for s in selected if (not s.heavy) or (s.feature_flag and _flag_enabled(feature_flags, s.feature_flag))]

    # Allow operator to disable auto-install entirely via env
    if os.getenv("OCTA_DISABLE_AUTO_INSTALL", "").strip().lower() in {"1", "true", "yes"}:
        return selected

    return selected


__all__ = [
    "PackageSpec",
    "check_import",
    "ensure_installed",
    "verify_versions",
    "select_required_packages",
]
