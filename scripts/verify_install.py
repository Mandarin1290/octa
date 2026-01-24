#!/usr/bin/env python3
"""Verify local environment without installing dependencies."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


def main() -> None:
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    print("python_version=", sys.version.replace("\n", " "))
    missing = []
    for name in ["octa", "octa.core", "octa.support", "octa.infra", "octa.research", "yaml", "numpy", "pandas"]:
        try:
            importlib.import_module(name)
            print(f"import_ok={name}")
        except Exception as exc:
            print(f"import_fail={name} err={exc.__class__.__name__}")
            missing.append(name)
    if missing:
        print("missing_deps=", ",".join(missing))
    else:
        print("missing_deps=none")


if __name__ == "__main__":
    main()
