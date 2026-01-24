#!/usr/bin/env python3
"""Legacy shim for scripts/download_aapl_1m.py."""
from __future__ import annotations

import importlib


def main() -> int:
    mod = importlib.import_module("scripts.download_aapl_1m")
    if hasattr(mod, "main"):
        return int(mod.main())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
