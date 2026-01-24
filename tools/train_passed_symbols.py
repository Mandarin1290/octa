#!/usr/bin/env python3
"""Legacy shim for scripts/train_passed_symbols.py."""
from __future__ import annotations

import importlib


def main() -> int:
    mod = importlib.import_module("scripts.train_passed_symbols")
    if hasattr(mod, "main"):
        return int(mod.main())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
