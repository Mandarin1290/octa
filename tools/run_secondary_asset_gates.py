#!/usr/bin/env python3
"""Legacy shim for scripts/run_secondary_asset_gates.py."""
from __future__ import annotations

import importlib


def main() -> int:
    mod = importlib.import_module("scripts.run_secondary_asset_gates")
    if hasattr(mod, "main"):
        return int(mod.main())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
