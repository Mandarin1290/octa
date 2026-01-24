#!/usr/bin/env python3
"""Legacy shim for scripts/arm_training_from_gate.py."""
from __future__ import annotations

import importlib


def main() -> int:
    mod = importlib.import_module("scripts.arm_training_from_gate")
    if hasattr(mod, "main"):
        return int(mod.main())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
