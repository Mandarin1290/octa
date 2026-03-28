"""Minimal OCTA CLI router with isolated offline governance entrypoints."""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print("Available commands: parquet-recycling, training-admission")
        return 0
    if args[0] == "parquet-recycling":
        from octa.core.data.recycling.cli import main as recycling_main

        return int(recycling_main(args[1:]))
    if args[0] == "training-admission":
        from octa.core.training_admission.cli import main as admission_main

        return int(admission_main(args[1:]))
    print("Unknown command. Available commands: parquet-recycling, training-admission")
    return 2
