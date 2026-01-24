"""OCTA package entrypoint."""

from __future__ import annotations

import importlib


def main() -> int:
    try:
        cli = importlib.import_module("octa.cli")
        if hasattr(cli, "main"):
            return int(cli.main())
    except Exception:
        print("OCTA package installed. CLI not yet available.")
        return 0
    print("OCTA package installed. CLI not yet available.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
