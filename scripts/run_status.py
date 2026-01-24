"""Lightweight entrypoint to validate wiring and show system status."""

from __future__ import annotations

from octa_fabric.config import ConfigurationError, load_config
from octa_sentinel.core import Sentinel


def main() -> int:
    try:
        cfg = load_config()
    except ConfigurationError as e:
        print("Configuration failed:", e)
        return 2

    sent = Sentinel.get_instance()
    print("OCTA foundation starting (dry-run)")
    print("env:", cfg.env)
    print("risk-enabled:", sent.is_enabled())
    # Do not perform executions in this script.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
