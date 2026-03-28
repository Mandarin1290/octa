#!/usr/bin/env python3
"""Blocked paper entrypoint for Foundation scope."""
from __future__ import annotations

from octa.foundation.control_plane import block_non_canonical_entry


def main() -> int:
    block_non_canonical_entry("scripts/run_paper_live.py")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
