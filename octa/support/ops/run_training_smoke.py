from __future__ import annotations

from octa.foundation.control_plane import run_foundation_training


def main() -> None:
    run_foundation_training(max_symbols=2, dry_run=True)


if __name__ == "__main__":
    main()
