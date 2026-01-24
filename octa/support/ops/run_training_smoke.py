from __future__ import annotations

from octa.core.orchestration.runner import run_cascade


def main() -> None:
    run_cascade(universe_limit=2)


if __name__ == "__main__":
    main()

