from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from octa.core.orchestration.runner import run_cascade


def main() -> None:
    os.environ.setdefault("OCTA_ALLOW_NET", "0")
    os.environ.setdefault("OCTA_CONTEXT", "smoke")
    result = run_cascade(universe_limit=5)
    print("run_id:", result.run_id)
    print("survivors_l1:", len(result.survivors_l1))
    print("survivors_l2:", len(result.survivors_l2))
    print("survivors_l3:", len(result.survivors_l3))
    print("survivors_l4:", len(result.survivors_l4))
    print("survivors_l5:", len(result.survivors_l5))


if __name__ == "__main__":
    main()
