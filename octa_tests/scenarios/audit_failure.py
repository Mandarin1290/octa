from __future__ import annotations

import os
from typing import Any

from octa_ledger.store import LedgerStore
from octa_sentinel.engine import SentinelEngine
from octa_sentinel.policies import SentinelPolicy


def run(bus_path: str, tmpdir) -> dict[str, Any]:
    ledger_dir = os.path.join(tmpdir, "ledger")
    ls = LedgerStore(ledger_dir)
    # create minimal valid policy
    policy = SentinelPolicy(schema_version=1, name="test_policy")
    se = SentinelEngine(
        policy=policy, state_path=os.path.join(tmpdir, "state.json"), ledger_store=ls
    )

    # corrupt or make verify_chain fail: ensure there's no ledger.log
    try:
        os.remove(ls.log_path)
    except Exception:
        pass

    dec = se.evaluate({})
    return {"level": dec.level, "reason": dec.reason}
