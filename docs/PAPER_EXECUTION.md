**PAPER EXECUTION**

The `PaperExecutor` simulates market-aware execution using:

- VWAP-style slicing across provided bars (each bar: `vwap`, `volume`, optional `ts`).
- Participation rate limit (`participation`) caps per-bar executed size.
- Partial fills supported; remaining is left unfilled and audited.
- Uses `octa_vertex.slippage` pre-trade estimates to compute impact and cost; applied to executed price deterministically.
- Sentinel is consulted before each slice; any freeze/critical decision halts execution and is audited.

Typical usage:

```py
from octa_vertex.paper_executor import PaperExecutor
exec = PaperExecutor(participation=0.1, ledger_api=ledger_api, sentinel=sentinel)
reports = exec.execute(order, bars, adv=1e6, sigma=0.02, half_spread=0.01)
```

Audit: every fill and halt is written to `octa_ledger` via `ledger_api.audit_or_fail()`.
