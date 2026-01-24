# Paper-to-Live Acceptance Standard

This document specifies the acceptance gates required before promoting a strategy from paper to candidate-live.

Hard rules
----------
- No promotion to "candidate live" without passing all gates.
- All gates are quantitative, deterministic and logged to the ledger.

Required gates
--------------
- Minimum paper runtime: default 60 trading days.
- Max drawdown threshold: configurable (default 20%).
- Stability of slippage vs forecast: relative tolerance configurable.
- Zero unresolved critical incidents (severity >= 2).
- Audit integrity: ledger verification must pass.

Process
-------
Run `PaperGates.evaluate_promotion(ledger)` which appends a `paper_gate.evaluation` audit event with details and pass/fail result. The evaluation itself is auditable and reproducible from the ledger.
