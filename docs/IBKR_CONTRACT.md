**IBKR Adapter Contract (Sandbox-Only)**

Overview
- A strict interface layer that mirrors IBKR semantics for testing and sandbox runs.

Key rules
- NO real or paper account orders — sandbox-only.
- All interactions must be audited via the ledger.
- Ambiguity or adapter failure MUST trigger sentinel gates.

Components
- `BrokerAdapter` (abstract): submit/cancel/status/account snapshot.
- `IBKRContractAdapter`: validation, rate limits, instrument qualification and audit hooks.
- `IBKRSandboxDriver`: deterministic scripted status replay for testing.

Instrument qualification and margin errors are simulated. Rate limits and rejections are deterministic and configurable.
