**Margin, Leverage & Financing Model**

Overview
- Provides conservative, cross-asset margin calculations for paper and live modes.

Instrument specs
- Define `InstrumentSpec` per instrument: `contract_multiplier`, `tick_size`, `margin_initial_rate`, `margin_maintenance_rate`, `haircut`.

Portfolio calculations
- `PortfolioMarginCalculator.compute(positions)` returns:
  - `gross_exposure`, `net_exposure`, `leverage`
  - `initial_margin`, `maintenance_margin`
  - `margin_utilization`, `headroom`
  - `borrow_cost_annual`, `funding_cost_annual`, `breach_flags`

Conservative defaults
- Unknown instruments use `conservative_margin_multiplier` to increase margin requirements.

Integration
- Feed results to `octa_sentinel.margin_gates.MarginGates.evaluate_and_act` to create sentinel gates and allocator scaling.
