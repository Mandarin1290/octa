Backtest Connectors
===================

This folder contains lightweight connector prototypes for backtesting libraries
used by the Octa project. The goal is to centralize adapter logic so higher-level
code can call a common API and the team can standardize inputs/outputs.

Current prototypes:
- `vectorbt_connector.py` - import-on-demand adapter for `vectorbt`.
- `zipline_connector.py` - shim for `zipline` with guidance for production wiring.

Usage example (vectorbt):

```bash
# install optionally
pip install vectorbt

# in Python
from octa_core.backtest.vectorbt_connector import run_vectorbt_backtest
# price_df: pandas Series/DataFrame; entries/exits: boolean Series
# res = run_vectorbt_backtest(price_df, entries, exits)
```

Notes:
- The connectors intentionally avoid importing heavy dependencies at module
  import-time; they raise helpful errors if the optional library is missing.
- For CI, these connectors do not require the libraries unless the feature is exercised.
