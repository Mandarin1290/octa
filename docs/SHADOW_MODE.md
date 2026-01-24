# Shadow Mode (Live Prices, Zero Exposure)

Overview
--------
Shadow Mode lets OCTA run as if trading live while never sending orders to real brokers. It uses live market data to simulate fills, slippage, and PnL while enforcing the same allocator and sentinel gates. Shadow mode is intended for monitoring, dry-run validation, and rehearsing kill-switch/chaos drills without real exposure.

Key Rules
---------
- Shadow mode uses live price feeds as input but intercepts final execution.
- Orders traverse the same pipeline (allocator → sentinel → vertex → broker adapter), but the final broker call is skipped.
- Simulated fills use a deterministic slippage model to ensure reproducibility.
- Shadow PnL is tracked separately from paper/live PnL.
- Drawdowns and sentinel gates are applied to shadow PnL; a kill-switch still blocks further orders.

Files
-----
- `octa_vertex/shadow_executor.py` — core executor that simulates fills and keeps shadow positions.
- `octa_nexus/shadow_runtime.py` — runtime coordinator storing the `shadow_mode` flag and basic metrics.

Configuration
-------------
Set `shadow_mode: true` in runtime config to enable shadow execution. Other useful settings:

- `kill_threshold` (int): gate level at or above which orders are blocked (default 3).
- `shadow_drawdown_threshold` (float): fraction of notional at which shadow drawdown notifies sentinel (default 0.2).

Testing
-------
Unit tests validate that no live broker calls are made, shadow PnL is tracked, and kill-switch prevents shadow orders.
