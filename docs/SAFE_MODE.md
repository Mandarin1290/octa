# Automated Trading Halt & Safe Mode

This document describes the Safe Mode mechanisms enforcing trading halts and protected unwinds.

Principles
- Safe Mode overrides all strategy logic: when enabled, the system will not accept new entries.
- Existing positions are protected: Safe Mode allows exits that reduce absolute exposure; exits that increase exposure
  require explicit risk approval.
- Any state change or attempted trade is auditable via `SafeModeManager.audit_log`.

API
- `SafeModeManager.set_halt(flag, actor, reason)` — enable/disable global halt.
- `SafeModeManager.allow_trade(instrument, delta, trade_type, risk_approved=False)` — check whether a trade is permitted.
- `SafeModeManager.execute_trade(...)` — enforce policy and apply trade if permitted; raises on disallowed trade.

Usage
```
from octa_ops.safe_mode import SafeModeManager
sm = SafeModeManager({"AAPL": 100})
sm.set_halt(True, actor="ops")
# entry blocked
sm.execute_trade("AAPL", 10, trade_type="entry", actor="strategy")  # raises
# exit allowed if reduces exposure
sm.execute_trade("AAPL", -50, trade_type="exit", actor="ops")  # allowed
```
