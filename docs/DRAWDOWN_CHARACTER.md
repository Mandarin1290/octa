# Drawdown Character Analysis

Overview
--------
Analyzes the temporal shape of drawdowns (duration, recovery speed, clustering) and classifies
profiles into `LONG_SHALLOW`, `SHARP_CRASH`, `QUICK_RECOVERY`, `CLUSTERED`, `MIXED`, or `NONE`.

Usage
-----
Call `analyze_drawdown(returns, window)` with chronological returns (oldest->newest). Returns a dict with:
- `episodes`: list of drawdown episodes (start, trough, end, depth, duration, recovery_time)
- `profile`: classification and metrics
- `drawdown_series`: point-wise drawdown values

Notes
-----
- Classification rules are deterministic and conservative; tune `window` for clustering sensitivity.
