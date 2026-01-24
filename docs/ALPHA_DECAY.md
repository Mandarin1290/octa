Alpha Decay Detection
=====================

Purpose
-------
Detect performance drift and alpha decay for strategy signals, while minimizing false positives and being regime‑aware.

Methods
-------
- Rolling attribution: compute rolling Pearson correlation between signal and subsequent returns.
- Signal efficacy decay: compare short‑term correlation vs long‑term baseline; flag drops beyond a fraction threshold.
- Regime normalization: correlations can be computed per regime (market regimes provided by caller) to avoid mixing regimes.

API
---
- `AlphaDecayDetector(long_window=120, short_window=20, drop_threshold=0.5, min_samples=30)` — constructor.
- `add_observation(date, signal_name, signal_value, ret, regime=None)` — add an observation.
- `rolling_correlation(signal_name, regime=None)` — returns baseline and recent correlations.
- `detect_decay(signal_name, regime=None)` — returns a `DecayAlert` when decay is detected.

Usage Notes
-----------
- Caller must supply regime labels when available to enable regime‑aware detection.
- Detector requires a minimum number of samples (`min_samples`) before producing decisions.
- Alerts are auditable; each alert contains an `evidence_hash` computed deterministically from inputs.
# Alpha Decay Detection

Overview
--------
Deterministic, statistics-based detection for strategy-specific alpha decay. Uses a combination of:

- Page-Hinkley (cumulative-sum) for change-point magnitude detection.
- Two-sample z-test (normal approximation) comparing a historical baseline window to a recent window for significance.
- A normalized decay score (0..1) combining magnitude, persistence and statistical confidence.

Guidelines
----------
- Provide returns as a chronological list (oldest -> newest) to `AlphaDecayDetector.detect_decay()`.
- The output is reproducible given the same ledger/return series.

Fields
------
- `decay_score`: 0..1 combined measure of decay severity.
- `confidence`: 0..1 derived from two-sided z-test p-value (1 - p).
- `changepoint_index`: index in the supplied returns where the Page-Hinkley detector first signaled (if any).
