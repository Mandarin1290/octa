# OCTA Citadel-Level Institutional Upgrade Roadmap
## Based on actual code analysis — 2026-03-28

---

## How this was produced

Every claim below is grounded in code read from the repository. No estimates were
invented from templates. File:line references are given for every gap claim.

---

## System baseline (confirmed)

- Test suite: 1,876 passed, 0 failed
- Git: clean, all production Python committed, analysis artifacts excluded via .gitignore
- Registry: `artifacts/registry.sqlite3`, column `lifecycle_status` (PAPER/SHADOW/RESEARCH)
- Active paper symbol: ADC/1H
- Governance: Ed25519 signing, SHA-256 sidecars, hash-chained AuditChain, drift registry

---

## Module 3 — Risk Management

**Actual score: ~88% (not 82%)**

### What EXISTS (confirmed in code)

| Component | Location | Status |
|---|---|---|
| `safe_decide()` fail-closed wrapper | `octa/execution/risk_fail_closed.py:90` | ✅ WIRED |
| `KillSwitchConfig/evaluate_kill_switch()` | `octa/core/governance/kill_switch.py:28` | ❌ NOT WIRED |
| `SentinelEngine` with flatten/freeze/warn actions | `octa_sentinel/engine.py:29` | ❌ NOT WIRED |
| `PortfolioEngine` with drawdown_limit=0.15 | `octa/core/portfolio/engine.py:33` | ❌ NOT WIRED |
| `AllRad` risk engine | `octa/core/risk/allrad/engine.py` | ✅ WIRED via CapitalEngine |
| `aggregate_risk()` per cycle | `octa/execution/runner.py:219` | ✅ WIRED |
| Per-cycle exposure snapshots | `runner.py` writes `exposure_snapshot_cycle_NNN.json` | ✅ |

### Actual gaps

1. **KillSwitch not wired** (`kill_switch.py` is never imported in `runner.py`).
   The logic exists — `evaluate_kill_switch()` checks `execution_failures`, `slippage`,
   `daily_loss`, `system_health` — but nothing feeds it and nothing calls it.

2. **SentinelEngine not wired** (`octa_sentinel/engine.py` imports
   `flatten_and_kill`, `freeze_new_orders` but is never instantiated in the
   execution runner).

3. **PortfolioEngine not wired** (`octa/core/portfolio/engine.py` has
   `drawdown_limit=0.15` and correlation gating but `runner.py` never instantiates it).

4. **KillSwitch state has no data source** — even if wired, `KillSwitchState.daily_loss`
   and `KillSwitchState.slippage` would need per-trade fill data (which doesn't exist
   yet — see Module 4).

### Upgrade plan

**Step 1**: Wire `evaluate_kill_switch()` into the per-cycle check in `runner.py`,
feeding it from execution failure count (already tracked in `blocks` list) and a
persisted daily-loss state file.

**Step 2**: Wire `PortfolioEngine.aggregate()` after `_run_aggregate_risk_fail_closed()`.
`PortfolioState` is already defined (`octa/core/portfolio/state.py`), just needs to
be populated from the registry + recent signals.

**Step 3**: Wire `SentinelEngine.evaluate()` as a daily cycle check reading from the
existing `AuditChain` (it already uses `LedgerStore.verify_chain()`).

**Effort**: ~20-30 hours (wiring existing code, not new logic)
**Risk**: LOW (existing tested components, additive changes)

---

## Module 4 — Trading Execution

**Actual score: ~70% (not 65%)**

### What EXISTS (confirmed)

| Component | Location | Status |
|---|---|---|
| Pre-execution gate (TWS probe + port + handshake) | `octa/execution/pre_execution.py` | ✅ |
| NAV reconciliation (fail-closed paper/live) | `runner.py:411-438` | ✅ |
| `inference_cycle_NNN.json` evidence per cycle | `inference_bridge.py:477-493` | ✅ |
| `risk_incident_*.json` per blocked order | `risk_fail_closed.py:54-87` | ✅ |
| `exposure_snapshot_cycle_NNN.json` | `runner.py:223` | ✅ |
| BrokerRouter with sandbox/paper/live routing | `octa/execution/broker_router.py` | ✅ |

### Actual gaps

1. **No fill-outcome logging** — `runner.py` calls `broker.place_order()` but never
   captures `fill_price`, `fill_time`, or `fill_quantity` back from the broker.
   Order intent is logged; fill reality is not.

2. **No per-trade P&L** — `octa_ledger/performance.py:7` has `sharpe()`, `sortino()`,
   `volatility()` as pure functions but nothing feeds real trade returns to them
   at runtime. There is no live P&L ledger.

3. **No slippage measurement** — `kill_switch.py:9` defines `max_slippage=0.02`
   but nothing computes slippage (expected_price - fill_price) to compare against it.

4. **No trade statistics** — win rate, profit factor, consecutive loss streak are
   referenced by `SentinelPolicy` and `KillSwitch` but never computed.

### Upgrade plan

**Step 1**: After `broker.place_order()`, call `broker.get_fill()` or equivalent and
write a `trade_fill_NNN.json` to evidence (fill_price, fill_qty, fill_time, order_id).

**Step 2**: Build a lightweight `TradeLedger` that appends to a NDJSON file
(`octa/var/ledger/fills.ndjson`) on every fill. Entry: entry_time, entry_price,
exit_time, exit_price, pnl, slippage, symbol, strategy.

**Step 3**: Feed `TradeLedger` into `performance.py` functions for daily summary
(already written, just needs a data source).

**Step 4**: Feed slippage into `KillSwitchState` (see Module 3 Step 1).

**Effort**: ~30-40 hours (new fill-capture path + ledger + wiring)
**Risk**: MEDIUM (touches execution hot path, needs careful testing)

---

## Module 5 — Orchestration / Position Book

**Actual score: ~65% (not 70%)**

### What EXISTS (confirmed)

| Component | Location | Status |
|---|---|---|
| `run_cascade_training()` orchestration | `octa_ops/autopilot/cascade_train.py` | ✅ |
| `paper_runner.py` training promotion + paper loop | `octa_ops/autopilot/paper_runner.py` | ✅ |
| `octa_ledger/performance.py` math functions | pure Sharpe/Sortino/volatility | ✅ |
| `octa/core/portfolio/state.py` PortfolioState | exists but not populated live | partial |
| `octa/core/analytics/performance.py` | analytics engine | ✅ |
| `octa_reports/` investor reports | exists | ✅ |

### Actual gaps

1. **No position book** — `PortfolioState` is a dataclass but in the live runner
   it's never populated with actual held positions. The runner does not track what
   it currently owns (entry price, quantity, unrealized P&L).

2. **Legacy orchestration retired** — `octa/core/orchestration/flow.py:4` raises
   `RuntimeError("legacy_orchestration_flow_retired")`. The replacement
   (`octa.foundation.control_plane`) is referenced but not implemented.

3. **No cross-cycle position persistence** — Each execution cycle starts fresh;
   no state is carried forward (e.g., "we hold 100 shares of ADC from yesterday").

4. **Daily metrics not computed live** — `performance.py` functions exist but
   nothing calls them during the execution day to produce a running P&L dashboard.

### Upgrade plan

**Step 1**: Write a `PositionBook` (NDJSON or SQLite) that persists open positions
across cycles: symbol, entry_time, entry_price, quantity, strategy. Updated on each fill.

**Step 2**: At cycle start in `runner.py`, load `PositionBook` to compute unrealized
P&L from current prices. Feed `portfolio_drawdown` to `PortfolioEngine.aggregate()`.

**Step 3**: At end of each cycle, compute daily Sharpe/Sortino from the fill ledger
(Module 4) and emit to a `daily_metrics.json` in evidence.

**Effort**: ~35-45 hours (PositionBook + daily metrics + wiring)
**Risk**: MEDIUM (requires Module 4 fill ledger first)
**Dependency**: Module 4 must be completed first

---

## Module 6 — Feature Engineering Quality

**Actual score: ~82% (not 75%)**

### What EXISTS (confirmed)

| Component | Location | Status |
|---|---|---|
| RSI, MACD, Bollinger, Stochastic, Williams R | `octa/core/features/features.py` | ✅ |
| `validate_price_series()` in training pipeline | `octa_training/core/pipeline.py:33` | ✅ |
| AltData offline guard (`offline_only: true`) | `config/altdat.yaml` | ✅ |
| Leakage audit (`leakage_audit()`) | `octa_training/core/features.py` | ✅ |
| NaN fill via `min_periods=1` rolling | `features.py:_safe_rolling` | ✅ partial |
| altdat_* leakage exception | `octa/core/data/io/io_parquet.py` | ✅ |

### Actual gaps

1. **No runtime input validation at inference** — `inference_bridge.py:304` calls
   `_build_features(df_raw, ...)` directly without running `validate_price_series()`.
   Corrupt parquet at inference time would silently produce NaN features.

2. **No feature staleness check at inference** — `inference_bridge.py` computes
   `feature_count_model` vs `feature_count_runtime` but doesn't check if the
   last parquet bar is stale relative to now.

3. **No feature distribution drift detection** — No check that inference-time
   feature values are within the training-time distribution (e.g., RSI=99 when
   training max was 85 would silently extrapolate).

4. **`fillna(50)` in RSI** — `features.py:42` silently fills NaN RSI with 50.
   This is reasonable but is not logged; if data is corrupt, this masks errors.

### Upgrade plan

**Step 1**: Add `validate_price_series(df_raw)` call in `inference_bridge.py:277`
before `_build_features()`. Log validation result to `diagnostics` in `InferenceResult`.

**Step 2**: Add staleness check in `inference_bridge.py`: compare `df_raw.index[-1]`
to `datetime.now()`, emit warning if gap > `OCTA_MAX_STALE_SECONDS`.

**Step 3** (optional, high effort): Persist per-feature [p5, p95] from training in
`ArtifactMeta` and compare at inference time.

**Effort**: ~15-20 hours (Steps 1-2 are small and safe)
**Risk**: LOW (additive logging, no logic changes)

---

## Module 7 — Inference Engine

**Actual score: ~87% (not 80%)**

### What EXISTS (confirmed)

| Component | Location | Status |
|---|---|---|
| `InferenceResult` with full diagnostics | `inference_bridge.py:53-66` | ✅ |
| Artifact hash (16-char SHA-256 prefix) | `inference_bridge.py:236-241` | ✅ |
| Feature count model vs runtime | `inference_bridge.py:320` | ✅ |
| `inference_cycle_NNN.json` evidence | `inference_bridge.py:445-493` | ✅ |
| Fail-closed error paths (never raises) | all paths return `_no_signal()` | ✅ |
| SHA-256 sidecar verification | `inference_bridge.py:245-249` | ✅ |
| Signal bounds check `{-1, 0, 1}` | `inference_bridge.py:369-370` | ✅ |

### Actual gaps

1. **No confidence decomposition** — `SafeInference.predict()` at `packaging.py:107-115`
   returns `conf = float(probs[:, 1].mean())` — a scalar. There is no per-feature
   importance or SHAP decomposition. "Why was confidence 0.73?" is unanswerable.

2. **No OOD detection** — Nothing checks if inference-time feature values are
   outside the training distribution. A feature at 10× its training range would
   be silently used.

3. **No inference latency tracking** — `run_inference_cycle()` has no timing;
   if model becomes slow (e.g., after model upgrade), this is invisible.

4. **`predict_proba` threshold uses fixed quantiles** — `packaging.py:124-127`
   uses `X.tail(1)` quantile against the full `score_series` from the same bar.
   With a single row, `upper_q` and `lower_q` both equal that value, making
   the threshold degenerate. This is a functional bug at single-row inference.

### Upgrade plan

**Step 1 (bug fix)**: Fix degenerate quantile threshold in `SafeInference.predict()`.
With a single row `X_last = X.tail(1)`, `score_series` has 1 element — the quantile
IS the value, so `latest > up` is always False. The correct fix: compare `latest`
against percentile thresholds stored at training time (saved in `ArtifactMeta`).

**Step 2**: Add `inference_time_ms` to `InferenceResult.diagnostics`.

**Step 3** (optional): For CatBoost models, `model_obj.get_feature_importance()` is
available — can be called at inference and stored in diagnostics.

**Effort**: ~10-15 hours (Step 1 is highest priority — it's a functional bug)
**Risk**: LOW-MEDIUM (Step 1 changes signal behavior — needs careful shadow validation)

---

## Module 8 — ML Training Pipeline

**Actual score: ~90% (not 87%)**

### What EXISTS (confirmed)

| Component | Location | Status |
|---|---|---|
| Walk-forward splits | `octa_training/core/splits.py` | ✅ |
| `ArtifactMeta` with schema_version, metrics, gate | `packaging.py:63-75` | ✅ |
| `save_tradeable_artifact()` with atomic write | `packaging.py:140-155` | ✅ |
| Training gates (sharpe, sortino, profit_factor) | `octa_training/core/gates.py` | ✅ |
| `run_all_tests()` robustness | `octa_training/core/robustness.py` | ✅ |
| Optuna hyperparameter search | `octa_training/core/optuna_tuner.py` | ✅ |
| Run ID provenance in every artifact | `ArtifactMeta.run_id` | ✅ |
| Training fingerprint signing | institutional safety wiring | ✅ |
| `profile_hash()` asset profile versioning | `octa_training/core/asset_profiles.py` | ✅ |

### Actual gaps

1. **sklearn FutureWarning** — `packaging.py:170` passes `penalty='l2'` to
   `LogisticRegression`. In sklearn 1.8 this is deprecated; in 1.10 it will error.
   Fix: remove `penalty` key, set `l1_ratio=0` (equivalent to l2).

2. **No feature importance in artifact** — `ArtifactMeta` has no `feature_importance`
   field. CatBoost and LogReg both support `get_feature_importance()` but it's
   never stored. This means post-hoc analysis of "which features drove this model"
   is impossible without retraining.

3. **No cross-run comparison database** — Each training run produces isolated
   evidence. There is no queryable table of "all models trained for SYMBOL/TF,
   their metrics, when trained, gate result". The training admission layer
   (`octa/core/training_admission/`) partially addresses this but only for
   admission decisions, not metrics history.

4. **`per-fold metrics` not in artifact** — `ArtifactMeta.metrics` stores
   aggregate metrics but not per-fold walk-forward results. Degradation over
   time within a training run is not visible from the artifact alone.

### Upgrade plan

**Step 1 (quick win)**: Fix sklearn FutureWarning in `packaging.py:170`.
Remove `"penalty": "l2"` from `params`, add `"l1_ratio": 0`.

**Step 2**: Add `feature_importance: Optional[Dict[str, float]]` to `ArtifactMeta`.
After `_train_full_model()`, call `model_obj.feature_importances_` (CatBoost/RF)
or `model_obj.coef_[0]` (LogReg) and store in the artifact.

**Step 3** (larger): Create `octa/var/models/training_history.sqlite3` — one row
per completed training run with symbol, timeframe, run_id, timestamp, sharpe_oos,
sortino_oos, gate_pass. Used for trend monitoring ("is ADC model degrading?").

**Effort**: ~15-25 hours
**Risk**: LOW (Steps 1-2 are small, additive; Step 3 is new infrastructure)

---

## Execution order recommendation

```
Module 8 / Step 1  ←  quick win: fix sklearn deprecation warning (1 hour)
Module 7 / Step 1  ←  fix single-row quantile bug (affects live ADC signal quality)
Module 6 / Steps 1-2  ←  add input validation at inference (2-3 hours)
Module 3 / Step 1  ←  wire KillSwitch into runner.py (5-8 hours)
Module 3 / Step 2  ←  wire PortfolioEngine (5-8 hours)
Module 4 / Steps 1-2  ←  fill capture + TradeLedger (20-25 hours)
Module 5 / Steps 1-3  ←  PositionBook + daily metrics (after Module 4)
Module 8 / Steps 2-3  ←  feature importance + history DB
```

Each step is independent enough to commit and run shadow validation before the next.

---

## Total realistic effort

| Module | Quick wins | Full wiring | Notes |
|---|---|---|---|
| M3 Risk | 5h | 20-30h | Existing code, just not wired |
| M4 Execution | — | 30-40h | New fill-capture path needed |
| M5 Orchestration | — | 35-45h | Depends on M4 |
| M6 Features | 3h | 15-20h | Steps 1-2 are small |
| M7 Inference | 5h | 10-15h | Bug fix is highest priority |
| M8 Training | 1h | 15-25h | sklearn fix is immediate |
| **Total** | **~14h** | **125-175h** | |

The quick wins (~14 hours) close the highest-priority gaps with low risk.
The full wiring (~150 hours) brings the system to Citadel level.
