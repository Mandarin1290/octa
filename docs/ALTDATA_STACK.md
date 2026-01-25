# AltData Feature Stack (Plan)

## Source → Feature → Gate Mapping (Baseline)

| Source | Feature Module | Gate/Timeframe | Notes |
| --- | --- | --- | --- |
| FRED | macro_features | Global Regime (1D) | Primary macro input; carried forward intraday |
| ECB/SDW | macro_features | Global Regime (1D) | EU rates/inflation/liquidity proxies |
| WorldBank/OECD | macro_features | Global Regime (1D) | Slow-moving global indicators |
| EIA | macro_features/event_features | Global Regime (1D) | Energy shock risk; carry-forward scalar |
| GDELT | event_features | Global Regime (1D) + Structure (30M) + Signal (1H) | Event risk score with lag (no look-ahead) |
| EDGAR | fundamental_features + event_features | Structure (30M) + Signal (1H) | Quality + event flags |
| COT | flow_features | Global Regime (1D) + Signal (1H) | Positioning bias |
| Google Trends | attention_features | Signal (1H) + optional Structure (30M) | Attention momentum/hype |
| Wikipedia Pageviews | attention_features | Signal (1H) | Attention proxy |
| Reddit | sentiment_features | Signal (1H) | Retail sentiment modifier |
| Stooq | macro_features | Global Regime (1D) | Market proxy returns/volatility |
| Optional (Stooq/FMP/IEX/Polygon) | fundamentals/liquidity | Structure (30M) / Execution (5M) | Only if enabled |

## Gate Integration Principles
- No gate makes network calls; all data comes from cached snapshots + registry.
- 1D macro features are computed daily and carried forward intraday.
- 30M structure uses slow-moving fundamentals + event risk + regime carry.
- 1H signal uses sentiment/attention/positioning modifiers.
- 5M execution and 1M micro use only liquidity proxies (fast).

## Implementation Checklist
1) Core abstractions (sources, cache, orchestrator, registry).
2) Source adapters (free sources, offline cache first).
3) Feature builders + provenance in DuckDB/sqlite.
4) Gate adapter integrations (fail-closed, config-driven).
5) Tests + smoke script + offline fixtures.
6) Docs (runbook + provenance).

## Timeframe Alignment
- Global: 1D features computed once per as-of date.
- Structure: uses last-known 1D features + fundamental/event flags.
- Signal: uses 1H features aligned to nearest prior snapshot.
- Execution/Micro: consume precomputed liquidity scalars only.

Note: current cascade implementation uses `L2_signal_1H`; the 1H feature set is aligned to the latest available snapshot at or before the bar. GDELT availability respects lag hours (default 6h).

## Safety
- If cache missing and net disabled, features are empty and gates run unchanged.
- All secrets via env vars; no logging of tokens.
