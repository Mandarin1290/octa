# Alpha Performance Feedback Loop

Purpose
- Feed realized performance into alpha scoring conservatively to reward persistent signals and ignore noise.

Core rules
- Use rolling performance windows and exclude the most recent `lag_periods` to avoid overreaction.
- Require at least `min_periods` samples before adjusting scores.
- Apply gentle multipliers capped by `max_adjust_pct` to avoid structural overfitting.

API
- `FeedbackEngine(window_size, lag_periods, min_periods, learning_rate, significance_threshold, max_adjust_pct)`
- `add_return(alpha_id, period, ret)` to feed realized return data.
- `adjust_scores(base_scores, current_period)` returns adjusted scores and multipliers.

Notes
- The engine is intentionally conservative: small transient moves are ignored, persistent returns shift scores slowly.
- For production, combine with statistical significance tests and out-of-sample validation windows.
