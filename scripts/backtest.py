from __future__ import annotations

from typing import List


def walk_forward_backtest(model_factory, X: List[list], y: List[float], initial_window: int = 10) -> dict:
    n = len(X)
    if n <= initial_window:
        return {"error": "insufficient data for backtest"}
    preds = []
    trues = []
    for t in range(initial_window, n):
        X_train = X[:t]
        y_train = y[:t]
        X_test = [X[t]]
        y_test = [y[t]]
        m = model_factory()
        m.fit(X_train, y_train)
        p = m.predict(X_test)[0]
        preds.append(float(p))
        trues.append(float(y_test[0]))

    # compute simple metrics and produce a pseudo-PnL series for HF checks
    import numpy as np
    if not preds:
        return {"mse": 0.0, "mae": 0.0, "n": 0, "returns": [], "equity": [], "sharpe": 0.0, "max_drawdown": 0.0}

    preds_a = np.array(preds)
    trues_a = np.array(trues)
    mse = float(np.mean((preds_a - trues_a) ** 2))
    mae = float(np.mean(np.abs(preds_a - trues_a)))

    # construct simple returns series: relative prediction error sign as proxy
    # r_t = (pred - true) / (abs(true) + eps)
    eps = 1e-9
    returns = ((preds_a - trues_a) / (np.abs(trues_a) + eps)).tolist()

    # equity curve: cumulative returns
    equity = np.cumsum(returns).tolist()

    # annualized-ish Sharpe (assume daily frequency); use sqrt(252)
    try:
        mean_r = float(np.mean(returns))
        std_r = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
        sharpe = float((mean_r / std_r) * (252 ** 0.5)) if std_r and std_r > 0 else 0.0
    except Exception:
        sharpe = 0.0

    # max drawdown on equity curve
    try:
        eq = np.array(equity)
        peaks = np.maximum.accumulate(eq)
        drawdown = (peaks - eq) / (peaks + eps)
        max_dd = float(np.max(drawdown)) if drawdown.size else 0.0
    except Exception:
        max_dd = 0.0

    return {
        "mse": mse,
        "mae": mae,
        "n": len(preds),
        "returns": returns,
        "equity": equity,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
    }
