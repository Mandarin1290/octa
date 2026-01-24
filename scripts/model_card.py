from __future__ import annotations

import json
from pathlib import Path
from typing import Dict


def write_model_card(out_dir: str, metadata: Dict, cv_results: Dict | None = None, backtest_results: Dict | None = None) -> Path:
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    metrics = metadata.metrics if hasattr(metadata, 'metrics') else metadata.get('metrics', {})
    # if backtest produced HF metrics, surface them into top-level metrics for gating visibility
    if backtest_results:
        try:
            if isinstance(backtest_results, dict):
                if 'sharpe' in backtest_results:
                    metrics.setdefault('backtest', {})['sharpe'] = backtest_results.get('sharpe')
                if 'max_drawdown' in backtest_results:
                    metrics.setdefault('backtest', {})['max_drawdown'] = backtest_results.get('max_drawdown')
        except Exception:
            pass

    card = {
        "model": metadata.asset_id if hasattr(metadata, 'asset_id') else metadata.get('asset_id', 'model'),
        "version": metadata.version if hasattr(metadata, 'version') else metadata.get('version', 'v1'),
        "metrics": metrics,
        "cv": cv_results or {},
        "backtest": backtest_results or {},
        "created_at": metadata.created_at if hasattr(metadata, 'created_at') else metadata.get('created_at')
    }
    out = p / "model_card.json"
    out.write_text(json.dumps(card, indent=2))
    return out
