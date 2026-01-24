from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from octa.core.data.io.io_parquet import load_parquet
from octa.core.gates.global_regime.gate import GlobalRegimeGate, GlobalRegimeGateConfig


@dataclass
class L1GlobalResult:
    symbol: str
    timeframe: str
    decision: str
    reason: str
    details: Dict[str, Any]


class L1GlobalAdapter:
    def __init__(self, config: Optional[GlobalRegimeGateConfig] = None) -> None:
        self._config = config or GlobalRegimeGateConfig()

    def evaluate(self, *, symbol: str, parquet_path: Optional[str]) -> L1GlobalResult:
        if not parquet_path:
            return L1GlobalResult(
                symbol=symbol,
                timeframe="1D",
                decision="SKIP",
                reason="missing_parquet",
                details={},
            )
        try:
            df = load_parquet(Path(parquet_path))
        except Exception as exc:
            return L1GlobalResult(
                symbol=symbol,
                timeframe="1D",
                decision="FAIL",
                reason="data_load_failed",
                details={"error": str(exc), "path": parquet_path},
            )

        if "close" not in df.columns:
            return L1GlobalResult(
                symbol=symbol,
                timeframe="1D",
                decision="FAIL",
                reason="missing_close",
                details={"path": parquet_path},
            )
        if not isinstance(df.index, pd.DatetimeIndex):
            return L1GlobalResult(
                symbol=symbol,
                timeframe="1D",
                decision="FAIL",
                reason="timestamp_not_datetimeindex",
                details={"path": parquet_path},
            )

        close = pd.to_numeric(df["close"], errors="coerce").dropna()
        if close.empty:
            return L1GlobalResult(
                symbol=symbol,
                timeframe="1D",
                decision="FAIL",
                reason="no_close_data",
                details={"path": parquet_path},
            )

        gate = GlobalRegimeGate(config=self._config)
        gate.set_series(close.tolist(), df.index[-len(close) :].tolist())
        outcome = gate.evaluate([symbol])
        artifacts = gate.emit_artifacts([symbol])
        regime_label = artifacts.get("regime_label") if isinstance(artifacts, dict) else None

        return L1GlobalResult(
            symbol=symbol,
            timeframe="1D",
            decision=outcome.decision.value,
            reason=str(regime_label or "gate_result"),
            details=dict(artifacts) if isinstance(artifacts, dict) else {},
        )
