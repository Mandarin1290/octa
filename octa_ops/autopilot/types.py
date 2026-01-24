from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class UniverseSymbol:
    symbol: str
    asset_class: str  # equity|etf|fx|future|crypto|index|option|unknown
    currency: Optional[str] = None
    session: Optional[str] = None  # equities_rth|fx_24_5|crypto_24_7|unknown
    source: Optional[str] = None
    parquet_paths: Optional[Dict[str, str]] = None  # timeframe -> path


@dataclass(frozen=True)
class GateDecision:
    symbol: str
    timeframe: str
    stage: str  # data_quality|global|train
    status: str  # PASS|FAIL|SKIP
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class PaperOrderIntent:
    symbol: str
    timeframe: str
    model_id: str
    side: str  # BUY|SELL
    qty: float
    order_type: str = "MKT"
    limit_price: Optional[float] = None
    client_order_id: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ResourceBudgets:
    max_runtime_s: int = 3600
    max_ram_mb: int = 12000
    max_disk_mb: int = 20000
    max_workers: int = 4
    max_threads: int = 4


def now_utc_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def stable_hash(obj: Any) -> str:
    import hashlib
    import json

    raw = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def normalize_timeframe(tf: str) -> str:
    t = str(tf or "").strip()
    return t.upper().replace("MIN", "M")


def timeframe_seconds(tf: str) -> Optional[int]:
    tf = normalize_timeframe(tf)
    if tf == "1D":
        return 86400
    if tf == "1H":
        return 3600
    if tf in {"30M", "30m".upper()}:
        return 1800
    if tf in {"15M", "15m".upper()}:
        return 900
    if tf == "5M":
        return 300
    if tf == "1M":
        return 60
    return None
