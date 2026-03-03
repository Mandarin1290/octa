"""Asset-class contract routing for IBKR order submission.

Maps asset_class strings to ContractSpec (exchange, currency, contract_type, multiplier).
Used by IBKRContractAdapter (sandbox) and IBKRIBInsyncAdapter (live) to resolve the
correct IB contract type for every order.

Fail-closed: unknown/empty asset_class raises RuntimeError with UNKNOWN_ASSET_CLASS prefix.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

_ROUTING_TABLE: Dict[str, Dict[str, Any]] = {
    "equity":   {"contract_type": "stock",  "exchange": "SMART",    "currency": "USD",     "multiplier": None},
    "equities": {"contract_type": "stock",  "exchange": "SMART",    "currency": "USD",     "multiplier": None},
    "stock":    {"contract_type": "stock",  "exchange": "SMART",    "currency": "USD",     "multiplier": None},
    "etf":      {"contract_type": "stock",  "exchange": "SMART",    "currency": "USD",     "multiplier": None},
    "forex":    {"contract_type": "forex",  "exchange": "IDEALPRO", "currency": "FOREIGN", "multiplier": None},
    "fx_carry": {"contract_type": "forex",  "exchange": "IDEALPRO", "currency": "FOREIGN", "multiplier": None},
    "futures":  {"contract_type": "future", "exchange": "GLOBEX",   "currency": "USD",     "multiplier": None},
    "future":   {"contract_type": "future", "exchange": "GLOBEX",   "currency": "USD",     "multiplier": None},
    "index":    {"contract_type": "index",  "exchange": "CBOE",     "currency": "USD",     "multiplier": None},
    "options":  {"contract_type": "option", "exchange": "CBOE",     "currency": "USD",     "multiplier": 100},
    "option":   {"contract_type": "option", "exchange": "CBOE",     "currency": "USD",     "multiplier": 100},
    "crypto":   {"contract_type": "crypto", "exchange": "PAXOS",    "currency": "USD",     "multiplier": None},
    "bond":     {"contract_type": "bond",   "exchange": "SMART",    "currency": "USD",     "multiplier": None},
}

# Maps IBKR exchange → pretrade venue name for calendar check
_EXCHANGE_TO_VENUE: Dict[str, str] = {
    "SMART":    "NYSE",
    "IDEALPRO": "FOREX",
    "GLOBEX":   "CME",
    "CBOE":     "CBOE",
    "PAXOS":    "CRYPTO",
}


@dataclass(frozen=True)
class ContractSpec:
    """Resolved contract routing metadata for a single order."""

    symbol: str
    asset_class: str
    contract_type: str
    exchange: str
    currency: str
    multiplier: Optional[int]


def resolve_contract_spec(symbol: str, asset_class: str) -> ContractSpec:
    """Resolve routing metadata for symbol + asset_class.

    Raises RuntimeError on unknown asset_class, missing exchange, or missing currency.
    """
    ac = str(asset_class).lower().strip()
    if not ac or ac == "unknown":
        raise RuntimeError(f"UNKNOWN_ASSET_CLASS:{symbol}:{ac!r}")
    meta = _ROUTING_TABLE.get(ac)
    if meta is None:
        raise RuntimeError(f"UNKNOWN_ASSET_CLASS:{symbol}:{ac!r}")
    exchange = meta.get("exchange", "")
    currency = meta.get("currency", "")
    if not exchange:
        raise RuntimeError(f"EXCHANGE_MISSING:{symbol}:{ac!r}")
    if not currency:
        raise RuntimeError(f"CURRENCY_MISSING:{symbol}:{ac!r}")
    return ContractSpec(
        symbol=symbol,
        asset_class=ac,
        contract_type=str(meta["contract_type"]),
        exchange=exchange,
        currency=currency,
        multiplier=meta.get("multiplier"),
    )


def check_market_open(exchange: str, ts: Optional[datetime] = None) -> Optional[str]:
    """Return None if market is open (or unknowable), error string if closed.

    Uses pretrade venue calendar.  Missing calendar entry → assume open (safe default).
    """
    from octa_vertex.market_hours import pretrade_check  # local import to avoid circular dep

    venue = _EXCHANGE_TO_VENUE.get(exchange)
    if venue is None:
        return None  # unknown exchange mapping → allow
    result = pretrade_check({"venue": venue}, ts=ts)
    reason = str(result.get("reason", ""))
    if not result.get("eligible", True) and reason != "missing_calendar":
        return f"MARKET_CLOSED:{venue}:{reason}"
    return None
