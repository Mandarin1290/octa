import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class Report:
    investor: str
    period_start: str
    period_end: str
    total_return: float
    annualized_return: Optional[float]
    volatility_annual: Optional[float]
    max_drawdown: float
    fees_total: float
    nav_start: float
    nav_end: float
    reconciliation_hash: str
    notes: List[str]
    details: Dict[str, Any]


def _parse_iso(dt: str) -> datetime:
    return datetime.fromisoformat(dt)


def _daily_returns(nav_series: List[Dict[str, Any]]) -> List[float]:
    # nav_series: list of {'date': iso, 'nav': float}
    rets: List[float] = []
    for i in range(1, len(nav_series)):
        prev = float(nav_series[i - 1]["nav"])
        cur = float(nav_series[i]["nav"])
        if prev == 0:
            rets.append(0.0)
        else:
            rets.append((cur / prev) - 1.0)
    return rets


def _annualize_return(total_return: float, days: float) -> Optional[float]:
    if days <= 0:
        return None
    try:
        return (1.0 + total_return) ** (365.0 / days) - 1.0
    except Exception:
        return None


def _volatility_annual(daily_returns: List[float]) -> Optional[float]:
    if not daily_returns:
        return None
    mean = sum(daily_returns) / len(daily_returns)
    var = sum((r - mean) ** 2 for r in daily_returns) / len(daily_returns)
    sd = math.sqrt(var)
    return sd * math.sqrt(252.0)


def _max_drawdown(nav_series: List[Dict[str, Any]]) -> float:
    peak = -float("inf")
    max_dd = 0.0
    for p in nav_series:
        nav = float(p["nav"])
        if nav > peak:
            peak = nav
        if peak > 0:
            dd = (peak - nav) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def generate_investor_report(
    investor: str,
    nav_series: List[Dict[str, Any]],
    fee_records: List[Dict[str, Any]],
    notes: Optional[List[str]] = None,
) -> Report:
    """Generate an investor-facing report.

    nav_series: ordered list of dicts with keys: 'date' (ISO) and 'nav' (NAV per share)
    fee_records: list of dicts with keys: 'date', 'type' and 'amount'
    """
    if not nav_series:
        raise ValueError("nav_series must contain at least one point")

    notes = notes or []
    start_dt = _parse_iso(nav_series[0]["date"])
    end_dt = _parse_iso(nav_series[-1]["date"])
    days = (end_dt - start_dt).days or 0

    nav_start = float(nav_series[0]["nav"])
    nav_end = float(nav_series[-1]["nav"])

    total_return = (nav_end / nav_start) - 1.0 if nav_start != 0 else 0.0
    ann = _annualize_return(total_return, days)

    daily_rets = _daily_returns(nav_series)
    vol_ann = _volatility_annual(daily_rets)
    max_dd = _max_drawdown(nav_series)

    fees_total = sum(float(f.get("amount", 0.0)) for f in fee_records)

    # Reconciliation hash ties inputs together deterministically
    recon_obj = {
        "investor": investor,
        "period": {"start": nav_series[0]["date"], "end": nav_series[-1]["date"]},
        "nav_series": [(p["date"], float(p["nav"])) for p in nav_series],
        "fees": sorted(
            [
                (f.get("date"), float(f.get("amount", 0.0)), f.get("type"))
                for f in fee_records
            ]
        ),
    }
    recon_hash = canonical_hash(recon_obj)

    details = {
        "daily_returns_count": len(daily_rets),
        "fees_count": len(fee_records),
    }

    # No forward-looking promises: add explicit note
    notes.append("This report is historical and does not guarantee future performance.")

    return Report(
        investor=investor,
        period_start=nav_series[0]["date"],
        period_end=nav_series[-1]["date"],
        total_return=total_return,
        annualized_return=ann,
        volatility_annual=vol_ann,
        max_drawdown=max_dd,
        fees_total=fees_total,
        nav_start=nav_start,
        nav_end=nav_end,
        reconciliation_hash=recon_hash,
        notes=notes,
        details=details,
    )
