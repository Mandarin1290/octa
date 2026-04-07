"""
Market Hours & Holiday Governance for US Equities.
Fail-closed: When uncertain, block trade.
"""
from datetime import datetime, time
from typing import Tuple
import pytz

US_TZ = pytz.timezone("America/New_York")

US_HOLIDAYS = {
    "2024": ["01-01","01-15","02-19","03-29","05-27","06-19","07-04","09-02","11-28","12-25"],
    "2025": ["01-01","01-20","02-17","04-18","05-26","06-19","07-04","09-01","11-27","12-25"],
    "2026": ["01-01","01-19","02-16","04-03","05-25","06-19","07-04","09-07","11-26","12-25"],
}

def is_market_open(dt: datetime = None) -> Tuple[bool, str]:
    """
    Check if US equity market is open. Fail-closed.
    Returns (is_open, reason_code).
    reason_codes: OPEN | WEEKEND | HOLIDAY | OUTSIDE_REGULAR_SESSION | UNKNOWN_MARKET_STATE
    """
    try:
        if dt is None:
            dt = datetime.now(US_TZ)
        if dt.tzinfo is None:
            dt = US_TZ.localize(dt)
        else:
            dt = dt.astimezone(US_TZ)
        if dt.weekday() >= 5:
            return False, "WEEKEND"
        year = dt.strftime("%Y")
        if dt.strftime("%m-%d") in US_HOLIDAYS.get(year, []):
            return False, "HOLIDAY"
        if not (time(9, 30) <= dt.time() < time(16, 0)):
            return False, "OUTSIDE_REGULAR_SESSION"
        return True, "OPEN"
    except Exception:
        return False, "UNKNOWN_MARKET_STATE"

def can_trade(dt: datetime = None) -> Tuple[bool, str]:
    """Fail-closed wrapper for is_market_open."""
    return is_market_open(dt)
