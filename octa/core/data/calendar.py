"""Market hours and calendar engine.

UTC internal times. Venue calendars must be explicit; missing calendar => ineligible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo


@dataclass
class Session:
    weekday: int  # 0=Mon .. 6=Sun
    start_local: time
    end_local: time


@dataclass
class VenueCalendar:
    venue: str
    timezone: str
    sessions: List[Session] = field(default_factory=list)
    holidays: List[date] = field(default_factory=list)
    do_not_trade_windows: List[Tuple[time, time]] = field(
        default_factory=list
    )  # times in local tz

    def is_holiday(self, dt_utc: datetime) -> bool:
        loc = dt_utc.astimezone(ZoneInfo(self.timezone))
        return loc.date() in self.holidays

    def is_in_session(self, dt_utc: datetime) -> bool:
        loc = dt_utc.astimezone(ZoneInfo(self.timezone))
        # check weekday and time window
        loc_week = loc.weekday()
        loc_seconds = loc.hour * 3600 + loc.minute * 60 + loc.second
        for s in self.sessions:
            if loc_week != s.weekday:
                continue
            start_sec = (
                s.start_local.hour * 3600
                + s.start_local.minute * 60
                + s.start_local.second
            )
            end_sec = (
                s.end_local.hour * 3600 + s.end_local.minute * 60 + s.end_local.second
            )
            if start_sec <= end_sec:
                if start_sec <= loc_seconds <= end_sec:
                    return True
            else:
                # overnight session
                if loc_seconds >= start_sec or loc_seconds <= end_sec:
                    return True
        return False

    def is_in_do_not_trade(self, dt_utc: datetime) -> bool:
        loc = dt_utc.astimezone(ZoneInfo(self.timezone))
        loc_seconds = loc.hour * 3600 + loc.minute * 60 + loc.second
        for start, end in self.do_not_trade_windows:
            start_sec = start.hour * 3600 + start.minute * 60 + start.second
            end_sec = end.hour * 3600 + end.minute * 60 + end.second
            if start_sec <= end_sec:
                if start_sec <= loc_seconds <= end_sec:
                    return True
            else:
                if loc_seconds >= start_sec or loc_seconds <= end_sec:
                    return True
        return False


class CalendarRegistry:
    def __init__(self):
        self._cal: Dict[str, VenueCalendar] = {}

    def register(self, cal: VenueCalendar):
        self._cal[cal.venue] = cal

    def get(self, venue: str) -> VenueCalendar | None:
        return self._cal.get(venue)


registry = CalendarRegistry()


# Pre-register common venue calendars (explicit definitions)
def _register_defaults():
    # NYSE / NASDAQ: America/New_York, normal session 09:30-16:00 Mon-Fri, avoid open auction 09:28-09:32
    ny_tz = "America/New_York"
    sessions = [Session(w, time(9, 30), time(16, 0)) for w in range(0, 5)]
    do_not = [(time(9, 28), time(9, 32)), (time(15, 59), time(16, 10))]
    registry.register(
        VenueCalendar(
            "NYSE", ny_tz, sessions=list(sessions), do_not_trade_windows=do_not
        )
    )
    registry.register(
        VenueCalendar(
            "NASDAQ", ny_tz, sessions=list(sessions), do_not_trade_windows=do_not
        )
    )

    # CME: futures - near 24/5 with maintenance window 17:00-18:00 US/Eastern (example)
    cme_tz = "America/New_York"
    sessions = []
    for w in range(0, 5):
        sessions.append(Session(w, time(18, 0), time(17, 0)))
    registry.register(
        VenueCalendar(
            "CME",
            cme_tz,
            sessions=sessions,
            do_not_trade_windows=[(time(17, 0), time(18, 0))],
        )
    )

    # EUREX: Europe/Berlin 08:00-20:00 Mon-Fri
    eurex_tz = "Europe/Berlin"
    sessions = [Session(w, time(8, 0), time(20, 0)) for w in range(0, 5)]
    registry.register(VenueCalendar("EUREX", eurex_tz, sessions=sessions))

    # FX 24/5: open Sunday 22:00 UTC -> Friday 22:00 UTC
    fx_tz = "UTC"
    fx_sessions: List[Session] = []
    # Sunday partial
    fx_sessions.append(Session(6, time(22, 0), time(23, 59, 59)))
    # Mon-Fri full days until 22:00
    for w in range(0, 5):
        fx_sessions.append(Session(w, time(0, 0), time(22, 0)))
    registry.register(VenueCalendar("FX", fx_tz, sessions=fx_sessions))

    # Crypto 24/7: register sessions for all weekdays full day
    crypto_tz = "UTC"
    crypto_sessions = [Session(w, time(0, 0), time(23, 59, 59)) for w in range(0, 7)]
    registry.register(VenueCalendar("CRYPTO", crypto_tz, sessions=crypto_sessions))


_register_defaults()
