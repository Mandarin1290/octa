**Market Hours & Calendar Engine**

Purpose
- Provide explicit, auditable venue calendars and pretrade checks. If calendar data is missing the asset is ineligible.

Venues
- NYSE / NASDAQ: America/New_York, session 09:30-16:00 Mon-Fri, avoid open/close auction windows.
- CME: futures near 24/5 with maintenance window (example: 17:00-18:00 US/Eastern).
- EUREX: Europe/Berlin 08:00-20:00 Mon-Fri.
- FX: UTC sessions to approximate 24/5 (Sunday 22:00 -> Friday 22:00).
- Crypto: 24/7 UTC.

Behavior
- All calendars are explicit. Missing venue calendar -> instrument ineligible for execution.
- Times are stored and checked in local venue time; API uses UTC timestamps.
- Do-not-trade windows (auctions, news) are explicit and prevent execution.

Extension
- Add holiday dates using `VenueCalendar.holidays` for each venue (explicit list required).
