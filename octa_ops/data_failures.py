from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class FeedState:
    name: str
    last_update: Optional[datetime] = None
    consecutive_fresh: int = 0


class DataFeedManager:
    """Manage market data feeds, freshness, fallback hierarchy and degraded trading mode.

    Rules enforced:
    - Trading on stale data is forbidden.
    - Degradation is explicit when primary feed stale but fallback available; degraded mode only allows exits.
    - Recovery requires `recovery_required` consecutive fresh reports before normal trading resumes.
    """

    def __init__(
        self,
        freshness_seconds: int = 5,
        recovery_required: int = 2,
        now_fn: Optional[Callable[[], datetime]] = None,
    ):
        self.freshness = timedelta(seconds=freshness_seconds)
        self.recovery_required = recovery_required
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))

        # feed_name -> FeedState
        self.feeds: Dict[str, FeedState] = {}
        # instrument -> ordered list of feeds (primary first)
        self.hierarchy: Dict[str, List[str]] = {}

    def register_feed(self, feed_name: str) -> None:
        self.feeds.setdefault(feed_name, FeedState(name=feed_name))

    def set_hierarchy(self, instrument: str, feeds: List[str]) -> None:
        self.hierarchy[instrument] = list(feeds)
        for f in feeds:
            self.register_feed(f)

    def report_update(
        self, feed_name: str, instrument: str, ts: Optional[datetime] = None
    ) -> None:
        if feed_name not in self.feeds:
            raise KeyError(feed_name)
        if ts is None:
            ts = self.now_fn()
        state = self.feeds[feed_name]
        state.last_update = ts
        # check freshness
        if self.is_fresh_feed(feed_name):
            state.consecutive_fresh += 1
        else:
            state.consecutive_fresh = 0

    def is_fresh_feed(self, feed_name: str) -> bool:
        fs = self.feeds.get(feed_name)
        if not fs or not fs.last_update:
            return False
        return (self.now_fn() - fs.last_update) <= self.freshness

    def best_available_feed(self, instrument: str) -> Optional[str]:
        """Return the best feed name for the instrument that is fresh, or None if none are fresh."""
        for f in self.hierarchy.get(instrument, []):
            if self.is_fresh_feed(f):
                return f
        return None

    def is_degraded(self, instrument: str) -> bool:
        """Degraded when primary is stale but a fallback is fresh."""
        feeds = self.hierarchy.get(instrument, [])
        if not feeds:
            return False
        primary = feeds[0]
        if self.is_fresh_feed(primary):
            return False
        # any fallback fresh?
        for f in feeds[1:]:
            if self.is_fresh_feed(f):
                return True
        return False

    def recovered(self, instrument: str) -> bool:
        """Return True if primary feed has met recovery requirement (consecutive fresh reports)."""
        feeds = self.hierarchy.get(instrument, [])
        if not feeds:
            return False
        primary = feeds[0]
        fs = self.feeds.get(primary)
        if not fs:
            return False
        return fs.consecutive_fresh >= self.recovery_required

    def allow_trade(self, instrument: str, trade_type: str) -> Tuple[bool, str]:
        """Decide whether trade allowed based on data freshness and degradation.

        - trade_type: 'entry' or 'exit'
        - Trading on stale data is forbidden. If degraded mode active (fallback used), only 'exit' allowed.
        - Returns (allowed, reason).
        """
        best = self.best_available_feed(instrument)
        if best is None:
            return False, "no_fresh_feed"

        # if primary is fresh, allow
        feeds = self.hierarchy.get(instrument, [])
        if feeds and best == feeds[0]:
            return True, "primary_fresh"

        # fallback being used -> degraded
        if self.is_degraded(instrument):
            if trade_type == "exit":
                return True, "degraded_exit_allowed"
            return False, "degraded_blocks_entries"

        return False, "unknown_state"


__all__ = ["DataFeedManager"]
