from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

from octa.core.data.sources.stream.live_quality import LiveQualityChecker


@dataclass
class Bar:
    instrument: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class LiveFeed:
    """Simple live feed interface:
    - callers push `Bar` objects into `on_bar_receive`
    - validators (LiveQualityChecker) ensure feed quality
    - subscribers can register callbacks to receive validated bars
    - provides heartbeat/latency metrics
    """

    def __init__(
        self,
        quality_checker: Optional[LiveQualityChecker] = None,
        sentinel_api=None,
        audit_fn: Optional[Callable[[str, dict], None]] = None,
    ):
        self.quality = quality_checker or LiveQualityChecker()
        self.sentinel = sentinel_api
        self.audit = audit_fn or (lambda e, p: None)
        self.subscribers: List[Callable[[Bar], None]] = []

    def subscribe(self, cb: Callable[[Bar], None]):
        self.subscribers.append(cb)

    def on_bar_receive(self, bar: Bar) -> bool:
        """Validate bar and dispatch to subscribers. Returns True if accepted."""
        try:
            ok = self.quality.validate_bar(
                bar, sentinel_api=self.sentinel, audit_fn=self.audit
            )
        except Exception as e:
            self.audit(
                "live_feed_validation_error", {"error": str(e), "bar": bar.__dict__}
            )
            return False

        if not ok:
            self.audit("live_feed_reject", {"bar": bar.__dict__})
            return False

        for cb in self.subscribers:
            try:
                cb(bar)
            except Exception:
                # subscriber errors should not crash feed
                self.audit("subscriber_error", {"instrument": bar.instrument})

        self.audit("live_feed_accepted", {"bar": bar.__dict__})
        return True

    def heartbeat_metrics(self) -> dict:
        return self.quality.heartbeat()
