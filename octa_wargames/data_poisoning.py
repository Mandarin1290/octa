import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(s: str) -> datetime:
    # accepts timezone-aware ISO strings; ensure UTC
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def _canonical_serialize(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical_serialize(obj).encode("utf-8")).hexdigest()


@dataclass
class DataFeed:
    name: str
    price: float
    ts: str
    history: List[Dict[str, Any]] = field(default_factory=list)

    def snapshot(self) -> Dict[str, Any]:
        return {"name": self.name, "price": self.price, "ts": self.ts}

    def update(self, price: float, ts: Optional[str] = None) -> None:
        if ts is None:
            ts = _now_iso()
        # append to history
        self.history.append({"ts": ts, "price": price})
        self.price = price
        self.ts = ts


class DataPoisoningSimulator:
    """Inject synthetic corruption into DataFeed objects."""

    @staticmethod
    def delayed_timestamp(feed: DataFeed, delay_seconds: int) -> None:
        # set timestamp into the past
        t = _parse_iso(feed.ts) - timedelta(seconds=delay_seconds)
        feed.ts = t.isoformat()
        feed.history.append({"ts": feed.ts, "price": feed.price})

    @staticmethod
    def price_spike(feed: DataFeed, spike_pct: float) -> None:
        # introduce an immediate spike
        new_price = feed.price * (1.0 + spike_pct)
        feed.update(new_price)

    @staticmethod
    def silent_drift(feed: DataFeed, total_pct: float, steps: int = 5) -> None:
        # gradually change price over several updates
        per_step = (1.0 + total_pct) ** (1.0 / max(1, steps))
        for _i in range(steps):
            feed.update(feed.price * per_step)


class DetectionEngine:
    def __init__(
        self,
        max_age_seconds: int = 5,
        spike_threshold: float = 0.15,
        drift_threshold: float = 0.05,
        drift_window: int = 5,
    ):
        self.max_age_seconds = max_age_seconds
        self.spike_threshold = spike_threshold
        self.drift_threshold = drift_threshold
        self.drift_window = drift_window

    def _age_seconds(self, feed: DataFeed) -> float:
        try:
            feed_time = _parse_iso(feed.ts)
        except Exception:
            return float("inf")
        return (datetime.now(timezone.utc) - feed_time).total_seconds()

    def detect_delayed(self, feed: DataFeed) -> Optional[str]:
        if self._age_seconds(feed) > self.max_age_seconds:
            return "delayed_timestamp"
        return None

    def detect_spike(self, feed: DataFeed) -> Optional[str]:
        if len(feed.history) < 2:
            return None
        prev = feed.history[-2]["price"]
        if prev <= 0:
            return None
        pct = abs(feed.price - prev) / prev
        if pct >= self.spike_threshold:
            return "price_spike"
        return None

    def detect_silent_drift(self, feed: DataFeed) -> Optional[str]:
        hist = feed.history[-self.drift_window :]
        if len(hist) < 2:
            return None
        start = hist[0]["price"]
        end = hist[-1]["price"]
        if start <= 0:
            return None
        cum_pct = abs(end - start) / start
        if cum_pct >= self.drift_threshold:
            # ensure no single large spike caused it
            for i in range(1, len(hist)):
                prev = hist[i - 1]["price"]
                if prev <= 0:
                    continue
                step_pct = abs(hist[i]["price"] - prev) / prev
                if step_pct >= self.spike_threshold:
                    return None
            return "silent_drift"
        return None

    def inspect(self, feed: DataFeed) -> Tuple[bool, Optional[str]]:
        """Return (is_corrupted, reason)"""
        for detector in (
            self.detect_delayed,
            self.detect_spike,
            self.detect_silent_drift,
        ):
            r = detector(feed)
            if r:
                return True, r
        return False, None


class MarketExecutionGuard:
    """Validates data feeds before execution; selects fallback when primary is corrupted."""

    def __init__(self, detector: DetectionEngine):
        self.detector = detector

    def validate_feed(self, feed: DataFeed) -> Tuple[bool, Optional[str]]:
        return self.detector.inspect(feed)

    def select_feed(
        self, primary: DataFeed, fallbacks: List[DataFeed]
    ) -> Tuple[DataFeed, Optional[str]]:
        corrupted, reason = self.validate_feed(primary)
        if not corrupted:
            return primary, None

        # choose best fallback: non-corrupted and freshest
        candidates = []
        for f in fallbacks:
            c, r = self.validate_feed(f)
            if not c:
                # compute age
                try:
                    age = (
                        datetime.now(timezone.utc) - _parse_iso(f.ts)
                    ).total_seconds()
                except Exception:
                    age = float("inf")
                candidates.append((age, f))
        if not candidates:
            raise RuntimeError(
                f"primary feed corrupted ({reason}) and no healthy fallbacks"
            )
        # pick freshest (smallest age)
        candidates.sort(key=lambda x: x[0])
        chosen = candidates[0][1]
        return chosen, reason


__all__ = [
    "DataFeed",
    "DataPoisoningSimulator",
    "DetectionEngine",
    "MarketExecutionGuard",
]
