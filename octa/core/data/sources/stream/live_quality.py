from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional


class LiveQualityChecker:
    """Checks live market data quality.

    - staleness: bar timestamp too old compared to now
    - gaps: large gap since last timestamp
    - out-of-order timestamps
    - timestamp kind mixing: disallow mixing naive vs tz-aware
    """

    def __init__(self, max_latency_seconds: int = 5, max_gap_seconds: int = 60):
        self.max_latency = timedelta(seconds=max_latency_seconds)
        self.max_gap = timedelta(seconds=max_gap_seconds)
        self.last_ts: Dict[str, datetime] = {}
        self.tz_kind: Optional[str] = None  # 'aware' or 'naive'

    def _timestamp_kind(self, ts: datetime) -> str:
        return (
            "aware"
            if ts.tzinfo is not None and ts.tzinfo.utcoffset(ts) is not None
            else "naive"
        )

    def validate_timestamp_kind(self, ts: datetime):
        kind = self._timestamp_kind(ts)
        if self.tz_kind is None:
            self.tz_kind = kind
            return True
        if self.tz_kind != kind:
            raise ValueError(
                f"Mixed timestamp kinds: expected {self.tz_kind}, got {kind}"
            )
        return True

    def validate_bar(
        self,
        bar,
        sentinel_api=None,
        audit_fn: Optional[Callable[[str, dict], None]] = None,
    ) -> bool:
        audit = audit_fn or (lambda e, p: None)

        # Validate timestamp kind consistency
        try:
            self.validate_timestamp_kind(bar.timestamp)
        except Exception as e:
            audit("bad_timestamp_kind", {"instrument": bar.instrument, "error": str(e)})
            if sentinel_api and hasattr(sentinel_api, "set_gate"):
                sentinel_api.set_gate(2, f"bad_timestamp_kind:{str(e)}")
            return False

        # Normalize to UTC-aware for comparison
        ts = bar.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)

        now = datetime.now(timezone.utc)
        latency = now - ts
        if latency > self.max_latency:
            audit(
                "stale_bar",
                {"instrument": bar.instrument, "latency_sec": latency.total_seconds()},
            )
            if sentinel_api and hasattr(sentinel_api, "set_gate"):
                sentinel_api.set_gate(2, f"stale:{latency.total_seconds():.1f}s")
            return False

        last = self.last_ts.get(bar.instrument)
        if last is not None:
            # normalize last
            last_norm = last
            if last_norm.tzinfo is None:
                last_norm = last_norm.replace(tzinfo=timezone.utc)
            else:
                last_norm = last_norm.astimezone(timezone.utc)

            if ts <= last_norm:
                audit(
                    "out_of_order",
                    {
                        "instrument": bar.instrument,
                        "ts": ts.isoformat(),
                        "last": last_norm.isoformat(),
                    },
                )
                if sentinel_api and hasattr(sentinel_api, "set_gate"):
                    sentinel_api.set_gate(2, "out_of_order")
                return False

            gap = ts - last_norm
            if gap > self.max_gap:
                audit(
                    "gap_detected",
                    {"instrument": bar.instrument, "gap_sec": gap.total_seconds()},
                )
                if sentinel_api and hasattr(sentinel_api, "set_gate"):
                    sentinel_api.set_gate(2, f"gap:{gap.total_seconds():.1f}s")
                return False

        # all checks passed
        self.last_ts[bar.instrument] = bar.timestamp
        return True

    def heartbeat(self) -> dict:
        return {
            instr: (ts.isoformat() if ts is not None else None)
            for instr, ts in self.last_ts.items()
        }
