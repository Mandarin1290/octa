import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Alert:
    id: str
    ts: str
    pattern: str
    score: float
    details: Dict[str, Any]


@dataclass
class SurveillanceEngine:
    """Lightweight trade surveillance engine.

    Notes:
    - Independent from strategy logic: accepts observed order events.
    - Alerts are appended to `alert_log` and not actioned automatically.
    - Conservative defaults favour false positives over false negatives.
    """

    window_seconds: int = 30
    small_order_size: float = 10.0
    spoofing_cancel_ratio: float = 0.7
    layering_levels: int = 3
    cancellation_threshold: float = 0.6
    wash_time_seconds: int = 2

    events: List[Dict[str, Any]] = field(default_factory=list)
    alert_log: List[Alert] = field(default_factory=list)

    def ingest(self, event: Dict[str, Any]) -> None:
        """Ingest an order-book event.

        Event fields expected: `id`, `actor`, `instrument`, `side` ('buy'|'sell'), `price`, `qty`, `type` ('new'|'cancel'|'fill'), `ts` (ISO or numeric seconds)
        """
        # Normalize timestamp to datetime
        e = dict(event)
        ts = e.get("ts")
        if isinstance(ts, (int, float)):
            e_ts = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        elif isinstance(ts, str):
            try:
                # accept ISO strings with trailing Z
                e_ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if e_ts.tzinfo is None:
                    e_ts = e_ts.replace(tzinfo=timezone.utc)
            except Exception:
                e_ts = datetime.now(timezone.utc)
        else:
            e_ts = datetime.now(timezone.utc)
        e["_ts"] = e_ts
        self.events.append(e)
        # prune window
        self._prune()
        # run detectors
        self._detect_spoofing(instrument=e.get("instrument"))
        self._detect_layering(instrument=e.get("instrument"), actor=e.get("actor"))
        self._detect_abnormal_cancellations(instrument=e.get("instrument"))
        self._detect_wash_trades()

    def _prune(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
        self.events = [e for e in self.events if e["_ts"] >= cutoff]

    def _window_events(self, instrument: Optional[str] = None) -> List[Dict[str, Any]]:
        if instrument:
            return [e for e in self.events if e.get("instrument") == instrument]
        return list(self.events)

    def _emit_alert(self, pattern: str, score: float, details: Dict[str, Any]) -> None:
        alert = Alert(
            id=str(uuid.uuid4()),
            ts=_now_iso(),
            pattern=pattern,
            score=float(score),
            details=details,
        )
        self.alert_log.append(alert)

    def _detect_spoofing(self, instrument: Optional[str] = None) -> None:
        evs = self._window_events(instrument)
        if not evs:
            return
        # group by actor
        by_actor: Dict[str, List[Dict[str, Any]]] = {}
        for e in evs:
            a = e.get("actor", "unknown")
            by_actor.setdefault(a, []).append(e)

        for actor, acts in by_actor.items():
            new_orders = [o for o in acts if o.get("type") == "new"]
            cancels = [o for o in acts if o.get("type") == "cancel"]
            if not new_orders:
                continue
            small_orders = [
                o for o in new_orders if float(o.get("qty", 0)) <= self.small_order_size
            ]
            cancel_ratio = len(cancels) / max(1, len(new_orders))
            # suspicious if many small orders and high cancel ratio
            if len(small_orders) >= 3 and cancel_ratio >= self.spoofing_cancel_ratio:
                score = min(1.0, cancel_ratio + (len(small_orders) / 10.0))
                details = {
                    "actor": actor,
                    "instrument": instrument,
                    "small_orders": len(small_orders),
                    "cancel_ratio": cancel_ratio,
                }
                self._emit_alert("spoofing_like", score, details)

    def _detect_layering(
        self, instrument: Optional[str] = None, actor: Optional[str] = None
    ) -> None:
        evs = self._window_events(instrument)
        if not evs:
            return
        # look per actor for multiple price levels with new orders then cancels
        for a in set(e.get("actor", "unknown") for e in evs):
            if actor and a != actor:
                continue
            acts = [
                e
                for e in evs
                if e.get("actor") == a and e.get("type") in ("new", "cancel")
            ]
            price_levels = set(e.get("price") for e in acts if e.get("type") == "new")
            cancels = [e for e in acts if e.get("type") == "cancel"]
            if len(price_levels) >= self.layering_levels and len(cancels) >= len(
                price_levels
            ):
                score = min(1.0, len(price_levels) / 10.0 + len(cancels) / 10.0)
                details = {
                    "actor": a,
                    "instrument": instrument,
                    "levels": len(price_levels),
                    "cancels": len(cancels),
                }
                self._emit_alert("layering_like", score, details)

    def _detect_abnormal_cancellations(self, instrument: Optional[str] = None) -> None:
        evs = self._window_events(instrument)
        if not evs:
            return
        new_count = len([e for e in evs if e.get("type") == "new"])
        cancel_count = len([e for e in evs if e.get("type") == "cancel"])
        if new_count == 0:
            return
        cancel_rate = cancel_count / new_count
        if cancel_rate >= self.cancellation_threshold and cancel_count >= 5:
            details = {
                "instrument": instrument,
                "new_count": new_count,
                "cancel_count": cancel_count,
                "cancel_rate": cancel_rate,
            }
            self._emit_alert("abnormal_cancellations", float(cancel_rate), details)

    def _detect_wash_trades(self) -> None:
        # naive wash detection: fills where same actor on buy and sell within short time window
        fills = [e for e in self.events if e.get("type") == "fill"]
        if not fills:
            return
        # group by instrument and time proximity
        for inst in set(f.get("instrument") for f in fills):
            inst_fills = [f for f in fills if f.get("instrument") == inst]
            # compare pairs
            for i in range(len(inst_fills)):
                for j in range(i + 1, len(inst_fills)):
                    a = inst_fills[i]
                    b = inst_fills[j]
                    dt = abs((a["_ts"] - b["_ts"]).total_seconds())
                    if (
                        dt <= self.wash_time_seconds
                        and a.get("actor") == b.get("actor")
                        and a.get("side") != b.get("side")
                        and abs(float(a.get("qty", 0)) - float(b.get("qty", 0))) < 1e-6
                    ):
                        details = {
                            "actor": a.get("actor"),
                            "instrument": inst,
                            "t0": a["_ts"].isoformat(),
                            "t1": b["_ts"].isoformat(),
                            "qty": a.get("qty"),
                        }
                        self._emit_alert("wash_trade_indicator", 1.0, details)
