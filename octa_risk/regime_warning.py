import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class RegimeWarning:
    timestamp: str
    strategy: str
    vol_baseline: Optional[float]
    vol_recent: Optional[float]
    corr_baseline: Optional[float]
    corr_recent: Optional[float]
    macro_shock: Optional[float]
    warning_score: float
    severity: str
    evidence_hash: str = ""


class RegimeWarningSystem:
    """Early-warning system for regime shifts.

    Conservative design: only emit warnings when multiple inputs move materially
    in adverse directions (volatility up, correlation down, macro shock up).
    Warnings are advisory; no automated trade/capital actions.
    """

    def __init__(
        self,
        long_window: int = 120,
        short_window: int = 20,
        min_samples: int = 40,
        vol_increase_pct: float = 0.3,
        corr_drop_pct: float = 0.25,
        macro_shock_threshold: float = 0.4,
        min_warning_score: float = 0.2,
        escalate_count: int = 2,
    ):
        self.long_window = int(long_window)
        self.short_window = int(short_window)
        self.min_samples = int(min_samples)
        self.vol_increase_pct = float(vol_increase_pct)
        self.corr_drop_pct = float(corr_drop_pct)
        self.macro_shock_threshold = float(macro_shock_threshold)
        self.min_warning_score = float(min_warning_score)
        self.escalate_count = int(escalate_count)

        from typing import Any as _Any

        self._history: Dict[str, List[Dict[str, _Any]]] = {}
        self._counts: Dict[str, int] = {}
        self.alerts: List[RegimeWarning] = []
        self.audit_log: List[Dict[str, Any]] = []

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record_audit(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = {"ts": ts, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)

    def record_metrics(
        self,
        date: str,
        strategy: str,
        volatility: float,
        avg_corr: float,
        macro_shock: float = 0.0,
    ):
        r = {
            "date": date,
            "volatility": float(volatility),
            "avg_corr": float(avg_corr),
            "macro_shock": float(macro_shock),
        }
        self._history.setdefault(strategy, []).append(r)
        # cap history reasonably
        max_hist = max(1000, 10 * self.long_window)
        if len(self._history[strategy]) > max_hist:
            self._history[strategy] = self._history[strategy][-max_hist:]
        self._record_audit("record_metrics", {"strategy": strategy, **r})

    def _select(self, strategy: str):
        return self._history.get(strategy, [])

    def _mean(self, xs: List[float]) -> float:
        return statistics.mean(xs) if xs else 0.0

    def rolling_compare(self, strategy: str):
        hist = self._select(strategy)
        n = len(hist)
        if n == 0:
            return {"samples": 0}
        bw = min(self.long_window, n)
        rw = min(self.short_window, n)
        baseline = hist[-bw:]
        recent = hist[-rw:]

        vb = self._mean([h["volatility"] for h in baseline])
        vr = self._mean([h["volatility"] for h in recent])
        cb = self._mean([h["avg_corr"] for h in baseline])
        cr = self._mean([h["avg_corr"] for h in recent])
        mb = self._mean([h.get("macro_shock", 0.0) for h in baseline])
        mr = self._mean([h.get("macro_shock", 0.0) for h in recent])

        self._record_audit(
            "rolling_compare",
            {
                "strategy": strategy,
                "samples": n,
                "vb": vb,
                "vr": vr,
                "cb": cb,
                "cr": cr,
                "mb": mb,
                "mr": mr,
            },
        )
        return {
            "samples": n,
            "vol_baseline": vb,
            "vol_recent": vr,
            "corr_baseline": cb,
            "corr_recent": cr,
            "macro_baseline": mb,
            "macro_recent": mr,
        }

    def evaluate(self, strategy: str) -> Optional[RegimeWarning]:
        stats = self.rolling_compare(strategy)
        samples = stats.get("samples", 0)
        if samples < self.min_samples:
            return None

        vb = stats["vol_baseline"]
        vr = stats["vol_recent"]
        cb = stats["corr_baseline"]
        cr = stats["corr_recent"]
        mr = stats["macro_recent"]

        # normalized movements
        vol_inc = 0.0 if vb <= 0 else max(0.0, (vr - vb) / vb)
        corr_drop = 0.0 if cb == 0 else max(0.0, (cb - cr) / abs(cb))
        macro_sig = float(mr)

        # per-component scores (conservative: require sizeable moves)
        s_vol = min(1.0, vol_inc / self.vol_increase_pct)
        s_corr = min(1.0, corr_drop / self.corr_drop_pct)
        s_macro = (
            1.0
            if macro_sig >= self.macro_shock_threshold
            else (macro_sig / self.macro_shock_threshold)
        )

        # aggregate with conservative bias: square-root-weighted avg favoring lower false positives
        score = float((math.sqrt(s_vol) + math.sqrt(s_corr) + math.sqrt(s_macro)) / 3.0)

        self._record_audit(
            "evaluate",
            {
                "strategy": strategy,
                "vol_inc": vol_inc,
                "corr_drop": corr_drop,
                "macro": macro_sig,
                "score": score,
            },
        )

        if score < self.min_warning_score:
            return None

        cnt = self._counts.get(strategy, 0) + 1
        self._counts[strategy] = cnt
        severity = "advisory" if cnt < self.escalate_count else "warning"
        ts = self._now_iso()

        alert = RegimeWarning(
            timestamp=ts,
            strategy=strategy,
            vol_baseline=vb,
            vol_recent=vr,
            corr_baseline=cb,
            corr_recent=cr,
            macro_shock=mr,
            warning_score=score,
            severity=severity,
        )
        alert.evidence_hash = canonical_hash(
            {"ts": ts, "strategy": strategy, "score": score, "severity": severity}
        )
        self.alerts.append(alert)
        self._record_audit(
            "regime_warning",
            {"strategy": strategy, "score": score, "severity": severity},
        )
        return alert

    def get_alerts(self) -> List[RegimeWarning]:
        return list(self.alerts)

    def get_audit(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)
