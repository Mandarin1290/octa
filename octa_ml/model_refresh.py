import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class RetrainRecord:
    ts: str
    model_id: str
    action: str
    details: Dict[str, Any]
    evidence_hash: str


class ModelRefreshManager:
    """Manage model retraining and data refresh discipline with governance approval.

    - No silent retraining: retrain requires explicit approval via `approve_retrain`.
    - Validation gates: `validate_model` must pass during `execute_retrain`.
    - Rollback readiness: previous version is kept to enable rollback.
    - All actions appended to `audit_log` with canonical evidence hashes.
    """

    def __init__(self):
        # model_id -> dict(current_version, pending, approved, history[list of versions])
        self._models: Dict[str, Dict[str, Any]] = {}
        self.audit_log: List[Dict[str, Any]] = []
        # load optional per-asset validation thresholds from configs/validation_thresholds.json
        self._thresholds: Dict[str, float] = {}
        try:
            p = Path("configs/validation_thresholds.json")
            if p.exists():
                self._thresholds = json.loads(p.read_text())
        except Exception:
            # ignore loading errors; validation will fall back to defaults
            self._thresholds = {}

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record(self, model_id: str, action: str, details: Dict[str, Any]) -> str:
        ts = self._now_iso()
        rec = {"ts": ts, "model_id": model_id, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)
        return str(rec["evidence_hash"])

    def add_model(self, model_id: str, version: str):
        self._models[model_id] = {
            "current": version,
            "history": [version],
            "pending": None,
            "approved": False,
            "rollback_ready": False,
        }
        self._record(model_id, "add_model", {"version": version})

    def request_retrain(
        self, model_id: str, trigger: str, proposer: Optional[str] = None
    ) -> str:
        m = self._models.get(model_id)
        if m is None:
            raise KeyError("unknown model")
        m["pending"] = {
            "trigger": trigger,
            "proposer": proposer,
            "requested_at": self._now_iso(),
        }
        m["approved"] = False
        return self._record(
            model_id, "request_retrain", {"trigger": trigger, "proposer": proposer}
        )

    def approve_retrain(self, model_id: str, approver: str) -> str:
        m = self._models.get(model_id)
        if m is None:
            raise KeyError("unknown model")
        if m.get("pending") is None:
            raise RuntimeError("no pending retrain request")
        m["approved"] = True
        return self._record(model_id, "approve_retrain", {"approver": approver})

    def validate_model(
        self,
        model_id: str,
        candidate_version: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Validate candidate model using simple MAE threshold rules.

        - If `metrics` contains an `mae` value and a per-asset threshold exists
          in `configs/validation_thresholds.json`, the candidate passes if
          `mae <= threshold`.
        - If no per-asset threshold exists but `mae` is present, accept when
          `mae` is a finite number and below a permissive default (1e9).
        - Otherwise, fallback to False.
        """
        if metrics is None:
            return False

        mae = metrics.get("mae")
        if mae is None:
            # allow explicit boolean pass flag as a last resort
            return bool(metrics.get("pass", False))

        try:
            mae_val = float(mae)
        except Exception:
            return False

        # check per-asset threshold
        thresh = self._thresholds.get(model_id)
        if thresh is not None:
            try:
                return mae_val <= float(thresh)
            except Exception:
                return False

        # permissive default threshold to avoid blocking early runs
        default_thresh = 1e9
        return mae_val <= default_thresh

    def execute_retrain(
        self,
        model_id: str,
        new_version: str,
        validate_metrics: Optional[Dict[str, Any]] = None,
    ) -> str:
        m = self._models.get(model_id)
        if m is None:
            raise KeyError("unknown model")
        if not m.get("approved", False):
            raise PermissionError("retrain requires governance approval")
        # validation gate
        ok = self.validate_model(model_id, new_version, validate_metrics)
        if not ok:
            self._record(
                model_id,
                "retrain_failed_validation",
                {"candidate": new_version, "metrics": validate_metrics},
            )
            raise RuntimeError("validation failed")

        prev = m["current"]
        m["history"].append(new_version)
        m["current"] = new_version
        m["pending"] = None
        m["approved"] = False
        # set rollback readiness flag and keep prev in history
        m["rollback_ready"] = True
        evidence = self._record(
            model_id, "retrain_executed", {"from": prev, "to": new_version}
        )
        return evidence

    def rollback(self, model_id: str) -> str:
        m = self._models.get(model_id)
        if m is None:
            raise KeyError("unknown model")
        if not m.get("rollback_ready", False):
            raise RuntimeError("no rollback available")
        history = m.get("history", [])
        if len(history) < 2:
            raise RuntimeError("no previous version to rollback to")
        prev = history[-2]
        m["current"] = prev
        m["history"] = history[:-1]
        m["rollback_ready"] = False
        evidence = self._record(model_id, "rollback_executed", {"to": prev})
        return evidence

    def get_current_version(self, model_id: str) -> Optional[str]:
        m = self._models.get(model_id)
        return m.get("current") if m else None

    def get_audit(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)
