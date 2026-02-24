"""Lifecycle state machine controller for the append-only JSONL model registry (I6).

Reads ``registry.jsonl``, evaluates lifecycle transitions deterministically, and
appends ``promotion_event`` entries (never overwrites existing lines).

Transition rules
----------------
T1  CANDIDATE → SHADOW            gates PASS + blessed_1d_1h (if required by policy)
T2  SHADOW    → PAPER             shadow evidence threshold (fail-closed: HOLD by default)
T3  PAPER     → LIVE              promotion_candidates PASS + min_trading_days + arm token
    PAPER     → PAPER_READY_FOR_LIVE  all T3 conditions except arm token
T4  Any       → REJECTED          gate FAIL or critical incident
*   Any       → stays current     missing evidence / missing data (HOLD semantics)

Registry writes are RESEARCH-context-only; ``assert_write_allowed`` blocks
production contexts silently.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .immutability_guard import assert_write_allowed

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BLESSED_STATUSES = frozenset({"SHADOW", "PAPER", "LIVE", "PAPER_READY_FOR_LIVE"})


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# LifecycleDecision
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LifecycleDecision:
    """Immutable record of a single lifecycle transition decision."""

    model_id: str
    symbol: str
    timeframe: str
    from_status: str
    to_status: str
    allowed: bool
    reason: str
    inputs: Dict[str, Any] = field(default_factory=dict)
    as_of: str = field(default_factory=_now_utc)

    def to_promotion_event(self) -> Dict[str, Any]:
        """Build the JSONL promotion_event dict for this decision."""
        body: Dict[str, Any] = {
            "schema_version": 1,
            "entry_type": "promotion_event",
            "model_id": self.model_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "from_status": self.from_status,
            "to_status": self.to_status,
            "allowed": self.allowed,
            "reason": self.reason,
            "inputs": self.inputs,
            "as_of": self.as_of,
            "created_at": self.as_of,
        }
        payload_str = json.dumps(
            body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
        )
        body["entry_sha256"] = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
        return body


# ---------------------------------------------------------------------------
# LifecycleControllerConfig
# ---------------------------------------------------------------------------


@dataclass
class LifecycleControllerConfig:
    """Policy parameters for the lifecycle controller."""

    registry_path: Path = field(
        default_factory=lambda: Path("octa") / "var" / "registry" / "models" / "registry.jsonl"
    )
    candidates_path: Optional[Path] = None  # promotion_candidates.json; None → HOLD
    token_path: Path = field(
        default_factory=lambda: Path("octa") / "var" / "state" / "live_armed.json"
    )
    ttl_seconds: int = 900
    require_blessed_1d_1h: bool = True
    min_trading_days: int = 20
    max_drawdown_threshold: float = 0.05


# ---------------------------------------------------------------------------
# Pure data-loading helpers
# ---------------------------------------------------------------------------


def load_registry_entries(path: Path) -> List[Dict[str, Any]]:
    """Load all non-empty JSON objects from a JSONL file.  Skips malformed lines."""
    result: List[Dict[str, Any]] = []
    if not path.exists():
        return result
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            if isinstance(obj, dict):
                result.append(obj)
    return result


def latest_by_model_id(entries: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return the most recent registry entry per model_id (by created_at string sort)."""
    result: Dict[str, Dict[str, Any]] = {}
    result_ts: Dict[str, str] = {}
    for e in entries:
        mid = str(e.get("model_id", ""))
        if not mid:
            continue
        ts = str(e.get("created_at", ""))
        if mid not in result or ts > result_ts[mid]:
            result[mid] = e
            result_ts[mid] = ts
    return result


def _resolve_current_statuses(entries: List[Dict[str, Any]]) -> Dict[str, str]:
    """Return {"{sym}|{tf}": current_status} considering promotion_event overrides.

    Iterates entries sorted by created_at ascending; allowed promotion_events
    override the status forward.
    """
    sorted_entries = sorted(entries, key=lambda e: str(e.get("created_at", "")))
    status_map: Dict[str, str] = {}
    for e in sorted_entries:
        sym = str(e.get("symbol", ""))
        tf = str(e.get("timeframe", ""))
        if not sym or not tf:
            continue
        key = f"{sym}|{tf}"
        if e.get("entry_type") == "promotion_event":
            if e.get("allowed"):
                status_map[key] = str(e.get("to_status", "HOLD"))
            # Non-allowed events do not change status.
        else:
            promo = e.get("promotion") or {}
            status_map[key] = str(promo.get("status", "CANDIDATE"))
    return status_map


def _build_blessed_map(entries: List[Dict[str, Any]]) -> Dict[str, bool]:
    """Return {"{sym}|{tf}": True} if any model entry has structural + performance PASS gates."""
    blessed: Dict[str, bool] = {}
    for e in entries:
        if e.get("entry_type") == "promotion_event":
            continue
        sym = str(e.get("symbol", ""))
        tf = str(e.get("timeframe", ""))
        if not sym or not tf:
            continue
        gates = e.get("gates") or {}
        if (
            str(gates.get("structural", "")).upper() == "PASS"
            and str(gates.get("performance", "")).upper() == "PASS"
        ):
            blessed[f"{sym}|{tf}"] = True
    return blessed


def load_promotion_candidates(path: Path) -> Dict[str, Dict[str, Any]]:
    """Load promotion_candidates.json → {"{sym}|{tf}": candidate_dict}.

    Returns empty dict (fail-closed) if file missing or unreadable.
    """
    if path is None or not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for c in raw.get("candidates") or []:
        sym = str(c.get("symbol", ""))
        tf = str(c.get("timeframe", ""))
        if sym and tf:
            result[f"{sym}|{tf}"] = c
    return result


def load_live_arm_token(token_path: Path, ttl_seconds: int) -> Tuple[bool, Dict[str, Any]]:
    """Return ``(armed, details)``.  ``armed=True`` only if token exists and is not expired."""
    if not token_path.exists():
        return False, {"reason": "token_file_missing", "path": str(token_path)}
    try:
        raw = json.loads(token_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, {"reason": "token_parse_error", "error": str(exc)}

    expires_at_str = str(raw.get("expires_at", "") or "")
    if not expires_at_str:
        armed_at_str = str(raw.get("armed_at", "") or "")
        if not armed_at_str:
            return False, {"reason": "token_missing_expiry_and_armed_at"}
        try:
            armed_at = datetime.fromisoformat(armed_at_str.replace("Z", "+00:00"))
            if armed_at.tzinfo is None:
                armed_at = armed_at.replace(tzinfo=timezone.utc)
            expires_at = armed_at + timedelta(seconds=int(ttl_seconds))
        except Exception as exc:
            return False, {"reason": "token_parse_armed_at_error", "error": str(exc)}
    else:
        try:
            expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        except Exception as exc:
            return False, {"reason": "token_parse_expires_at_error", "error": str(exc)}

    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    if now > expires_at:
        return False, {
            "reason": "token_expired",
            "expires_at": str(expires_at),
            "now": str(now),
        }
    return True, {
        "armed": True,
        "expires_at": str(expires_at),
        "now": str(now),
        "armed_by": str(raw.get("armed_by", "") or ""),
    }


# ---------------------------------------------------------------------------
# Core decision function
# ---------------------------------------------------------------------------


def decide_next_status(
    entry: Dict[str, Any],
    current_status: str,
    policy: LifecycleControllerConfig,
    candidates: Dict[str, Dict[str, Any]],
    arm_token: Tuple[bool, Dict[str, Any]],
    blessed_map: Dict[str, bool],
) -> LifecycleDecision:
    """Deterministically compute the next lifecycle status for one model entry.

    Never raises.  Same inputs → same output (except the ``as_of`` timestamp).
    """
    sym = str(entry.get("symbol", ""))
    tf = str(entry.get("timeframe", ""))
    model_id = str(entry.get("model_id", ""))
    key = f"{sym}|{tf}"
    as_of = _now_utc()

    def _dec(
        to: str, allowed: bool, reason: str, inp: Optional[Dict[str, Any]] = None
    ) -> LifecycleDecision:
        return LifecycleDecision(
            model_id=model_id,
            symbol=sym,
            timeframe=tf,
            from_status=current_status,
            to_status=to,
            allowed=allowed,
            reason=reason,
            inputs=dict(inp or {}),
            as_of=as_of,
        )

    # ------------------------------------------------------------------
    # T1: CANDIDATE → SHADOW (or HOLD / REJECTED)
    # ------------------------------------------------------------------
    if current_status == "CANDIDATE":
        gates = entry.get("gates") or {}
        structural = str(gates.get("structural", "HOLD")).upper()
        risk = str(gates.get("risk", "HOLD")).upper()
        performance = str(gates.get("performance", "HOLD")).upper()

        if structural == "FAIL" or performance == "FAIL" or risk == "FAIL":
            return _dec(
                "REJECTED", False, "gate_fail",
                {"structural": structural, "risk": risk, "performance": performance},
            )
        if structural != "PASS" or performance != "PASS":
            return _dec(
                current_status, False, "gates_not_pass",
                {"structural": structural, "performance": performance},
            )

        # require_blessed_1d_1h: both 1D and 1H for symbol must have PASS gates
        if policy.require_blessed_1d_1h:
            has_1d = bool(blessed_map.get(f"{sym}|1D", False))
            has_1h = bool(blessed_map.get(f"{sym}|1H", False))
            if not has_1d or not has_1h:
                return _dec(
                    current_status, False, "blessed_1d_1h_missing",
                    {"has_1d": has_1d, "has_1h": has_1h, "sym": sym},
                )

        return _dec(
            "SHADOW", True, "gates_pass",
            {"structural": structural, "performance": performance},
        )

    # ------------------------------------------------------------------
    # T2: SHADOW → PAPER
    # Fail-closed default: shadow evidence must be verified explicitly.
    # Without explicit evidence the entry stays in SHADOW.
    # ------------------------------------------------------------------
    if current_status == "SHADOW":
        gates = entry.get("gates") or {}
        if str(gates.get("drift", "HOLD")).upper() == "FAIL":
            return _dec("REJECTED", False, "drift_fail", {"drift": gates.get("drift")})
        return _dec(
            current_status, False, "shadow_evidence_required",
            {"note": "Remain in SHADOW until shadow evidence threshold is verified."},
        )

    # ------------------------------------------------------------------
    # T3: PAPER → LIVE
    # ------------------------------------------------------------------
    if current_status == "PAPER":
        if not candidates:
            return _dec(current_status, False, "promotion_candidates_missing", {"key": key})

        candidate = candidates.get(key)
        if candidate is None:
            return _dec(current_status, False, "no_candidate_for_key", {"key": key})

        cand_status = str(candidate.get("status", "HOLD")).upper()
        if cand_status == "FAIL":
            return _dec("REJECTED", False, "paper_eval_fail", {"candidate_status": cand_status})
        if cand_status != "PASS":
            return _dec(
                current_status, False, "paper_eval_not_pass",
                {"candidate_status": cand_status},
            )

        # min_trading_days — fail-closed: missing field → 0 → HOLD
        trading_days = int(candidate.get("trading_days") or 0)
        if trading_days < policy.min_trading_days:
            return _dec(
                current_status, False, "min_trading_days_not_met",
                {"trading_days": trading_days, "min": policy.min_trading_days},
            )

        # max_drawdown — fail-closed: missing field → 0.0 (passes threshold)
        drawdown = float(candidate.get("drawdown") or 0.0)
        if drawdown > policy.max_drawdown_threshold:
            return _dec(
                "REJECTED", False, "drawdown_exceeded",
                {"drawdown": drawdown, "max": policy.max_drawdown_threshold},
            )

        # Live arm token required
        armed, arm_details = arm_token
        if not armed:
            return _dec(
                "PAPER_READY_FOR_LIVE", False, "live_arm_required",
                {"arm_details": arm_details},
            )

        return _dec(
            "LIVE", True, "live_ready",
            {
                "armed_by": arm_details.get("armed_by", ""),
                "expires_at": arm_details.get("expires_at", ""),
            },
        )

    # No actionable transition for this status
    return _dec(current_status, False, "no_transition", {"current_status": current_status})


# ---------------------------------------------------------------------------
# LifecycleController
# ---------------------------------------------------------------------------


class LifecycleController:
    """Orchestrates lifecycle transitions for all model entries in registry.jsonl.

    Writes are RESEARCH-context-only and guarded by ``assert_write_allowed``.
    """

    def __init__(self, config: LifecycleControllerConfig) -> None:
        self._cfg = config

    def run(
        self,
        ctx: Mapping[str, Any],
        evidence_dir: Optional[Path] = None,
    ) -> List[LifecycleDecision]:
        """Evaluate lifecycle transitions for all latest model entries.

        Appends ``promotion_event`` JSONL entries for allowed status changes,
        subject to the immutability guard (research context only).
        """
        cfg = self._cfg
        entries = load_registry_entries(cfg.registry_path)
        current_statuses = _resolve_current_statuses(entries)
        blessed_map = _build_blessed_map(entries)
        candidates = (
            load_promotion_candidates(cfg.candidates_path)
            if cfg.candidates_path is not None
            else {}
        )
        arm_token = load_live_arm_token(cfg.token_path, cfg.ttl_seconds)

        # Evaluate model entries only (skip promotion_events in the decision loop)
        model_entries = [e for e in entries if e.get("entry_type") != "promotion_event"]
        latest_map = latest_by_model_id(model_entries)

        stage = str(ctx.get("stage", "")).strip().lower()
        decisions: List[LifecycleDecision] = []

        for _mid, entry in sorted(latest_map.items()):
            sym = str(entry.get("symbol", ""))
            tf = str(entry.get("timeframe", ""))
            if not sym or not tf:
                continue
            current_status = current_statuses.get(f"{sym}|{tf}", "CANDIDATE")
            decision = decide_next_status(
                entry, current_status, cfg, candidates, arm_token, blessed_map
            )
            decisions.append(decision)

            if decision.allowed and decision.to_status != decision.from_status:
                if stage == "research":
                    self._append_promotion_event(ctx, decision, evidence_dir)

        return decisions

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _append_promotion_event(
        self,
        ctx: Mapping[str, Any],
        decision: LifecycleDecision,
        evidence_dir: Optional[Path],
    ) -> None:
        """Append one promotion_event to registry.jsonl.

        Silently skipped if the immutability guard raises (production context).
        """
        try:
            assert_write_allowed(
                ctx,
                operation="lifecycle_promotion_event",
                target=str(self._cfg.registry_path),
                details={
                    "model_id": decision.model_id,
                    "from_status": decision.from_status,
                    "to_status": decision.to_status,
                },
            )
        except RuntimeError:
            return  # Production immutability: skip silently.

        event = decision.to_promotion_event()
        line = json.dumps(
            event, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
        )
        path = self._cfg.registry_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())

        if evidence_dir is not None:
            ev_dir = Path(evidence_dir)
            ev_dir.mkdir(parents=True, exist_ok=True)
            safe_mid = str(decision.model_id).replace("/", "_")
            ev_path = ev_dir / f"promotion_decision_{safe_mid}.json"
            ev_path.write_text(
                json.dumps(event, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
