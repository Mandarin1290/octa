from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple


@dataclass(frozen=True)
class RiskIncident:
    timestamp_utc: str
    strategy: str
    symbol: str
    cycle: int
    simulated_case: str
    blocked: bool
    broker_router_called: bool
    fail_closed_enforced: bool
    stack_or_error: str
    incident_path: str
    sha256_path: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _simulated_case() -> str:
    return str(os.getenv("OCTA_TEST_RISK_FAIL_CLOSED_CASE", "")).strip().lower()


def _is_valid_decision(decision: Any) -> bool:
    return hasattr(decision, "allow") and isinstance(getattr(decision, "allow"), bool) and hasattr(decision, "reason")


def _write_incident(
    *,
    evidence_dir: Path,
    strategy: str,
    symbol: str,
    cycle: int,
    simulated_case: str,
    stack_or_error: str,
) -> RiskIncident:
    incidents_dir = evidence_dir / "risk_incidents"
    incidents_dir.mkdir(parents=True, exist_ok=True)
    safe_symbol = symbol or "UNKNOWN"
    name = f"risk_incident_{strategy}_{safe_symbol}_{int(cycle):03d}.json"
    path = incidents_dir / name
    payload = {
        "timestamp_utc": _utc_now_iso(),
        "strategy": strategy,
        "symbol": safe_symbol,
        "cycle": int(cycle),
        "simulated_case": simulated_case or "none",
        "blocked": True,
        "broker_router_called": False,
        "fail_closed_enforced": True,
        "stack_or_error": str(stack_or_error)[:300],
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2).encode("utf-8")
    path.write_bytes(raw)
    sha = _sha256_bytes(raw)
    sha_path = path.with_suffix(path.suffix + ".sha256")
    sha_path.write_text(sha + "\n", encoding="utf-8")
    return RiskIncident(
        timestamp_utc=payload["timestamp_utc"],
        strategy=strategy,
        symbol=safe_symbol,
        cycle=int(cycle),
        simulated_case=payload["simulated_case"],
        blocked=True,
        broker_router_called=False,
        fail_closed_enforced=True,
        stack_or_error=payload["stack_or_error"],
        incident_path=str(path),
        sha256_path=str(sha_path),
    )


def safe_decide(
    *,
    decide_fn: Callable[..., Any],
    decide_kwargs: Dict[str, Any],
    evidence_dir: Path,
    strategy: str,
    symbol: str,
    cycle: int,
) -> Tuple[bool, Optional[Any], Optional[RiskIncident]]:
    simulated_case = _simulated_case()
    try:
        if simulated_case == "exception":
            raise RuntimeError("risk_simulated_exception")
        decision = decide_fn(**decide_kwargs)
        if simulated_case == "invalid_return":
            decision = None
        if not _is_valid_decision(decision):
            incident = _write_incident(
                evidence_dir=evidence_dir,
                strategy=strategy,
                symbol=symbol,
                cycle=cycle,
                simulated_case=(simulated_case or "invalid_return"),
                stack_or_error="invalid risk decision object",
            )
            return True, None, incident
        return False, decision, None
    except Exception as exc:
        incident = _write_incident(
            evidence_dir=evidence_dir,
            strategy=strategy,
            symbol=symbol,
            cycle=cycle,
            simulated_case=(simulated_case or "exception"),
            stack_or_error=f"{type(exc).__name__}: {exc}",
        )
        return True, None, incident


def incident_to_dict(incident: RiskIncident) -> Dict[str, Any]:
    return asdict(incident)

