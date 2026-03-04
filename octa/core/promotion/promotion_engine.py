"""PromotionEvaluator — paper → live eligibility decision engine.

Responsibilities
----------------
1. load_paper_run()      — read metrics from a run directory (decisions.csv or SQLite)
2. load_registry_state() — read lifecycle status from SQLite + JSONL; fail-closed on mismatch
3. evaluate_promotion()  — full pipeline: load → evaluate criteria → write immutable decision
4. write_decision()      — persist JSON + sha256 sidecar to evidence directory

Fail-closed contract
--------------------
- Registry mismatch (SQLite vs JSONL disagree)  → reason REGISTRY_MISMATCH
- Missing metrics                                → reason MISSING_METRIC:<key>
- Open drift breach                              → reason DRIFT_BREACH:<key>
- Risk incidents > 0 (default)                  → reason RISK_INCIDENTS_PRESENT:<n>
- No live-enable switch is set here.  Output is only: ``eligible_for_live: true/false``.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .promotion_criteria import PromotionCriteria, evaluate


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(obj: Any) -> bytes:
    """Stable JSON bytes (sorted keys, no trailing whitespace)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()


def _sha256_of(obj: Any) -> str:
    return hashlib.sha256(_canonical_json(obj)).hexdigest()


# ---------------------------------------------------------------------------
# Paper run loading
# ---------------------------------------------------------------------------


def load_paper_run(
    run_dir: Path,
    *,
    sqlite_path: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Load per-symbol, per-timeframe metrics from a paper/training run.

    Strategy (in priority order):
    1. ``decisions.csv`` in *run_dir* (most common; written by autopilot cascade)
    2. SQLite ``metrics`` table for *run_id* (if *sqlite_path* given)

    Returns
    -------
    ``{symbol: {timeframe: {metric_key: value, ...}}}``

    Raises
    ------
    ValueError
        If no data source is available or the CSV is malformed.
    """
    run_dir = Path(run_dir)
    csv_path = run_dir / "decisions.csv"

    if csv_path.exists():
        return _load_from_decisions_csv(csv_path)

    if sqlite_path is not None and run_id is not None:
        return _load_from_sqlite(Path(sqlite_path), run_id)

    raise ValueError(
        f"No metrics source found for run_dir={run_dir!s}; "
        "expected decisions.csv or (sqlite_path + run_id)"
    )


def _load_from_decisions_csv(
    csv_path: Path,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Parse decisions.csv → {symbol: {tf: metrics_dict}}."""
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            tf = str(row.get("timeframe", "")).strip()
            sym = str(row.get("candidate_id", "")).strip()
            if not tf or not sym:
                continue
            metrics_raw: Dict[str, Any] = {}
            # Parse embedded metrics_json if present
            mj = row.get("metrics_json", "")
            if mj:
                try:
                    metrics_raw = json.loads(mj)
                except (json.JSONDecodeError, ValueError):
                    pass
            # Top-level CSV columns override (they are pre-extracted floats)
            for col in ("sharpe", "max_drawdown", "profit_factor", "calmar"):
                val = row.get(col, "")
                if val not in ("", None):
                    try:
                        metrics_raw[col] = float(val)
                    except ValueError:
                        pass
            # n_trades may be embedded in metrics_json as "n_trades"
            result.setdefault(sym, {})[tf] = metrics_raw
    return result


def _load_from_sqlite(
    sqlite_path: Path, run_id: str
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Load metrics from SQLite metrics table."""
    conn = sqlite3.connect(str(sqlite_path))
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT symbol, timeframe, metrics_json FROM metrics WHERE run_id = ?",
            (run_id,),
        )
        result: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for sym, tf, mj in cur.fetchall():
            metrics_raw: Dict[str, Any] = {}
            if mj:
                try:
                    metrics_raw = json.loads(mj)
                except (json.JSONDecodeError, ValueError):
                    pass
            result.setdefault(sym, {})[tf] = metrics_raw
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registry state loading + mismatch detection
# ---------------------------------------------------------------------------


def load_registry_state(
    symbol: str,
    timeframe: str,
    *,
    sqlite_path: Optional[Path] = None,
    jsonl_path: Optional[Path] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Read lifecycle status from SQLite and/or JSONL registry.

    Returns
    -------
    (status, details)
        status:  lifecycle status string (CANDIDATE if no record found)
        details: diagnostic dict

    Raises
    ------
    ValueError with reason REGISTRY_MISMATCH
        If both SQLite and JSONL exist and report different lifecycle statuses.
    """
    sqlite_status: Optional[str] = None
    jsonl_status: Optional[str] = None

    if sqlite_path is not None and Path(sqlite_path).exists():
        sqlite_status = _read_sqlite_lifecycle(Path(sqlite_path), symbol, timeframe)

    if jsonl_path is not None and Path(jsonl_path).exists():
        jsonl_status = _read_jsonl_lifecycle(Path(jsonl_path), symbol, timeframe)

    details: Dict[str, Any] = {
        "sqlite_status": sqlite_status,
        "jsonl_status": jsonl_status,
    }

    # Fail-closed: mismatch if both have a value and they disagree
    if (
        sqlite_status is not None
        and jsonl_status is not None
        and sqlite_status != jsonl_status
    ):
        raise ValueError(
            f"REGISTRY_MISMATCH:{symbol}:{timeframe}:"
            f"sqlite={sqlite_status!r} jsonl={jsonl_status!r}"
        )

    # Source of truth: SQLite > JSONL > default CANDIDATE
    status = sqlite_status or jsonl_status or "CANDIDATE"
    details["resolved_status"] = status
    return status, details


def _read_sqlite_lifecycle(
    sqlite_path: Path, symbol: str, timeframe: str
) -> Optional[str]:
    conn = sqlite3.connect(str(sqlite_path))
    try:
        cur = conn.cursor()
        # Check column exists (lifecycle_status added in Phase 1 I1)
        cur.execute("PRAGMA table_info(artifacts)")
        cols = {row[1] for row in cur.fetchall()}
        if "lifecycle_status" not in cols:
            return None
        cur.execute(
            "SELECT lifecycle_status FROM artifacts WHERE symbol=? AND timeframe=? "
            "ORDER BY id DESC LIMIT 1",
            (symbol, timeframe),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None
    finally:
        conn.close()


def _read_jsonl_lifecycle(
    jsonl_path: Path, symbol: str, timeframe: str
) -> Optional[str]:
    """Read the latest promotion status for (symbol, timeframe) from JSONL registry."""
    status: Optional[str] = None
    with jsonl_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if str(entry.get("symbol", "")) == symbol and str(
                entry.get("timeframe", "")
            ) == timeframe:
                # Both model registry entries and promotion_event entries
                promo = entry.get("promotion") or {}
                if isinstance(promo, dict) and "status" in promo:
                    status = str(promo["status"])
                # promotion_event entries written by LifecycleController
                if entry.get("event_type") == "promotion_event":
                    to_s = entry.get("to_status")
                    if to_s:
                        status = str(to_s)
    return status


# ---------------------------------------------------------------------------
# Drift breach helper
# ---------------------------------------------------------------------------


def load_drift_state(
    symbol: str,
    timeframe: str,
    *,
    drift_registry_dir: Optional[Path] = None,
) -> bool:
    """Return True if drift breach is active for symbol+timeframe.

    Semantics (matching drift_monitor.py):
      registry file missing   → breach active (fail-closed) → True
      disabled=False          → breach active               → True
      disabled=True           → admin-suppressed            → False
    """
    if drift_registry_dir is None:
        return False  # no drift registry configured → skip check
    model_key = f"{symbol}_{timeframe}"
    reg_file = Path(drift_registry_dir) / f"{model_key}.json"
    if not reg_file.exists():
        # No file → no drift record → not breached (model never evaluated)
        return False
    try:
        state = json.loads(reg_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return True  # unreadable → fail-closed
    # disabled=True means admin-suppressed → clear; False/missing → breach
    return state.get("disabled") is not True


# ---------------------------------------------------------------------------
# Decision writing
# ---------------------------------------------------------------------------


def write_decision(
    evidence_dir: Path,
    decision: Dict[str, Any],
) -> Tuple[Path, str]:
    """Write promotion_decision.json + .sha256 to evidence_dir.

    Parameters
    ----------
    evidence_dir:
        Directory that must exist before calling.
    decision:
        Decision dict.  A ``decision_id`` (UUID4) and ``decided_at`` (UTC ISO)
        are injected if not already present.

    Returns
    -------
    (json_path, sha256_hex)
    """
    evidence_dir = Path(evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    payload = dict(decision)
    payload.setdefault("decision_id", str(uuid.uuid4()))
    payload.setdefault("decided_at", _now_utc())
    payload.setdefault("schema_version", 1)

    # Compute sha256 BEFORE adding it to the payload (canonical, no self-ref)
    digest = _sha256_of(payload)
    payload["decision_sha256"] = digest

    json_path = evidence_dir / "promotion_decision.json"
    sha_path = evidence_dir / "promotion_decision.sha256"

    json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    sha_path.write_text(digest + "\n", encoding="utf-8")
    return json_path, digest


# ---------------------------------------------------------------------------
# Full evaluation pipeline
# ---------------------------------------------------------------------------


@dataclass
class PromotionResult:
    eligible_for_live: bool
    symbol: str
    reasons: List[str]
    details: Dict[str, Any]
    decision_sha256: str = ""
    decision_path: str = ""


def evaluate_promotion(
    symbol: str,
    *,
    run_dir: Optional[Path] = None,
    metrics_by_tf: Optional[Mapping[str, Mapping[str, Any]]] = None,
    criteria: Optional[PromotionCriteria] = None,
    sqlite_path: Optional[Path] = None,
    jsonl_path: Optional[Path] = None,
    drift_registry_dir: Optional[Path] = None,
    risk_incident_count: int = 0,
    evidence_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> PromotionResult:
    """Full paper → live eligibility evaluation for a single symbol.

    Metrics can be supplied either directly via *metrics_by_tf* or loaded
    from *run_dir* / *sqlite_path* + *run_id*.

    No live-enable switch is set.  The result is purely: eligible_for_live bool.
    """
    if criteria is None:
        criteria = PromotionCriteria()

    # --- 1. Load metrics ---
    if metrics_by_tf is None:
        if run_dir is None:
            raise ValueError("Either metrics_by_tf or run_dir must be provided")
        all_metrics = load_paper_run(
            run_dir, sqlite_path=sqlite_path, run_id=run_id
        )
        sym_metrics: Dict[str, Dict[str, Any]] = all_metrics.get(symbol, {})
    else:
        sym_metrics = {tf: dict(m) for tf, m in metrics_by_tf.items()}

    # --- 2. Registry alignment check ---
    registry_details: Dict[str, Any] = {}
    registry_mismatch_reasons: List[str] = []
    for tf in sym_metrics:
        try:
            status, rd = load_registry_state(
                symbol,
                tf,
                sqlite_path=sqlite_path,
                jsonl_path=jsonl_path,
            )
            registry_details[tf] = rd
        except ValueError as exc:
            # REGISTRY_MISMATCH
            registry_mismatch_reasons.append(str(exc))

    if registry_mismatch_reasons:
        decision: Dict[str, Any] = {
            "symbol": symbol,
            "eligible_for_live": False,
            "reasons": registry_mismatch_reasons,
            "registry_details": registry_details,
            "criteria": {
                "min_sharpe": criteria.min_sharpe,
                "max_drawdown": criteria.max_drawdown,
                "min_trades": criteria.min_trades,
                "min_survivors": criteria.min_survivors,
            },
        }
        sha = ""
        dpath = ""
        if evidence_dir is not None:
            _, sha = write_decision(Path(evidence_dir), decision)
            dpath = str(Path(evidence_dir) / "promotion_decision.json")
        return PromotionResult(
            eligible_for_live=False,
            symbol=symbol,
            reasons=registry_mismatch_reasons,
            details=decision,
            decision_sha256=sha,
            decision_path=dpath,
        )

    # --- 3. Drift state ---
    drift_breaches: Dict[str, bool] = {}
    if criteria.drift_guard_required and drift_registry_dir is not None:
        for tf in sym_metrics:
            model_key = f"{symbol}_{tf}"
            breached = load_drift_state(symbol, tf, drift_registry_dir=drift_registry_dir)
            if breached:
                drift_breaches[model_key] = True

    # --- 4. Evaluate criteria ---
    eligible, reasons, eval_details = evaluate(
        sym_metrics,
        criteria=criteria,
        drift_breaches=drift_breaches,
        risk_incident_count=risk_incident_count,
    )

    decision = {
        "symbol": symbol,
        "eligible_for_live": eligible,
        "reasons": reasons,
        "evaluation": eval_details,
        "registry_details": registry_details,
    }

    sha = ""
    dpath = ""
    if evidence_dir is not None:
        _, sha = write_decision(Path(evidence_dir), decision)
        dpath = str(Path(evidence_dir) / "promotion_decision.json")
        decision["decision_sha256"] = sha

    return PromotionResult(
        eligible_for_live=eligible,
        symbol=symbol,
        reasons=reasons,
        details=decision,
        decision_sha256=sha,
        decision_path=dpath,
    )
