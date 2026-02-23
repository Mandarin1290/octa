from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class EligibilitySnapshot:
    eligible_symbols: list[str]
    latest_by_symbol: dict[str, dict[str, Any]]
    registry_exists: bool
    reason: str


def _read_approved_eligibility() -> EligibilitySnapshot | None:
    try:
        from octa.models.approved_loader import list_approved_models
    except Exception:
        return None

    try:
        rows = list_approved_models()
    except Exception:
        return None

    if not rows:
        return None

    latest: dict[str, dict[str, Any]] = {}
    tf_ok_by_symbol: dict[str, set[str]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper().strip()
        timeframe = str(row.get("timeframe", "")).upper().strip()
        if not symbol or not timeframe:
            continue
        has_all = bool(
            row.get("has_model", False)
            and row.get("has_sig", False)
            and row.get("has_sha256", False)
        )
        latest[symbol] = {
            "source": "approved_loader",
            "symbol": symbol,
            "timeframes_seen": sorted(
                set((latest.get(symbol, {}) or {}).get("timeframes_seen", [])) | {timeframe}
            ),
            "timeframe_ok": dict((latest.get(symbol, {}) or {}).get("timeframe_ok", {})),
        }
        latest[symbol]["timeframe_ok"][timeframe] = has_all
        if has_all:
            tf_ok_by_symbol.setdefault(symbol, set()).add(timeframe)

    eligible = sorted(
        symbol for symbol, tf_set in tf_ok_by_symbol.items() if "1D" in tf_set and "1H" in tf_set
    )

    reason = "ok" if eligible else "no_approved_eligible_symbols"
    return EligibilitySnapshot(
        eligible_symbols=eligible,
        latest_by_symbol=latest,
        registry_exists=True,
        reason=reason,
    )


def _read_compat_blessed_jsonl(registry_path: Path) -> EligibilitySnapshot:
    if not registry_path.exists():
        return EligibilitySnapshot([], {}, False, "blessed_registry_missing")

    latest: dict[str, dict[str, Any]] = {}
    for line in registry_path.read_text(encoding="utf-8").splitlines():
        row = line.strip()
        if not row:
            continue
        try:
            parsed = json.loads(row)
        except Exception:
            continue
        symbol = str(parsed.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        # append-only jsonl: last entry per symbol wins.
        latest[symbol] = parsed

    eligible: list[str] = []
    for symbol, rec in sorted(latest.items(), key=lambda x: x[0]):
        pass_1d = str(rec.get("performance_1d", "")).upper() == "PASS"
        pass_1h = str(rec.get("performance_1h", "")).upper() == "PASS"
        if pass_1d and pass_1h:
            eligible.append(symbol)

    if not latest:
        reason = "blessed_registry_empty"
    elif not eligible:
        reason = "no_blessed_eligible_symbols"
    else:
        reason = "ok"

    return EligibilitySnapshot(eligible, latest, True, reason)


def read_blessed_eligibility(registry_path: Path) -> EligibilitySnapshot:
    approved = _read_approved_eligibility()
    if approved is not None:
        return approved
    return _read_compat_blessed_jsonl(registry_path)
