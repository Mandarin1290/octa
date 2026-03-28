from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class EligibleSymbol:
    symbol: str
    asset_class: str
    scaling_level: int
    source_result: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _run_allowed_for_execution(run_dir: Path) -> Tuple[bool, str]:
    manifest = run_dir / "run_manifest.json"
    if not manifest.exists():
        return True, "legacy_manifest_missing"
    try:
        raw = _load_json(manifest)
    except Exception:
        return False, "manifest_unreadable"
    regime = str(raw.get("training_regime") or raw.get("regime") or "").strip().lower()
    if regime == "foundation_validation":
        return False, "foundation_validation_regime_blocked"
    return True, "allowed"


def _safe_metrics(stage: Dict[str, Any]) -> bool:
    metrics = stage.get("metrics_summary")
    return isinstance(metrics, dict) and len(metrics) > 0


def _stage_map(stages: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for st in stages:
        tf = str(st.get("timeframe", "")).upper().strip()
        if tf:
            out[tf] = st
    return out


def _is_pass_with_metrics(stage: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not isinstance(stage, dict):
        return False, "missing_stage"
    status = str(stage.get("status", "")).upper().strip()
    if status != "PASS":
        return False, f"status_{status or 'MISSING'}"
    if not _safe_metrics(stage):
        return False, "metrics_missing_or_empty"
    return True, "pass"


def _discover_latest_run(base_evidence_dir: Path) -> Path:
    candidates = sorted(base_evidence_dir.glob("*/preflight/summary.json"))
    valid: List[Tuple[float, Path]] = []
    for marker in candidates:
        run_dir = marker.parent.parent
        results_dir = run_dir / "results"
        if not results_dir.is_dir():
            continue
        allowed, _ = _run_allowed_for_execution(run_dir)
        if not allowed:
            continue
        try:
            _ = _load_json(marker)
            valid.append((marker.stat().st_mtime, run_dir))
        except Exception:
            continue
    if not valid:
        raise RuntimeError(
            f"No readable training runs with results/ found under {base_evidence_dir}"
        )
    valid.sort(key=lambda x: x[0], reverse=True)
    return valid[0][1]


def _resolve_run_dir(base_evidence_dir: Path, run_id: Optional[str]) -> Path:
    if run_id:
        run_dir = base_evidence_dir / str(run_id).strip()
        marker = run_dir / "preflight" / "summary.json"
        if not marker.exists():
            raise RuntimeError(f"Requested run id missing summary marker: {marker}")
        allowed, reason = _run_allowed_for_execution(run_dir)
        if not allowed:
            raise RuntimeError(f"Requested run id not execution-eligible: {run_dir} ({reason})")
        _ = _load_json(marker)
        return run_dir
    return _discover_latest_run(base_evidence_dir)


def build_ml_selection(
    *,
    evidence_out_dir: Path,
    base_evidence_dir: Path = Path("octa") / "var" / "evidence",
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    selection_dir = evidence_out_dir / "selection"
    selection_dir.mkdir(parents=True, exist_ok=True)

    run_dir = _resolve_run_dir(base_evidence_dir=base_evidence_dir, run_id=run_id)
    results_dir = run_dir / "results"
    if not results_dir.exists():
        raise RuntimeError(f"Run missing results directory: {results_dir}")

    eligible: List[EligibleSymbol] = []
    ineligible: List[Dict[str, Any]] = []

    for result_file in sorted(results_dir.glob("*.json")):
        raw = _load_json(result_file)
        symbol = str(raw.get("symbol", "")).upper().strip()
        asset_class = str(raw.get("asset_class", "unknown")).strip().lower() or "unknown"
        stages = raw.get("stages") if isinstance(raw.get("stages"), list) else []
        by_tf = _stage_map(stages)

        pass_1d, reason_1d = _is_pass_with_metrics(by_tf.get("1D"))
        pass_1h, reason_1h = _is_pass_with_metrics(by_tf.get("1H"))
        if not (pass_1d and pass_1h):
            ineligible.append(
                {
                    "symbol": symbol,
                    "asset_class": asset_class,
                    "reason": "entry_gate_failed",
                    "checks": {"1D": reason_1d, "1H": reason_1h},
                    "source_result": str(result_file),
                }
            )
            continue

        pass_30m, reason_30m = _is_pass_with_metrics(by_tf.get("30M"))
        pass_5m, reason_5m = _is_pass_with_metrics(by_tf.get("5M"))
        pass_1m, reason_1m = _is_pass_with_metrics(by_tf.get("1M"))
        scaling_level = 0
        scaling_reasons: Dict[str, str] = {}
        if pass_30m:
            scaling_level = 1
            if pass_5m:
                scaling_level = 2
                if pass_1m:
                    scaling_level = 3
                else:
                    scaling_reasons["1M"] = reason_1m
            else:
                scaling_reasons["5M"] = reason_5m
                scaling_reasons["1M"] = reason_1m
        else:
            scaling_reasons["30M"] = reason_30m
            scaling_reasons["5M"] = reason_5m
            scaling_reasons["1M"] = reason_1m

        eligible.append(
            EligibleSymbol(
                symbol=symbol,
                asset_class=asset_class,
                scaling_level=scaling_level,
                source_result=str(result_file),
            )
        )
        if scaling_reasons:
            ineligible.append(
                {
                    "symbol": symbol,
                    "asset_class": asset_class,
                    "reason": "scaling_not_fully_eligible",
                    "checks": scaling_reasons,
                    "source_result": str(result_file),
                }
            )

    eligible_txt = selection_dir / "eligible_symbols.txt"
    eligible_json = selection_dir / "eligible_symbols.json"
    ineligible_json = selection_dir / "ineligible_summary.json"
    manifest_json = selection_dir / "selection_manifest.json"

    eligible_txt.write_text(
        "\n".join(sorted([x.symbol for x in eligible])) + ("\n" if eligible else ""),
        encoding="utf-8",
    )
    eligible_rows = [
        {
            "symbol": x.symbol,
            "asset_class": x.asset_class,
            "scaling_level": x.scaling_level,
            "source_result": x.source_result,
        }
        for x in sorted(eligible, key=lambda z: z.symbol)
    ]
    eligible_json.write_text(json.dumps(eligible_rows, indent=2, sort_keys=True), encoding="utf-8")

    ineligible_payload = {
        "count": len(ineligible),
        "rows": sorted(ineligible, key=lambda z: (str(z.get("symbol", "")), str(z.get("reason", "")))),
    }
    ineligible_json.write_text(json.dumps(ineligible_payload, indent=2, sort_keys=True), encoding="utf-8")

    manifest = {
        "timestamp_utc": _utc_now_iso(),
        "source_run_dir": str(run_dir),
        "source_results_dir": str(results_dir),
        "run_id": run_dir.name,
        "eligible_count": len(eligible_rows),
        "ineligible_count": len(ineligible),
        "paths": {
            "eligible_symbols_txt": str(eligible_txt),
            "eligible_symbols_json": str(eligible_json),
            "ineligible_summary_json": str(ineligible_json),
        },
    }
    manifest_json.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "selection_dir": str(selection_dir),
        "manifest": manifest,
        "eligible_rows": eligible_rows,
        "ineligible_rows": ineligible_payload["rows"],
    }
