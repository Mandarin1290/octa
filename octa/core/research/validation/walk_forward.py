from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class WFOResult:
    stability: float
    degrade: float


def run_wfo(metrics: Mapping[str, float]) -> WFOResult:
    base = float(metrics.get("sharpe", 0.5))
    degrade = max(0.0, 0.5 - base)
    stability = max(0.0, min(1.0, base))
    return WFOResult(stability=stability, degrade=degrade)


from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

import json
import math

import pandas as pd


@dataclass(frozen=True)
class Split:
    train_idx: List[int]
    test_idx: List[int]
    meta: Dict[str, Any]


@dataclass(frozen=True)
class ValidationReport:
    run_id: str
    gate: str
    timeframe: str
    splits: List[Dict[str, Any]]
    metrics_by_split: List[Dict[str, Any]]
    aggregate_metrics: Dict[str, Any]
    ok: bool
    errors: List[str]


def make_walk_forward_splits(index: pd.Index, cfg: Mapping[str, Any]) -> List[Split]:
    timeframe = str(cfg.get("timeframe", "1D"))
    train_days = int(cfg.get("train_days", 252))
    test_days = int(cfg.get("test_days", 63))
    step_days = int(cfg.get("step_days", test_days))
    warmup_days = int(cfg.get("warmup_days", 0))
    embargo_bars = int(cfg.get("embargo_bars", 0))

    bars_per_day = _bars_per_day(timeframe)
    train_bars = max(1, train_days * bars_per_day)
    test_bars = max(1, test_days * bars_per_day)
    step_bars = max(1, step_days * bars_per_day)
    warmup_bars = max(0, warmup_days * bars_per_day)

    n = len(index)
    splits: List[Split] = []
    if n <= train_bars + test_bars:
        return splits

    start = train_bars + warmup_bars
    for test_start in range(start, n - test_bars + 1, step_bars):
        test_end = test_start + test_bars - 1
        train_start = 0
        train_end = test_start - 1
        train_idx = list(range(train_start, train_end + 1))
        test_idx = list(range(test_start, test_end + 1))
        if embargo_bars > 0:
            embargo_from = test_end + 1
            embargo_to = min(n - 1, test_end + embargo_bars)
            train_idx = [i for i in train_idx if not (embargo_from <= i <= embargo_to)]
        if not train_idx or not test_idx:
            continue
        splits.append(
            Split(
                train_idx=train_idx,
                test_idx=test_idx,
                meta={
                    "train_start": train_start,
                    "train_end": train_end,
                    "test_start": test_start,
                    "test_end": test_end,
                    "train_size": len(train_idx),
                    "test_size": len(test_idx),
                },
            )
        )
    return splits


def validate_model(
    model_train_fn: Callable[[Iterable[int], Mapping[str, Any]], Any],
    model_eval_fn: Callable[[Any, Iterable[int], Mapping[str, Any]], Dict[str, Any]],
    splits: List[Split],
    seed: int,
    ctx: Mapping[str, Any],
) -> ValidationReport:
    run_id = str(ctx.get("run_id", "validation"))
    gate = str(ctx.get("gate", "unknown"))
    timeframe = str(ctx.get("timeframe", "1D"))
    errors: List[str] = []
    metrics_by_split: List[Dict[str, Any]] = []
    split_rows: List[Dict[str, Any]] = []

    for i, split in enumerate(splits):
        try:
            model = model_train_fn(split.train_idx, ctx)
            metrics = model_eval_fn(model, split.test_idx, ctx)
            metrics_by_split.append({"fold": i, **(metrics or {})})
            split_rows.append({"fold": i, **split.meta})
        except Exception as exc:
            errors.append(f"fold_{i}:{exc}")

    aggregate = _aggregate_metrics(metrics_by_split)
    ok = bool(metrics_by_split) and not errors

    report = ValidationReport(
        run_id=run_id,
        gate=gate,
        timeframe=timeframe,
        splits=split_rows,
        metrics_by_split=metrics_by_split,
        aggregate_metrics=aggregate,
        ok=ok,
        errors=errors,
    )
    _write_validation_artifact(report)
    _write_validation_audit(report, ctx, seed)
    return report


def _aggregate_metrics(metrics_by_split: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not metrics_by_split:
        return {"count": 0}
    keys = set()
    for row in metrics_by_split:
        keys.update(row.keys())
    keys.discard("fold")
    summary: Dict[str, Any] = {"count": len(metrics_by_split)}
    for key in keys:
        vals = []
        for row in metrics_by_split:
            try:
                vals.append(float(row.get(key)))
            except Exception:
                continue
        if not vals:
            continue
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = math.sqrt(var)
        cv = std / mean if mean != 0 else None
        summary[f"{key}_mean"] = mean
        summary[f"{key}_std"] = std
        summary[f"{key}_cv"] = cv
    return summary


def _bars_per_day(timeframe: str) -> int:
    tf = str(timeframe).upper()
    if tf == "1D":
        return 1
    if tf == "1H":
        return 24
    if tf == "30M":
        return 48
    if tf == "5M":
        return 288
    if tf == "1M":
        return 1440
    return 1


def _write_validation_artifact(report: ValidationReport) -> None:
    root = Path("octa") / "var" / "artifacts" / "validation" / report.run_id / report.gate / report.timeframe
    root.mkdir(parents=True, exist_ok=True)
    path = root / "validation.json"
    path.write_text(json.dumps(report.__dict__, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _write_validation_audit(report: ValidationReport, ctx: Mapping[str, Any], seed: int) -> None:
    root = Path("octa") / "var" / "audit" / "validation"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "run_id": report.run_id,
        "gate": report.gate,
        "timeframe": report.timeframe,
        "seed": seed,
        "context": dict(ctx),
        "split_count": len(report.splits),
        "metrics_summary": report.aggregate_metrics,
        "errors": report.errors,
        "ok": report.ok,
    }
    path = root / f"validation_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
