from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class OrchestrationPaths:
    var_root: Path
    artifacts_root: Path
    runs_root: Path
    metrics_root: Path


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def get_paths(var_root: Path | None = None) -> OrchestrationPaths:
    base = Path(var_root) if var_root is not None else Path("octa") / "var"
    return OrchestrationPaths(
        var_root=base,
        artifacts_root=base / "artifacts",
        runs_root=base / "artifacts" / "runs",
        metrics_root=base / "metrics",
    )


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{_now_tag()}"


def ensure_run_dirs(run_id: str, var_root: Path | None = None) -> Mapping[str, Path]:
    paths = get_paths(var_root)
    run_root = paths.runs_root / run_id
    survivors_dir = run_root / "survivors"
    metrics_dir = run_root / "metrics"
    models_dir = run_root / "models"
    reports_dir = run_root / "reports"

    for p in (paths.var_root, paths.artifacts_root, paths.metrics_root, run_root, survivors_dir, metrics_dir, models_dir, reports_dir):
        p.mkdir(parents=True, exist_ok=True)

    return {
        "run_root": run_root,
        "survivors_dir": survivors_dir,
        "metrics_dir": metrics_dir,
        "models_dir": models_dir,
        "reports_dir": reports_dir,
    }


def write_config_snapshot(run_root: Path, config: Mapping[str, Any]) -> Path:
    path = run_root / "config_snapshot.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path

