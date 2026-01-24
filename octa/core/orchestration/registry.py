from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from octa.core.monitoring import store as monitoring_store
from octa.core.runtime.run_registry import RunRegistry


@dataclass
class RegistryConfig:
    db_path: Path


def get_default_registry(var_root: Optional[Path] = None) -> RunRegistry:
    db_path = monitoring_store.get_default_db_path(var_root)
    return RunRegistry(db_path)


def record_run_start(
    *,
    run_id: str,
    config: Mapping[str, Any],
    git_sha: Optional[str] = None,
    var_root: Optional[Path] = None,
) -> None:
    registry = get_default_registry(var_root)
    registry.record_run_start(run_id=run_id, config=config, git_sha=git_sha)

