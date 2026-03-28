from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import read_yaml


@dataclass(frozen=True)
class RecyclingPolicy:
    raw_roots: list[str]
    output_root: str
    evidence_root: str
    curated_zone_root: str
    feature_zone_root: str
    risk_zone_root: str
    simulation_zone_root: str
    quarantine_zone_root: str
    allowed_time_columns: list[str]
    allowed_publish_columns: list[str]
    allowed_effective_columns: list[str]
    forbidden_alpha_source_families: list[str]
    transform_whitelist: list[str]
    quality_minimum: float
    confidence_minimum: float
    model_ready_minimum: float
    coverage_minimum_rows: int
    freshness_days_soft_limit: int
    max_null_fraction: float
    max_duplicate_fraction: float
    fail_closed_on_unknown_asset: bool
    default_region: str
    default_provider: str
    default_source_family: str


def load_policy(path: Path) -> RecyclingPolicy:
    raw = read_yaml(path)

    def _get(name: str, default: Any) -> Any:
        return raw.get(name, default)

    return RecyclingPolicy(
        raw_roots=[str(x) for x in _get("raw_roots", ["raw", "data/altdat/parquet"])],
        output_root=str(_get("output_root", "artifacts/parquet_recycling")),
        evidence_root=str(_get("evidence_root", "octa/var/evidence/parquet_recycling")),
        curated_zone_root=str(_get("curated_zone_root", "artifacts/parquet_recycling/curated")),
        feature_zone_root=str(_get("feature_zone_root", "artifacts/parquet_recycling/recycled_features")),
        risk_zone_root=str(_get("risk_zone_root", "artifacts/parquet_recycling/risk_monitoring")),
        simulation_zone_root=str(_get("simulation_zone_root", "artifacts/parquet_recycling/simulation")),
        quarantine_zone_root=str(_get("quarantine_zone_root", "artifacts/parquet_recycling/quarantine")),
        allowed_time_columns=[str(x) for x in _get("allowed_time_columns", ["timestamp", "datetime", "date", "time"])],
        allowed_publish_columns=[str(x) for x in _get("allowed_publish_columns", ["publish_time", "published_at", "release_ts"])],
        allowed_effective_columns=[str(x) for x in _get("allowed_effective_columns", ["effective_time", "effective_at", "asof_date"])],
        forbidden_alpha_source_families=[str(x) for x in _get("forbidden_alpha_source_families", ["unknown", "archive_only"])],
        # Only the 4 implemented transforms are listed; unlisted values are silently skipped.
        transform_whitelist=[str(x) for x in _get("transform_whitelist", ["zscore", "rolling_delta", "percentile", "anomaly_flag"])],
        quality_minimum=float(_get("quality_minimum", 0.55)),
        confidence_minimum=float(_get("confidence_minimum", 0.60)),
        model_ready_minimum=float(_get("model_ready_minimum", 0.85)),
        coverage_minimum_rows=int(_get("coverage_minimum_rows", 128)),
        freshness_days_soft_limit=int(_get("freshness_days_soft_limit", 30)),
        max_null_fraction=float(_get("max_null_fraction", 0.25)),
        max_duplicate_fraction=float(_get("max_duplicate_fraction", 0.02)),
        fail_closed_on_unknown_asset=bool(_get("fail_closed_on_unknown_asset", True)),
        default_region=str(_get("default_region", "global")),
        default_provider=str(_get("default_provider", "unknown")),
        default_source_family=str(_get("default_source_family", "unknown")),
    )
