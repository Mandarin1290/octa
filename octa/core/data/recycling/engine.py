from __future__ import annotations

import math
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from octa.core.data.io.io_parquet import compute_sha256
from octa.core.data.io.timeseries_integrity import write_quarantine_entry
from octa.support.ops.universe_preflight import ASSET_CLASS_ALIASES

from .common import ensure_relative_to, stable_hash
from .models import (
    ClassificationDecision,
    DatasetRecord,
    RecycledFeatureArtifact,
    RoutingDecision,
    ScanIssue,
    UtilityAssessment,
)
from .policy import RecyclingPolicy


def _canonical_asset_class(parts: tuple[str, ...]) -> list[str]:
    inferred: list[str] = []
    for part in parts:
        lower = part.lower()
        alias = ASSET_CLASS_ALIASES.get(lower)
        if alias and alias not in inferred:
            inferred.append(alias)
    return inferred


def _infer_dataset_name(path: Path) -> str:
    parts = [p for p in path.parts if p]
    if len(parts) >= 3:
        return "__".join(parts[-3:]).replace(".parquet", "")
    return path.stem


def _infer_frequency(path: Path, columns: list[str]) -> str:
    stem = path.stem.upper()
    for tf in ("1D", "1H", "30M", "5M", "1M"):
        if stem.endswith(f"_{tf}"):
            return tf
    lowered = {c.lower() for c in columns}
    if "timestamp" in lowered:
        return "unknown_temporal"
    return "unknown"


def _resolve_time_column(df: pd.DataFrame, policy: RecyclingPolicy) -> str | None:
    for candidate in policy.allowed_time_columns:
        for col in df.columns:
            if str(col).lower() == candidate.lower():
                return str(col)
    if isinstance(df.index, pd.DatetimeIndex):
        return "__index__"
    return None


def _iso_or_none(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.tz_convert("UTC").isoformat()
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("UTC").isoformat()
    except Exception:
        return None


def discover_files(policy: RecyclingPolicy) -> list[Path]:
    found: list[Path] = []
    for root in policy.raw_roots:
        rp = Path(root)
        if not rp.exists():
            continue
        found.extend(sorted(rp.rglob("*.parquet"), key=lambda p: str(p).upper()))
    deduped = sorted({p.resolve() for p in found}, key=lambda p: str(p).upper())
    return deduped


def build_input_manifest(files: list[Path], policy: RecyclingPolicy) -> dict[str, Any]:
    return {
        "raw_roots": policy.raw_roots,
        "file_count": len(files),
        "files": [str(p) for p in files],
    }


def inventory_datasets(
    files: list[Path],
    policy: RecyclingPolicy,
    evidence_quarantine_dir: Path,
    *,
    reference_time: datetime | None = None,
) -> list[DatasetRecord]:
    # reference_time: inject for deterministic freshness_score; defaults to datetime.now(UTC)
    _reference_time = reference_time if reference_time is not None else datetime.now(timezone.utc)
    records: list[DatasetRecord] = []
    file_hash_counter: Counter[str] = Counter()
    schema_hash_counter: Counter[str] = Counter()

    for path in files:
        issues: list[ScanIssue] = []
        sha256 = compute_sha256(path)
        file_hash_counter[sha256] += 1
        try:
            parquet = pq.ParquetFile(path)
            schema = parquet.schema_arrow
            column_names = [str(field.name) for field in schema]
            schema_fingerprint = stable_hash(
                [{"name": str(field.name), "type": str(field.type)} for field in schema]
            )
            schema_hash_counter[schema_fingerprint] += 1
            row_count = int(parquet.metadata.num_rows)
            column_count = len(column_names)
            df = pd.read_parquet(path)
        except Exception as exc:
            issues.append(
                ScanIssue(
                    code="parquet_unreadable",
                    severity="SEVERE",
                    message=f"cannot read parquet: {type(exc).__name__}:{exc}",
                )
            )
            write_quarantine_entry(
                evidence_quarantine_dir,
                str(path),
                f"PARQUET_UNREADABLE:{type(exc).__name__}",
                "unknown",
                "unknown",
            )
            records.append(
                DatasetRecord(
                    file_path=str(path),
                    logical_dataset_name=_infer_dataset_name(path),
                    source_family=policy.default_source_family,
                    provider=policy.default_provider,
                    asset_mapping_status="unknown",
                    inferred_asset_classes=[],
                    inferred_symbols=[],
                    inferred_regions=[],
                    inferred_frequency="unknown",
                    time_coverage_start=None,
                    time_coverage_end=None,
                    row_count=None,
                    column_count=None,
                    schema_fingerprint="unreadable",
                    file_hash_sha256=sha256,
                    partitioning_info={"parts": list(path.parts[:-1])},
                    null_profile={},
                    duplicate_profile={"duplicate_rows": None, "duplicate_fraction": None},
                    monotonicity={"time_column": None, "monotonic": False},
                    timezone_semantics={"timezone": "unknown", "time_column": None},
                    event_time_semantics={"event_time": None, "publish_time": None, "effective_time": None},
                    lookahead_risk_flags=["unreadable"],
                    leakage_risk_flags=["unreadable"],
                    quality_score=0.0,
                    freshness_score=0.0,
                    utility_score_preliminary=0.0,
                    governance_status="quarantined",
                    lineage_parent_refs=[],
                    confidence_score=0.0,
                    quarantine_reason="parquet_unreadable",
                    issues=issues,
                )
            )
            continue

        time_col = _resolve_time_column(df, policy)
        asset_classes = _canonical_asset_class(tuple(path.parts))
        inferred_symbols = [path.stem.rsplit("_", 1)[0].upper()] if "_" in path.stem else [path.stem.upper()]
        source_family = "market_ohlcv" if {"open", "high", "low", "close"}.issubset({c.lower() for c in column_names}) else "altdata"
        provider = path.parts[1] if len(path.parts) > 1 else policy.default_provider
        null_profile = {str(col): round(float(df[col].isna().mean()), 6) for col in df.columns[:64]}
        duplicate_rows = int(df.duplicated().sum())
        duplicate_fraction = float(duplicate_rows / len(df)) if len(df) else 0.0
        monotonic = False
        tz_semantics = {"timezone": "unknown", "time_column": time_col}
        start = None
        end = None
        lookahead_flags: list[str] = []
        leakage_flags: list[str] = []
        event_time: str | None = None
        event_time_col: str | None = None
        publish_time: str | None = None
        effective_time: str | None = None
        confidence = 0.30

        if time_col:
            try:
                series = df.index.to_series() if time_col == "__index__" else pd.to_datetime(df[time_col], utc=True)
                monotonic = bool(series.is_monotonic_increasing)
                start = _iso_or_none(series.min())
                end = _iso_or_none(series.max())
                sample_ts = series.iloc[0] if len(series) else None
                if isinstance(sample_ts, pd.Timestamp) and sample_ts.tzinfo is not None:
                    tz_semantics["timezone"] = str(sample_ts.tzinfo)
                confidence += 0.25
            except Exception:
                issues.append(
                    ScanIssue(
                        code="time_parse_failed",
                        severity="SEVERE",
                        message="time column exists but cannot be parsed deterministically",
                    )
                )
                lookahead_flags.append("time_parse_failed")
        else:
            issues.append(
                ScanIssue(
                    code="missing_time_axis",
                    severity="SEVERE",
                    message="no recognized time axis found",
                )
            )

        lowered = {str(col).lower(): str(col) for col in df.columns}
        for candidate in policy.allowed_effective_columns:
            if candidate.lower() in lowered:
                effective_time = lowered[candidate.lower()]
                break
        for candidate in policy.allowed_publish_columns:
            if candidate.lower() in lowered:
                publish_time = lowered[candidate.lower()]
                break
        # event_time column tracked separately from effective_time.
        # "event_time" is intentionally NOT in allowed_effective_columns to avoid
        # conflating event semantics with effective-date semantics (S0-3 fix).
        if "event_time" in lowered:
            event_time_col = lowered["event_time"]
        if time_col and time_col != "__index__":
            event_time = time_col

        if publish_time and effective_time:
            try:
                pub = pd.to_datetime(df[publish_time], utc=True)
                eff = pd.to_datetime(df[effective_time], utc=True)
                if bool((pub > eff).any()):
                    lookahead_flags.append("publish_after_effective")
            except Exception:
                lookahead_flags.append("publish_effective_unparseable")
        elif publish_time and event_time_col:
            # explicit event_time column: publish before event = potential leakage
            try:
                pub = pd.to_datetime(df[publish_time], utc=True)
                evt = pd.to_datetime(df[event_time_col], utc=True)
                if bool((pub < evt).any()):
                    leakage_flags.append("publish_precedes_event")
            except Exception:
                leakage_flags.append("publish_event_unparseable")
        elif publish_time and event_time:
            # fallback: use main time axis as event reference
            try:
                pub = pd.to_datetime(df[publish_time], utc=True)
                evt = pd.to_datetime(df[event_time], utc=True)
                if bool((pub < evt).any()):
                    leakage_flags.append("publish_precedes_event")
            except Exception:
                leakage_flags.append("publish_event_unparseable")
        else:
            lookahead_flags.append("missing_publish_effective_semantics")

        if asset_classes:
            confidence += 0.15
        if source_family != policy.default_source_family:
            confidence += 0.10

        if not asset_classes and policy.fail_closed_on_unknown_asset:
            issues.append(
                ScanIssue(
                    code="unknown_asset_class",
                    severity="SEVERE",
                    message="asset class could not be inferred from path",
                )
            )

        max_null = max(null_profile.values()) if null_profile else 1.0
        if max_null > policy.max_null_fraction:
            issues.append(
                ScanIssue(
                    code="excess_null_fraction",
                    severity="SEVERE",
                    message=f"max null fraction {round(max_null, 4)} exceeds policy threshold {policy.max_null_fraction}",
                    details={"max_null_fraction": round(max_null, 6), "threshold": policy.max_null_fraction},
                )
            )
        if duplicate_fraction > policy.max_duplicate_fraction:
            issues.append(
                ScanIssue(
                    code="excess_duplicate_fraction",
                    severity="SEVERE",
                    message=f"duplicate fraction {round(duplicate_fraction, 4)} exceeds policy threshold {policy.max_duplicate_fraction}",
                    details={"duplicate_fraction": round(duplicate_fraction, 6), "threshold": policy.max_duplicate_fraction},
                )
            )
        quality_score = 1.0
        quality_score -= min(max_null, 1.0) * 0.35
        quality_score -= min(duplicate_fraction, 1.0) * 0.25
        quality_score -= 0.15 if not monotonic else 0.0
        quality_score -= 0.15 if not time_col else 0.0
        quality_score -= 0.10 if issues else 0.0
        quality_score = max(0.0, round(quality_score, 6))

        freshness_score = 0.0
        if end:
            try:
                days = (_reference_time - pd.Timestamp(end).to_pydatetime()).days
                freshness_score = max(0.0, round(1.0 - (days / max(policy.freshness_days_soft_limit, 1)), 6))
            except Exception:
                freshness_score = 0.0

        utility_prelim = round((quality_score * 0.6) + (freshness_score * 0.2) + min(confidence, 1.0) * 0.2, 6)
        governance_status = "candidate"
        quarantine_reason = None
        if issues or quality_score < policy.quality_minimum:
            governance_status = "quarantined"
            quarantine_reason = issues[0].code if issues else "quality_below_minimum"
            write_quarantine_entry(
                evidence_quarantine_dir,
                str(path),
                quarantine_reason.upper(),
                asset_classes[0] if asset_classes else "unknown",
                _infer_frequency(path, column_names),
            )

        records.append(
            DatasetRecord(
                file_path=str(path),
                logical_dataset_name=_infer_dataset_name(path),
                source_family=source_family,
                provider=provider,
                asset_mapping_status="mapped" if asset_classes else "unknown",
                inferred_asset_classes=asset_classes,
                inferred_symbols=inferred_symbols,
                inferred_regions=[policy.default_region],
                inferred_frequency=_infer_frequency(path, column_names),
                time_coverage_start=start,
                time_coverage_end=end,
                row_count=row_count,
                column_count=column_count,
                schema_fingerprint=schema_fingerprint,
                file_hash_sha256=sha256,
                partitioning_info={"parts": list(path.parts[:-1]), "relative_path": ensure_relative_to(path, Path.cwd())},
                null_profile=null_profile,
                duplicate_profile={"duplicate_rows": duplicate_rows, "duplicate_fraction": round(duplicate_fraction, 6)},
                monotonicity={"time_column": time_col, "monotonic": monotonic},
                timezone_semantics=tz_semantics,
                event_time_semantics={"event_time": event_time, "event_time_col": event_time_col, "publish_time": publish_time, "effective_time": effective_time},
                lookahead_risk_flags=sorted(set(lookahead_flags)),
                leakage_risk_flags=sorted(set(leakage_flags)),
                quality_score=quality_score,
                freshness_score=freshness_score,
                utility_score_preliminary=utility_prelim,
                governance_status=governance_status,
                lineage_parent_refs=[sha256],
                confidence_score=round(min(confidence, 1.0), 6),
                quarantine_reason=quarantine_reason,
                issues=issues,
            )
        )

    duplicate_hashes = {k for k, v in file_hash_counter.items() if v > 1}
    duplicate_schemas = {k for k, v in schema_hash_counter.items() if v > 1}
    adjusted: list[DatasetRecord] = []
    for record in records:
        issues = list(record.issues)
        if record.file_hash_sha256 in duplicate_hashes:
            issues.append(
                ScanIssue(
                    code="duplicate_file_content",
                    severity="WARNING",
                    message="identical file content also appears elsewhere",
                )
            )
        if record.schema_fingerprint in duplicate_schemas:
            issues.append(
                ScanIssue(
                    code="shared_schema_family",
                    severity="INFO",
                    message="schema fingerprint reused across multiple datasets",
                )
            )
        adjusted.append(
            DatasetRecord(
                **{
                    **asdict(record),
                    "issues": issues,
                }
            )
        )
    return adjusted


def classify_datasets(records: list[DatasetRecord], policy: RecyclingPolicy) -> list[ClassificationDecision]:
    decisions: list[ClassificationDecision] = []
    for record in records:
        reasons: list[str] = []
        secondary: list[str] = []
        blocking_flags: list[str] = []
        primary = "research_candidate"
        confidence = record.confidence_score
        frequency = record.inferred_frequency.upper()
        cols_risk = set(record.lookahead_risk_flags) | set(record.leakage_risk_flags)
        material_risk_flags = {
            flag
            for flag in cols_risk
            if not (
                record.source_family == "market_ohlcv"
                and flag == "missing_publish_effective_semantics"
            )
        }
        if record.governance_status == "quarantined":
            primary = "quarantine_candidate"
            blocking_flags.append(record.quarantine_reason or "quarantined")
            reasons.append("dataset already quarantined by inventory/validation layer")
            confidence = min(confidence, 0.4)
        elif record.source_family == "market_ohlcv" and record.quality_score >= 0.75:
            primary = "simulation_candidate"
            secondary.extend(["risk_candidate", "monitoring_candidate"])
            reasons.append("structured OHLCV coverage suits simulation, risk and monitoring usage")
            if frequency in {"1D", "1H", "30M", "5M", "1M"} and not material_risk_flags:
                secondary.append("alpha_candidate")
                reasons.append("temporal granularity is compatible with alpha research, subject to separate promotion")
        elif record.source_family == "altdata":
            primary = "regime_candidate"
            secondary.extend(["risk_candidate", "monitoring_candidate", "research_candidate"])
            reasons.append("altdata semantics fit regime/risk/monitoring before alpha")
            if record.event_time_semantics.get("publish_time"):
                secondary.append("simulation_candidate")
        if record.source_family in policy.forbidden_alpha_source_families:
            blocking_flags.append("forbidden_alpha_source_family")
        if material_risk_flags:
            blocking_flags.extend(sorted(material_risk_flags))
            if primary == "simulation_candidate":
                primary = "risk_candidate"
                reasons.append("lookahead/leakage flags downgrade dataset away from alpha/simulation priority")
        if not record.inferred_asset_classes:
            blocking_flags.append("unknown_asset_mapping")
        if record.row_count is not None and record.row_count < policy.coverage_minimum_rows:
            blocking_flags.append("insufficient_coverage")
            reasons.append("row coverage below policy minimum")
        promotion_eligibility = "blocked"
        if not blocking_flags and record.quality_score >= policy.quality_minimum and record.confidence_score >= policy.confidence_minimum:
            promotion_eligibility = "curated_only"
            if "alpha_candidate" in secondary and record.quality_score >= policy.model_ready_minimum:
                promotion_eligibility = "model_ready_candidate"
        decisions.append(
            ClassificationDecision(
                file_path=record.file_path,
                primary_role=primary,
                secondary_roles=sorted(dict.fromkeys(secondary)),
                classification_reasoning=reasons or ["defaulted conservatively due to limited evidence"],
                classification_confidence=round(confidence, 6),
                blocking_flags=sorted(dict.fromkeys(blocking_flags)),
                promotion_eligibility=promotion_eligibility,
            )
        )
    return decisions


def score_datasets(records: list[DatasetRecord], decisions: list[ClassificationDecision]) -> list[UtilityAssessment]:
    by_path = {d.file_path: d for d in decisions}
    assessments: list[UtilityAssessment] = []
    for record in records:
        decision = by_path[record.file_path]
        alpha = 0.0
        risk = 0.2 + (0.4 if decision.primary_role in {"risk_candidate", "simulation_candidate"} else 0.0)
        regime = 0.3 if decision.primary_role == "regime_candidate" else 0.1
        monitoring = 0.35 if "monitoring_candidate" in decision.secondary_roles else 0.15
        simulation = 0.5 if decision.primary_role == "simulation_candidate" else 0.1
        research = 0.3 if decision.primary_role in {"research_candidate", "archive_candidate"} else 0.2
        if "alpha_candidate" in decision.secondary_roles and not decision.blocking_flags:
            alpha = min(1.0, record.quality_score * 0.8 + record.confidence_score * 0.2)
        if decision.primary_role == "quarantine_candidate":
            alpha = 0.0
            risk = min(risk, 0.2)
            simulation = 0.0
        compute_cost = 0.1 + min((record.row_count or 0) / 1_000_000.0, 1.0) * 0.5
        maintenance_cost = 0.15 + min((record.column_count or 0) / 200.0, 1.0) * 0.35
        coverage_value = min(1.0, (record.row_count or 0) / 10_000.0)
        robustness = max(0.0, min(1.0, record.quality_score))
        interpretability = 0.75 if record.source_family == "market_ohlcv" else 0.6
        governance_burden = 0.2 + (0.3 if decision.blocking_flags else 0.0) + (0.2 if record.governance_status == "quarantined" else 0.0)
        roi = round(
            (
                alpha * 0.20
                + risk * 0.18
                + regime * 0.12
                + monitoring * 0.12
                + simulation * 0.14
                + research * 0.09
                + coverage_value * 0.08
                + robustness * 0.10
                + interpretability * 0.07
                - compute_cost * 0.05
                - maintenance_cost * 0.05
                - governance_burden * 0.10
            ),
            6,
        )
        if decision.primary_role == "quarantine_candidate":
            roi_class = "quarantine"
            zone = "quarantine_only"
            recommendation = "quarantine"
        elif roi >= 0.45:
            roi_class = "high_value"
            zone = {
                "simulation_candidate": "simulation_only",
                "risk_candidate": "risk_overlay_only",
                "regime_candidate": "regime_engine_only",
            }.get(decision.primary_role, "curated_only")
            recommendation = "review_for_governance_promotion" if decision.promotion_eligibility == "model_ready_candidate" else "hold"
        elif roi >= 0.20:
            roi_class = "medium_value"
            zone = "curated_only"
            recommendation = "hold"
        else:
            roi_class = "archive_value"
            zone = "archive_only"
            recommendation = "archive"
        assessments.append(
            UtilityAssessment(
                file_path=record.file_path,
                alpha_utility=round(alpha, 6),
                risk_utility=round(risk, 6),
                regime_utility=round(regime, 6),
                monitoring_utility=round(monitoring, 6),
                simulation_utility=round(simulation, 6),
                research_utility=round(research, 6),
                compute_cost=round(compute_cost, 6),
                maintenance_cost=round(maintenance_cost, 6),
                coverage_value=round(coverage_value, 6),
                robustness_value=round(robustness, 6),
                interpretability=round(interpretability, 6),
                governance_burden=round(governance_burden, 6),
                roi_composite_score=roi,
                roi_classification=roi_class,
                recommended_role=decision.primary_role,
                recommended_zone=zone,
                recommendation=recommendation,
            )
        )
    return assessments


def build_recycled_features(records: list[DatasetRecord], decisions: list[ClassificationDecision], policy: RecyclingPolicy) -> list[RecycledFeatureArtifact]:
    by_path = {d.file_path: d for d in decisions}
    artifacts: list[RecycledFeatureArtifact] = []
    for record in records:
        decision = by_path[record.file_path]
        if record.governance_status == "quarantined":
            continue
        path = Path(record.file_path)
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        numeric_cols = [str(col) for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        if not numeric_cols:
            continue
        col = numeric_cols[0]
        series = pd.to_numeric(df[col], errors="coerce")
        preview_rows: list[dict[str, Any]] = []
        transforms: list[tuple[str, pd.Series, str, str]] = []
        if "zscore" in policy.transform_whitelist:
            std = series.std()
            values = ((series - series.mean()) / std) if std and not pd.isna(std) else series * 0.0
            transforms.append(("zscore", values, f"{col}_zscore", "monitoring"))
        if "rolling_delta" in policy.transform_whitelist:
            transforms.append(("rolling_delta", series.diff().fillna(0.0), f"{col}_delta_1", "simulation"))
        if "percentile" in policy.transform_whitelist:
            transforms.append(("percentile", series.rank(pct=True).fillna(0.0), f"{col}_pct_rank", "regime"))
        if "anomaly_flag" in policy.transform_whitelist:
            baseline = series.rolling(20, min_periods=5).mean()
            spread = series.rolling(20, min_periods=5).std().replace(0, pd.NA)
            anomaly = (((series - baseline).abs() / spread) > 3.0).fillna(False).astype(int)
            transforms.append(("anomaly_flag", anomaly, f"{col}_anomaly_flag", "risk"))
        for transform_id, values, feature_name, role in transforms:
            for idx, value in list(values.tail(5).items()):
                preview_rows.append({"ts": _iso_or_none(idx), "value": None if pd.isna(value) else float(value)})
            lineage_hash = stable_hash({"file": record.file_hash_sha256, "transform": transform_id, "feature": feature_name})
            config_hash = stable_hash({"transform_id": transform_id, "target_role": role})
            artifacts.append(
                RecycledFeatureArtifact(
                    dataset_ref=record.file_hash_sha256,
                    transform_id=transform_id,
                    feature_name=feature_name,
                    target_role=role,
                    config_hash=config_hash,
                    lineage_hash=lineage_hash,
                    feature_hash=stable_hash(preview_rows),
                    economic_rationale=f"Compressed {col} into {transform_id} for {role} usage; no direct model promotion implied.",
                    leakage_safe=not bool(record.lookahead_risk_flags or record.leakage_risk_flags),
                    values_preview=preview_rows,
                )
            )
            preview_rows = []
        if decision.primary_role == "quarantine_candidate":
            continue
    return artifacts


def route_datasets(
    records: list[DatasetRecord],
    decisions: list[ClassificationDecision],
    utility: list[UtilityAssessment],
) -> list[RoutingDecision]:
    by_decision = {d.file_path: d for d in decisions}
    by_utility = {u.file_path: u for u in utility}
    routes: list[RoutingDecision] = []
    for record in records:
        decision = by_decision[record.file_path]
        utility_row = by_utility[record.file_path]
        allowed = False
        route = "quarantine_only"
        reason = "fail_closed_default"
        if record.governance_status == "quarantined" or decision.primary_role == "quarantine_candidate":
            reason = "quarantined_or_blocked"
        elif utility_row.recommended_zone == "archive_only":
            allowed = True
            route = "archive_only"
            reason = "low_roi_archive_route"
        elif decision.promotion_eligibility == "model_ready_candidate":
            allowed = True
            route = "model_ready_candidate"
            reason = "eligible_but_not_promoted"
        elif utility_row.recommended_zone in {
            "curated_only",
            "risk_overlay_only",
            "regime_engine_only",
            "monitoring_only",
            "simulation_only",
        }:
            allowed = True
            route = utility_row.recommended_zone
            reason = "policy_and_roi_allow_non_model_route"
        routes.append(
            RoutingDecision(
                file_path=record.file_path,
                route=route,
                allowed=allowed,
                reason=reason,
                governance_status=record.governance_status,
                model_ready_whitelisted=route == "model_ready_candidate" and allowed,
            )
        )
    return routes


def build_validation_report(records: list[DatasetRecord]) -> list[dict[str, Any]]:
    """Issue-centric validation view — semantically distinct from the full dataset catalog.

    Returns one row per dataset with severity summary, issue codes, quality/confidence
    scores and time/leakage flags.  Does NOT repeat the full structural fields of the
    catalog (source_family, partitioning_info, null_profile breakdown, etc.).
    """
    report: list[dict[str, Any]] = []
    for record in records:
        issue_codes = sorted({i.code for i in record.issues})
        severities = {i.severity for i in record.issues}
        max_severity = "OK"
        if "SEVERE" in severities:
            max_severity = "SEVERE"
        elif "WARNING" in severities:
            max_severity = "WARNING"
        elif "INFO" in severities:
            max_severity = "INFO"
        report.append(
            {
                "file_path": record.file_path,
                "governance_status": record.governance_status,
                "max_severity": max_severity,
                "issue_count": len(record.issues),
                "issue_codes": issue_codes,
                "quality_score": record.quality_score,
                "confidence_score": record.confidence_score,
                "lookahead_risk_flags": record.lookahead_risk_flags,
                "leakage_risk_flags": record.leakage_risk_flags,
                "null_profile_max": round(max(record.null_profile.values()), 6) if record.null_profile else None,
                "duplicate_fraction": record.duplicate_profile.get("duplicate_fraction"),
                "monotonic": record.monotonicity.get("monotonic"),
                "time_column": record.monotonicity.get("time_column"),
                "issues": [
                    {"code": i.code, "severity": i.severity, "message": i.message}
                    for i in record.issues
                ],
            }
        )
    return report


def summarize_run(
    records: list[DatasetRecord],
    decisions: list[ClassificationDecision],
    utility: list[UtilityAssessment],
    routes: list[RoutingDecision],
    artifacts: list[RecycledFeatureArtifact],
) -> str:
    total = len(records)
    quarantined = sum(1 for r in records if r.governance_status == "quarantined")
    role_counts = Counter(d.primary_role for d in decisions)
    route_counts = Counter(r.route for r in routes)
    roi_counts = Counter(u.roi_classification for u in utility)
    lines = [
        "# Parquet Recycling Summary",
        "",
        f"- datasets_scanned: {total}",
        f"- quarantined: {quarantined}",
        f"- recycled_feature_artifacts: {len(artifacts)}",
        f"- primary_roles: {dict(sorted(role_counts.items()))}",
        f"- routes: {dict(sorted(route_counts.items()))}",
        f"- roi_classes: {dict(sorted(roi_counts.items()))}",
        "",
        "Fail-closed behavior remains active: anything unclassified, low-confidence, time-unsafe, or governance-blocked stayed out of model-ready promotion.",
    ]
    return "\n".join(lines) + "\n"
