from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from octa.core.data.research_bridge import load_research_export
from octa.core.data.recycling.common import sha256_file, stable_hash, utc_now_compact
from octa.core.features.research_features import build_research_features
from octa.core.shadow.shadow_engine import run_shadow_trading
from octa.core.shadow.shadow_validation import validate_shadow_run
from octa.core.validation.research_validation import validate_research_payload


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_shadow_pipeline(
    *,
    research_export_path: str | Path,
    prices_df: pd.DataFrame,
    config: dict[str, Any],
    evidence_root: str | Path = "octa/var/evidence",
) -> dict[str, Any]:
    run_id = str(config.get("run_id") or f"shadow_run_{utc_now_compact()}")
    evidence_dir = Path(evidence_root) / run_id
    if evidence_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {evidence_dir}")
    evidence_dir.mkdir(parents=True, exist_ok=False)

    payload = load_research_export(research_export_path)
    features = build_research_features(payload["signals"])
    research_validation = validate_research_payload(features, payload["returns"])
    shadow_result = run_shadow_trading(features, prices_df, config)
    shadow_validation = validate_shadow_run(
        shadow_result["trades"],
        shadow_result["equity_curve"],
        max_equity_jump=float(config.get("max_equity_jump", 1.0)),
    )

    trades_path = evidence_dir / "trades.parquet"
    equity_path = evidence_dir / "equity_curve.parquet"
    metrics_path = evidence_dir / "metrics.json"
    config_path = evidence_dir / "shadow_config.json"
    sample_trades_path = evidence_dir / "sample_trades.txt"
    sample_equity_path = evidence_dir / "sample_equity_head.txt"
    manifest_path = evidence_dir / "run_manifest.json"

    shadow_result["trades"].to_parquet(trades_path)
    shadow_result["equity_curve"].to_parquet(equity_path)
    _write_json(metrics_path, shadow_result["metrics"])
    _write_json(config_path, config)
    _write_text(
        sample_trades_path,
        shadow_result["trades"].head().to_string() + "\n",
    )
    _write_text(
        sample_equity_path,
        shadow_result["equity_curve"].head().to_string() + "\n",
    )

    hashed_paths = {
        "trades.parquet": sha256_file(trades_path),
        "equity_curve.parquet": sha256_file(equity_path),
        "metrics.json": sha256_file(metrics_path),
        "shadow_config.json": sha256_file(config_path),
        "sample_trades.txt": sha256_file(sample_trades_path),
        "sample_equity_head.txt": sha256_file(sample_equity_path),
    }
    manifest = {
        "run_id": run_id,
        "research_export_path": str(Path(research_export_path).resolve()),
        "evidence_dir": str(evidence_dir.resolve()),
        "config_hash": stable_hash(config),
        "research_validation": research_validation,
        "shadow_validation": shadow_validation,
        "hashes": hashed_paths,
    }
    _write_json(manifest_path, manifest)

    return {
        "trades": shadow_result["trades"],
        "equity_curve": shadow_result["equity_curve"],
        "metrics": shadow_result["metrics"],
        "evidence_dir": evidence_dir,
        "run_manifest": manifest,
    }


__all__ = ["run_shadow_pipeline"]
