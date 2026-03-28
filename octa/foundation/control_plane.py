from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from octa.execution.runner import ExecutionConfig, run_execution
from octa.support.ops.run_full_cascade_training_from_parquets import (
    RunSettings,
    _load_symbols_file,
    _normalize_asset_class_filter,
    _parse_symbols_arg,
    run_full_cascade,
)
from octa_training.core.config import load_config
from octa_ops.autopilot.cascade_train import run_cascade_training


FOUNDATION_SCOPE = "v0.0.0_foundation"
CANONICAL_OPERATOR_SURFACE = "scripts/run_octa.py"
CANONICAL_MODULE = "octa.foundation.control_plane"


def _utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _default_run_id(prefix: str) -> str:
    return f"{prefix}_{_utc_ts()}"


def _default_evidence_dir(run_id: str) -> Path:
    return Path("octa") / "var" / "evidence" / run_id


def canonical_entry_message(*, entrypoint: str, approved_surface: str = CANONICAL_OPERATOR_SURFACE) -> str:
    return (
        f"non_canonical_foundation_entrypoint:{entrypoint}:"
        f"use_{approved_surface}_or_python_-m_{CANONICAL_MODULE}"
    )


def block_non_canonical_entry(entrypoint: str) -> None:
    raise SystemExit(canonical_entry_message(entrypoint=entrypoint))


def run_foundation_shadow(
    *,
    asset_class: Optional[str] = None,
    max_symbols: int = 0,
    run_id: Optional[str] = None,
    evidence_dir: Optional[Path] = None,
    loop: bool = False,
    cycle_seconds: int = 60,
    max_cycles: int = 1,
    enable_carry: bool = False,
    carry_config_path: Optional[Path] = None,
    carry_rates_path: Optional[Path] = None,
    # P0-2: Canonical ML inference activation (default=False, must be explicit)
    ml_inference_enabled: bool = False,
    artifact_dir: Optional[Path] = None,
    raw_data_dir: Optional[Path] = None,
    inference_timeframe: str = "1D",
    # P0-3/P0-5: Secondary artifact source (paper_ready/ from training promotion)
    paper_ready_dir: Optional[Path] = None,
    # P0-4: Broker config for TWS pre-execution gate (None = skip gate)
    broker_cfg_path: Optional[Path] = None,
) -> dict[str, Any]:
    resolved_run_id = str(run_id).strip() if run_id else _default_run_id("foundation_shadow")
    cfg = ExecutionConfig(
        mode="dry-run",
        asset_class=asset_class,
        max_symbols=int(max_symbols),
        evidence_dir=evidence_dir or _default_evidence_dir(resolved_run_id),
        training_run_id=(str(run_id).strip() if run_id else None),
        loop=bool(loop),
        cycle_seconds=int(cycle_seconds),
        max_cycles=int(max_cycles),
        enable_carry=bool(enable_carry),
        carry_config_path=carry_config_path or (Path("octa") / "var" / "config" / "carry_config.json"),
        carry_rates_path=carry_rates_path or (Path("octa") / "var" / "config" / "carry_rates.json"),
        # P0-2: inference activation
        ml_inference_enabled=bool(ml_inference_enabled),
        artifact_dir=artifact_dir if artifact_dir is not None else Path("raw") / "PKL",
        raw_data_dir=raw_data_dir if raw_data_dir is not None else Path("raw"),
        inference_timeframe=str(inference_timeframe).strip().upper() or "1D",
        # P0-3/P0-5: paper_ready secondary artifact lookup
        paper_ready_dir=paper_ready_dir,
        # P0-4: broker pre-execution gate
        broker_cfg_path=broker_cfg_path,
    )
    return run_execution(cfg)


def run_foundation_training(
    *,
    run_id: Optional[str] = None,
    evidence_dir: Optional[Path] = None,
    root: Path = Path("raw"),
    batch_size: int = 50,
    max_symbols: int = 0,
    start_at: Optional[str] = None,
    resume: bool = False,
    dry_run: bool = False,
    config_path: Optional[str] = None,
    symbols: Optional[str] = None,
    symbols_file: Optional[str] = None,
    asset_classes: Optional[Sequence[str]] = None,
    follow_symlinks: bool = False,
    promote_required_tfs: Sequence[str] = ("1D", "1H"),
    paper_registry_dir: Path = Path("octa") / "var" / "models" / "paper_ready",
) -> dict[str, Any]:
    resolved_run_id = str(run_id).strip() if run_id else _default_run_id("full_cascade")
    resolved_evidence_dir = evidence_dir or _default_evidence_dir(resolved_run_id)
    selected_symbols = _parse_symbols_arg(symbols) + _load_symbols_file(symbols_file)
    selected_asset_classes = _normalize_asset_class_filter(asset_classes)
    settings = RunSettings(
        root=Path(root),
        preflight_out=resolved_evidence_dir / "preflight",
        evidence_dir=resolved_evidence_dir,
        batch_size=int(batch_size),
        max_symbols=int(max_symbols),
        resume=bool(resume),
        start_at=start_at,
        dry_run=bool(dry_run),
        config_path=config_path,
        follow_symlinks=bool(follow_symlinks),
        asset_classes=selected_asset_classes,
        promote_required_tfs=tuple(str(tf).strip().upper() for tf in promote_required_tfs if str(tf).strip()) or ("1D", "1H"),
        paper_registry_dir=Path(paper_registry_dir),
        symbols_override=list(selected_symbols),
        symbols_requested_explicitly=bool((symbols and str(symbols).strip()) or (symbols_file and str(symbols_file).strip())),
        symbols_file_path=str(symbols_file).strip() if symbols_file else None,
    )
    cfg = load_config(settings.config_path or "octa_training/config/training.yaml")
    # Honour cascade_timeframes from config YAML if not already overridden in settings.
    if settings.cascade_timeframes is None and getattr(cfg, "cascade_timeframes", None):
        import dataclasses as _dc
        settings = _dc.replace(settings, cascade_timeframes=tuple(str(t).strip().upper() for t in cfg.cascade_timeframes if str(t).strip()))
    training_regime = str(getattr(cfg, "regime", "institutional_production") or "institutional_production").strip() or "institutional_production"
    manifest = {
        "scope": FOUNDATION_SCOPE,
        "entrypoint": CANONICAL_MODULE,
        "run_id": resolved_run_id,
        "mode": "offline_training",
        "training_regime": training_regime,
        "settings": {
            "root": str(settings.root),
            "preflight_out": str(settings.preflight_out),
            "evidence_dir": str(settings.evidence_dir),
            "batch_size": settings.batch_size,
            "max_symbols": settings.max_symbols,
            "resume": settings.resume,
            "start_at": settings.start_at,
            "dry_run": settings.dry_run,
            "config_path": settings.config_path,
            "asset_classes": list(settings.asset_classes or ()),
            "promote_required_tfs": list(settings.promote_required_tfs),
            "paper_registry_dir": str(settings.paper_registry_dir),
            "symbols_override": list(settings.symbols_override or ()),
            "symbols_requested_explicitly": bool(settings.symbols_requested_explicitly),
            "symbols_file_path": settings.symbols_file_path,
        },
    }
    settings.evidence_dir.mkdir(parents=True, exist_ok=True)
    (settings.evidence_dir / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return run_full_cascade(settings, train_fn=run_cascade_training)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Canonical OCTA Foundation control plane for offline training and shadow execution."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    shadow = sub.add_parser("shadow", help="Run the canonical Foundation shadow path.")
    shadow.add_argument("--asset-class", default=None)
    shadow.add_argument("--max-symbols", type=int, default=0)
    shadow.add_argument("--run-id", default=None)
    shadow.add_argument("--evidence-dir", default=None)
    shadow.add_argument("--loop", action="store_true", default=False)
    shadow.add_argument("--cycle-seconds", type=int, default=60)
    shadow.add_argument("--max-cycles", type=int, default=1)
    shadow.add_argument("--enable-carry", action="store_true", default=False)
    shadow.add_argument("--carry-config", default="octa/var/config/carry_config.json")
    shadow.add_argument("--carry-rates", default="octa/var/config/carry_rates.json")
    # P0-2: Canonical ML inference activation
    shadow.add_argument("--ml-inference-enabled", action="store_true", default=False)
    shadow.add_argument("--artifact-dir", default=None)
    shadow.add_argument("--raw-data-dir", default=None)
    shadow.add_argument("--inference-timeframe", default="1D")
    # P0-3/P0-5: Secondary artifact source (paper_ready/ from training promotion)
    shadow.add_argument("--paper-ready-dir", default=None)
    # P0-4: Broker config for TWS pre-execution gate (None = skip gate)
    shadow.add_argument("--broker-cfg", default=None)

    train = sub.add_parser("train", help="Run the canonical Foundation training cascade.")
    train.add_argument("--run-id", default=None)
    train.add_argument("--evidence-dir", default=None)
    train.add_argument("--root", default="raw")
    train.add_argument("--batch-size", type=int, default=50)
    train.add_argument("--max-symbols", type=int, default=0)
    train.add_argument("--start-at", default=None)
    train.add_argument("--resume", action="store_true", default=False)
    train.add_argument("--dry-run", action="store_true", default=False)
    train.add_argument("--config", default=None)
    train.add_argument("--symbols", default=None)
    train.add_argument("--symbols-file", default=None)
    train.add_argument("--asset-class", dest="asset_classes", action="append", default=[])
    train.add_argument("--follow-symlinks", action="store_true", default=False)
    train.add_argument("--promote-required-tfs", default="1D,1H")
    train.add_argument("--paper-registry-dir", default=str(Path("octa") / "var" / "models" / "paper_ready"))

    args = parser.parse_args(argv)
    if args.command == "shadow":
        summary = run_foundation_shadow(
            asset_class=args.asset_class,
            max_symbols=int(args.max_symbols),
            run_id=args.run_id,
            evidence_dir=Path(args.evidence_dir) if args.evidence_dir else None,
            loop=bool(args.loop),
            cycle_seconds=int(args.cycle_seconds),
            max_cycles=int(args.max_cycles),
            enable_carry=bool(args.enable_carry),
            carry_config_path=Path(args.carry_config),
            carry_rates_path=Path(args.carry_rates),
            # P0-2: ML inference activation
            ml_inference_enabled=bool(args.ml_inference_enabled),
            artifact_dir=Path(args.artifact_dir) if args.artifact_dir else None,
            raw_data_dir=Path(args.raw_data_dir) if args.raw_data_dir else None,
            inference_timeframe=str(args.inference_timeframe).strip().upper() or "1D",
            # P0-3/P0-5: Secondary artifact source
            paper_ready_dir=Path(args.paper_ready_dir) if args.paper_ready_dir else None,
            # P0-4: TWS pre-execution gate
            broker_cfg_path=Path(args.broker_cfg) if args.broker_cfg else None,
        )
        print(json.dumps(summary, sort_keys=True))
        return 0

    summary = run_foundation_training(
        run_id=args.run_id,
        evidence_dir=Path(args.evidence_dir) if args.evidence_dir else None,
        root=Path(args.root),
        batch_size=int(args.batch_size),
        max_symbols=int(args.max_symbols),
        start_at=args.start_at,
        resume=bool(args.resume),
        dry_run=bool(args.dry_run),
        config_path=args.config,
        symbols=args.symbols,
        symbols_file=args.symbols_file,
        asset_classes=args.asset_classes,
        follow_symlinks=bool(args.follow_symlinks),
        promote_required_tfs=[t.strip().upper() for t in str(args.promote_required_tfs).split(",") if t.strip()],
        paper_registry_dir=Path(args.paper_registry_dir),
    )
    print(json.dumps(summary, sort_keys=True))
    return int(summary.get("exit_code", 0))
