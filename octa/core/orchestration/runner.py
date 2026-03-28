from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from octa.core.monitoring import emit_event, emit_metric
from octa.core.orchestration.adapters.l1_global import L1GlobalAdapter
from octa.core.orchestration.adapters.l2_signal import L2SignalAdapter
from octa.core.orchestration.adapters.l3_structure import L3StructureAdapter
from octa.core.orchestration.adapters.l4_execution import L4ExecutionAdapter
from octa.core.orchestration.adapters.l5_micro import L5MicroAdapter
from octa.core.orchestration.resources import ensure_run_dirs, get_paths, new_run_id, write_config_snapshot
from octa.core.orchestration.state import RunState
from octa.core.runtime.run_registry import RunRegistry
from octa.core.data.providers.parquet import ParquetOHLCVProvider, find_raw_root
from octa.core.data.sources.altdata.orchestrator import load_altdata_config
from octa.core.features.altdata.registry import FeatureRegistry
from octa_ops.autopilot.universe import discover_universe


L1_LAYER = "L1_global_1D"
L2_LAYER = "L2_signal_1H"
L3_LAYER = "L3_structure_30M"
L4_LAYER = "L4_exec_5M"
L5_LAYER = "L5_micro_1M"


@dataclass
class CascadeResult:
    run_id: str
    survivors_l1: List[str]
    survivors_l2: List[str]
    survivors_l3: List[str] = field(default_factory=list)
    survivors_l4: List[str] = field(default_factory=list)
    survivors_l5: List[str] = field(default_factory=list)


def _config_to_dict(cfg: Any) -> Dict[str, Any]:
    if hasattr(cfg, "dict"):
        try:
            return cfg.dict()
        except Exception:
            pass
    if hasattr(cfg, "model_dump"):
        try:
            return cfg.model_dump()
        except Exception:
            pass
    return dict(cfg) if isinstance(cfg, dict) else {"config": str(cfg)}


def _load_training_config(config_path: Optional[str]) -> Any:
    import importlib

    mod = importlib.import_module("octa_training.core.config")
    load_config = getattr(mod, "load_config")
    return load_config(config_path)


def _write_survivors_parquet(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    df = pd.DataFrame(list(rows))
    if df.empty:
        df = pd.DataFrame(columns=["symbol", "timeframe", "decision", "reason_json"])
    df.to_parquet(path, index=False)


def _write_metrics_parquet(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    df = pd.DataFrame(list(rows))
    if df.empty:
        df = pd.DataFrame(columns=["run_id", "layer", "symbol", "timeframe", "key", "value"])
    df.to_parquet(path, index=False)


def _max_workers() -> int:
    return max(1, int(os.cpu_count() or 2) - 1)


def _map_symbols(
    symbols: Sequence[str],
    func: Callable[[str], Any],
) -> List[Any]:
    if not symbols:
        return []
    try:
        import ray  # type: ignore

        @ray.remote
        def _call(sym: str):
            return func(sym)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=_max_workers())
        return list(ray.get([_call.remote(sym) for sym in symbols]))
    except Exception:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=_max_workers()) as pool:
            return list(pool.map(func, symbols))


def run_cascade(
    *,
    config_path: Optional[str] = None,
    resume: bool = False,
    run_id: Optional[str] = None,
    universe_limit: int = 0,
    symbols: Optional[Sequence[str]] = None,
    altdata_config_path: Optional[str] = None,
    altdata_run_id: Optional[str] = None,
    var_root: Optional[str] = None,
) -> CascadeResult:
    raise RuntimeError(
        "legacy_orchestrator_retired:octa.core.orchestration.runner.run_cascade:"
        "use_octa.foundation.control_plane.run_foundation_training"
    )

    cfg = _load_training_config(config_path)
    cfg_dict = _config_to_dict(cfg)

    run_id = run_id or new_run_id("cascade")
    var_root_path = Path(var_root) if var_root else None
    paths = ensure_run_dirs(run_id, var_root=var_root_path)
    run_root = paths["run_root"]

    write_config_snapshot(run_root, cfg_dict)

    registry = RunRegistry(get_paths(var_root_path).metrics_root / "metrics.duckdb")
    registry.record_run_start(run_id=run_id, config=cfg_dict)

    run_state = RunState.load(run_root / "run_state.json", run_id=run_id)

    failures_path = paths["reports_dir"] / "failures.md"

    try:
        raw_root = find_raw_root()
        provider = ParquetOHLCVProvider(root=raw_root)
        universe = discover_universe(limit=universe_limit)
        if symbols:
            requested = [str(s).strip().upper() for s in symbols if str(s).strip()]
            universe_by_symbol = {u.symbol: u for u in universe}
            missing = [s for s in requested if s not in universe_by_symbol]
            if missing:
                from octa_ops.autopilot.types import UniverseSymbol

                fallback: list[UniverseSymbol] = []
                for sym in missing:
                    if not provider.has_timeframe(sym, "1D"):
                        raise RuntimeError(f"Requested symbols not found in universe: {missing}")
                    parquet_paths: Dict[str, str] = {}
                    for tf in ("1D", "1H", "30M", "5M", "1M"):
                        tf_paths = provider._index.get((sym, tf), [])
                        if tf_paths:
                            parquet_paths[tf] = str(tf_paths[0])
                    fallback.append(
                        UniverseSymbol(
                            symbol=sym,
                            asset_class="unknown",
                            currency=None,
                            session="unknown",
                            source="parquet_index",
                            parquet_paths=parquet_paths,
                        )
                    )
                for sym in missing:
                    universe_by_symbol[sym] = next(u for u in fallback if u.symbol == sym)
            universe = [universe_by_symbol[s] for s in requested]
        universe_rows = [
            {
                "symbol": u.symbol,
                "asset_class": u.asset_class,
                "session": u.session,
                "source": u.source,
                "has_1d": "1D" in (u.parquet_paths or {}),
                "has_1h": "1H" in (u.parquet_paths or {}),
            }
            for u in universe
        ]
        universe_path = run_root / "universe_input.parquet"
        pd.DataFrame(universe_rows).to_parquet(universe_path, index=False)

        l1_adapter = L1GlobalAdapter()

        l1_rows: List[Dict[str, Any]] = []
        l1_survivors: List[str] = []
        metrics_rows: List[Dict[str, Any]] = []

        parquet_by_symbol = {u.symbol: (u.parquet_paths or {}).get("1D") for u in universe}
        pending_l1: List[str] = []
        for u in universe:
            status = run_state.status_for(L1_LAYER, u.symbol) if resume else None
            if resume and status in {"PASS", "FAIL", "SKIP"}:
                decision = status
                reason = (run_state.data.get("layers", {}).get(L1_LAYER, {}).get(u.symbol, {}) or {}).get("reason")
                details: Dict[str, Any] = {}
                l1_rows.append(
                    {
                        "symbol": u.symbol,
                        "timeframe": "1D",
                        "decision": decision,
                        "reason_json": json.dumps({"reason": reason, "details": details}, default=str),
                    }
                )
                if decision == "PASS":
                    l1_survivors.append(u.symbol)
                continue
            run_state.mark_symbol(layer=L1_LAYER, symbol=u.symbol, status="IN_PROGRESS")
            pending_l1.append(u.symbol)

        def _eval_l1(sym: str):
            try:
                return l1_adapter.evaluate(symbol=sym, parquet_path=parquet_by_symbol.get(sym))
            except Exception:
                return L1GlobalAdapter().evaluate(symbol=sym, parquet_path=None)

        for result in _map_symbols(pending_l1, _eval_l1):
            decision = result.decision
            reason = result.reason
            details = result.details if isinstance(result.details, dict) else {}
            run_state.mark_symbol(
                layer=L1_LAYER,
                symbol=result.symbol,
                status=decision,
                decision=decision,
                reason=reason,
            )
            metrics = details.get("metrics") if isinstance(details, dict) else None
            if isinstance(metrics, dict):
                for key, val in metrics.items():
                    try:
                        fval = float(val)
                    except Exception:
                        continue
                    emit_metric(run_id, L1_LAYER, result.symbol, "1D", f"regime.{key}", fval)
                    metrics_rows.append(
                        {
                            "run_id": run_id,
                            "layer": L1_LAYER,
                            "symbol": result.symbol,
                            "timeframe": "1D",
                            "key": f"regime.{key}",
                            "value": fval,
                        }
                    )
            l1_rows.append(
                {
                    "symbol": result.symbol,
                    "timeframe": "1D",
                    "decision": decision,
                    "reason_json": json.dumps({"reason": reason, "details": details}, default=str),
                }
            )
            if decision == "PASS":
                l1_survivors.append(result.symbol)

        registry.clear_survivors(run_id=run_id, layer=L1_LAYER)
        registry.write_survivors(run_id=run_id, layer=L1_LAYER, rows=l1_rows)
        _write_survivors_parquet(paths["survivors_dir"] / "L1_global_1D.parquet", l1_rows)

        altdata_cfg = load_altdata_config(altdata_config_path) if altdata_config_path else None
        registry_root = None
        if isinstance(altdata_cfg, dict):
            registry_root = altdata_cfg.get("cache_dir")
        alt_registry = (
            FeatureRegistry(altdata_run_id or run_id, root=str(registry_root) if registry_root else None)
            if isinstance(altdata_cfg, dict)
            else None
        )
        l2_adapter = L2SignalAdapter(provider, feature_registry=alt_registry, altdata_config=altdata_cfg)
        l3_adapter = L3StructureAdapter(provider, feature_registry=alt_registry, altdata_config=altdata_cfg)
        l4_adapter = L4ExecutionAdapter(provider)
        l5_adapter = L5MicroAdapter(provider)

        l2_rows: List[Dict[str, Any]] = []
        l2_survivors: List[str] = []
        l2_payloads: Dict[str, Dict[str, Any]] = {}

        pending_l2: List[str] = []
        for symbol in l1_survivors:
            status = run_state.status_for(L2_LAYER, symbol) if resume else None
            if resume and status in {"PASS", "FAIL", "SKIP"}:
                decision = status
                reason = (run_state.data.get("layers", {}).get(L2_LAYER, {}).get(symbol, {}) or {}).get("reason")
                l2_rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": "1H",
                        "decision": decision,
                        "reason_json": json.dumps({"reason": reason}, default=str),
                    }
                )
                if decision == "PASS":
                    l2_survivors.append(symbol)
                continue
            run_state.mark_symbol(layer=L2_LAYER, symbol=symbol, status="IN_PROGRESS")
            pending_l2.append(symbol)

        def _eval_l2(sym: str):
            return l2_adapter.evaluate(symbol=sym)

        for result in _map_symbols(pending_l2, _eval_l2):
            decision = result.decision
            reason = result.reason
            payload = result.payload if isinstance(result.payload, dict) else {}
            run_state.mark_symbol(
                layer=L2_LAYER,
                symbol=result.symbol,
                status=decision,
                decision=decision,
                reason=reason,
            )
            l2_payloads[result.symbol] = payload
            signal = payload.get("signal") if isinstance(payload, dict) else None
            if isinstance(signal, dict) and signal.get("confidence") is not None:
                emit_metric(run_id, L2_LAYER, result.symbol, "1H", "signal_confidence", float(signal["confidence"]))
                metrics_rows.append(
                    {
                        "run_id": run_id,
                        "layer": L2_LAYER,
                        "symbol": result.symbol,
                        "timeframe": "1H",
                        "key": "signal_confidence",
                        "value": float(signal["confidence"]),
                    }
                )
            metrics = payload.get("signal_metrics") if isinstance(payload, dict) else None
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    try:
                        val = float(v)
                    except Exception:
                        continue
                    emit_metric(run_id, L2_LAYER, result.symbol, "1H", f"signal_metric.{k}", val)
                    metrics_rows.append(
                        {
                            "run_id": run_id,
                            "layer": L2_LAYER,
                            "symbol": result.symbol,
                            "timeframe": "1H",
                            "key": f"signal_metric.{k}",
                            "value": val,
                        }
                    )
            l2_rows.append(
                {
                    "symbol": result.symbol,
                    "timeframe": "1H",
                    "decision": decision,
                    "reason_json": json.dumps({"reason": reason}, default=str),
                }
            )
            if decision == "PASS":
                l2_survivors.append(result.symbol)

        registry.clear_survivors(run_id=run_id, layer=L2_LAYER)
        registry.write_survivors(run_id=run_id, layer=L2_LAYER, rows=l2_rows)
        _write_survivors_parquet(paths["survivors_dir"] / "L2_signal_1H.parquet", l2_rows)

        l3_rows: List[Dict[str, Any]] = []
        l3_survivors: List[str] = []

        pending_l3: List[str] = []
        for symbol in l2_survivors:
            status = run_state.status_for(L3_LAYER, symbol) if resume else None
            if resume and status in {"PASS", "FAIL", "SKIP"}:
                decision = status
                reason = (run_state.data.get("layers", {}).get(L3_LAYER, {}).get(symbol, {}) or {}).get("reason")
                l3_rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": "30M",
                        "decision": decision,
                        "reason_json": json.dumps({"reason": reason}, default=str),
                    }
                )
                if decision == "PASS":
                    l3_survivors.append(symbol)
                continue
            run_state.mark_symbol(layer=L3_LAYER, symbol=symbol, status="IN_PROGRESS")
            pending_l3.append(symbol)

        def _eval_l3(sym: str):
            return l3_adapter.evaluate(symbol=sym)

        for result in _map_symbols(pending_l3, _eval_l3):
            decision = result.decision
            reason = result.reason
            payload = result.payload if isinstance(result.payload, dict) else {}
            run_state.mark_symbol(
                layer=L3_LAYER,
                symbol=result.symbol,
                status=decision,
                decision=decision,
                reason=reason,
            )
            metrics = payload.get("structure_metrics") if isinstance(payload, dict) else None
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    try:
                        val = float(v)
                    except Exception:
                        continue
                    emit_metric(run_id, L3_LAYER, result.symbol, "30M", f"structure_metric.{k}", val)
                    metrics_rows.append(
                        {
                            "run_id": run_id,
                            "layer": L3_LAYER,
                            "symbol": result.symbol,
                            "timeframe": "30M",
                            "key": f"structure_metric.{k}",
                            "value": val,
                        }
                    )
            l3_rows.append(
                {
                    "symbol": result.symbol,
                    "timeframe": "30M",
                    "decision": decision,
                    "reason_json": json.dumps({"reason": reason}, default=str),
                }
            )
            if decision == "PASS":
                l3_survivors.append(result.symbol)

        registry.clear_survivors(run_id=run_id, layer=L3_LAYER)
        registry.write_survivors(run_id=run_id, layer=L3_LAYER, rows=l3_rows)
        _write_survivors_parquet(paths["survivors_dir"] / "L3_structure_30M.parquet", l3_rows)

        l4_rows: List[Dict[str, Any]] = []
        l4_survivors: List[str] = []
        l4_payloads: Dict[str, Dict[str, Any]] = {}

        # Ensure signal map is available for L4 even on resume.
        for symbol in l3_survivors:
            if symbol in l2_payloads:
                continue
            try:
                l2_payloads[symbol] = l2_adapter.evaluate(symbol=symbol).payload
            except Exception:
                l2_payloads[symbol] = {}

        l4_adapter.set_signal_map(l2_payloads)
        pending_l4: List[str] = []
        for symbol in l3_survivors:
            status = run_state.status_for(L4_LAYER, symbol) if resume else None
            if resume and status in {"PASS", "FAIL", "SKIP"}:
                decision = status
                reason = (run_state.data.get("layers", {}).get(L4_LAYER, {}).get(symbol, {}) or {}).get("reason")
                l4_rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": "5M",
                        "decision": decision,
                        "reason_json": json.dumps({"reason": reason}, default=str),
                    }
                )
                if decision == "PASS":
                    l4_survivors.append(symbol)
                continue
            run_state.mark_symbol(layer=L4_LAYER, symbol=symbol, status="IN_PROGRESS")
            pending_l4.append(symbol)

        def _eval_l4(sym: str):
            return l4_adapter.evaluate(symbol=sym)

        for result in _map_symbols(pending_l4, _eval_l4):
            decision = result.decision
            reason = result.reason
            payload = result.payload if isinstance(result.payload, dict) else {}
            run_state.mark_symbol(
                layer=L4_LAYER,
                symbol=result.symbol,
                status=decision,
                decision=decision,
                reason=reason,
            )
            l4_payloads[result.symbol] = payload
            metrics = payload.get("execution_metrics") if isinstance(payload, dict) else None
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    try:
                        val = float(v)
                    except Exception:
                        continue
                    emit_metric(run_id, L4_LAYER, result.symbol, "5M", f"execution_metric.{k}", val)
                    metrics_rows.append(
                        {
                            "run_id": run_id,
                            "layer": L4_LAYER,
                            "symbol": result.symbol,
                            "timeframe": "5M",
                            "key": f"execution_metric.{k}",
                            "value": val,
                        }
                    )
            l4_rows.append(
                {
                    "symbol": result.symbol,
                    "timeframe": "5M",
                    "decision": decision,
                    "reason_json": json.dumps({"reason": reason}, default=str),
                }
            )
            if decision == "PASS":
                l4_survivors.append(result.symbol)

        registry.clear_survivors(run_id=run_id, layer=L4_LAYER)
        registry.write_survivors(run_id=run_id, layer=L4_LAYER, rows=l4_rows)
        _write_survivors_parquet(paths["survivors_dir"] / "L4_exec_5M.parquet", l4_rows)

        l5_rows: List[Dict[str, Any]] = []
        l5_survivors: List[str] = []

        # Ensure execution map is available for L5 even on resume.
        for symbol in l4_survivors:
            if symbol in l4_payloads:
                continue
            try:
                l4_payloads[symbol] = l4_adapter.evaluate(symbol=symbol).payload
            except Exception:
                l4_payloads[symbol] = {}

        l5_adapter.set_execution_map(l4_payloads)
        pending_l5: List[str] = []
        for symbol in l4_survivors:
            status = run_state.status_for(L5_LAYER, symbol) if resume else None
            if resume and status in {"PASS", "FAIL", "SKIP"}:
                decision = status
                reason = (run_state.data.get("layers", {}).get(L5_LAYER, {}).get(symbol, {}) or {}).get("reason")
                l5_rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": "1M",
                        "decision": decision,
                        "reason_json": json.dumps({"reason": reason}, default=str),
                    }
                )
                if decision == "PASS":
                    l5_survivors.append(symbol)
                continue
            run_state.mark_symbol(layer=L5_LAYER, symbol=symbol, status="IN_PROGRESS")
            pending_l5.append(symbol)

        def _eval_l5(sym: str):
            return l5_adapter.evaluate(symbol=sym)

        for result in _map_symbols(pending_l5, _eval_l5):
            decision = result.decision
            reason = result.reason
            payload = result.payload if isinstance(result.payload, dict) else {}
            run_state.mark_symbol(
                layer=L5_LAYER,
                symbol=result.symbol,
                status=decision,
                decision=decision,
                reason=reason,
            )
            metrics = payload.get("micro_metrics") if isinstance(payload, dict) else None
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    try:
                        val = float(v)
                    except Exception:
                        continue
                    emit_metric(run_id, L5_LAYER, result.symbol, "1M", f"micro_metric.{k}", val)
                    metrics_rows.append(
                        {
                            "run_id": run_id,
                            "layer": L5_LAYER,
                            "symbol": result.symbol,
                            "timeframe": "1M",
                            "key": f"micro_metric.{k}",
                            "value": val,
                        }
                    )
            l5_rows.append(
                {
                    "symbol": result.symbol,
                    "timeframe": "1M",
                    "decision": decision,
                    "reason_json": json.dumps({"reason": reason}, default=str),
                }
            )
            if decision == "PASS":
                l5_survivors.append(result.symbol)

        registry.clear_survivors(run_id=run_id, layer=L5_LAYER)
        registry.write_survivors(run_id=run_id, layer=L5_LAYER, rows=l5_rows)
        _write_survivors_parquet(paths["survivors_dir"] / "L5_micro_1M.parquet", l5_rows)

        metrics_path = paths["metrics_dir"] / "metrics.parquet"
        _write_metrics_parquet(metrics_path, metrics_rows)

        summary_path = paths["reports_dir"] / "summary.md"
        summary_path.write_text(
            "\n".join(
                [
                    f"run_id: {run_id}",
                    f"universe: {len(universe)}",
                    f"L1 survivors: {len(l1_survivors)}",
                    f"L2 survivors: {len(l2_survivors)}",
                    f"L3 survivors: {len(l3_survivors)}",
                    f"L4 survivors: {len(l4_survivors)}",
                    f"L5 survivors: {len(l5_survivors)}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        registry.record_run_end(run_id=run_id, status="COMPLETED")

        emit_event(
            run_id,
            "INFO",
            "orchestration",
            "cascade_complete",
            {
                "l1_survivors": len(l1_survivors),
                "l2_survivors": len(l2_survivors),
                "l3_survivors": len(l3_survivors),
                "l4_survivors": len(l4_survivors),
                "l5_survivors": len(l5_survivors),
            },
        )

        return CascadeResult(
            run_id=run_id,
            survivors_l1=l1_survivors,
            survivors_l2=l2_survivors,
            survivors_l3=l3_survivors,
            survivors_l4=l4_survivors,
            survivors_l5=l5_survivors,
        )
    except Exception as exc:
        failures_path.write_text(f"error: {exc}\\n", encoding="utf-8")
        registry.record_run_end(run_id=run_id, status="FAILED")
        emit_event(
            run_id,
            "ERROR",
            "orchestration",
            "cascade_failed",
            {"error": str(exc)},
        )
        raise
