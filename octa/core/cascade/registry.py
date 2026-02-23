from __future__ import annotations

import importlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from .adapters import (
    ExecutionGateAdapter,
    GlobalRegimeGateAdapter,
    MicroGateAdapter,
    SafeNoopGate,
    SignalGateAdapter,
    StructureGateAdapter,
)
from .contracts import GateInterface
from octa.core.data.providers.ohlcv import OHLCVProvider

logger = logging.getLogger(__name__)


def _try_build_gate(
    path: str, adapter: Callable[[Any], GateInterface], ohlcv_provider: OHLCVProvider | None
) -> GateInterface | None:
    try:
        module_name, attr = path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        gate_cls = getattr(module, attr)
        if ohlcv_provider is not None:
            try:
                return adapter(delegate=gate_cls(ohlcv_provider=ohlcv_provider))
            except TypeError:
                return adapter(delegate=gate_cls())
        return adapter(delegate=gate_cls())
    except Exception as exc:
        logger.info("cascade_gate_missing", extra={"path": path, "error": str(exc)})
        return None


def _try_build_gate_paths(
    paths: Iterable[str],
    adapter: Callable[[Any], GateInterface],
    ohlcv_provider: OHLCVProvider | None,
) -> GateInterface | None:
    for path in paths:
        built = _try_build_gate(path, adapter, ohlcv_provider)
        if built is not None:
            return built
    return None


def build_default_gate_stack(ohlcv_provider: OHLCVProvider | None = None) -> list[GateInterface]:
    defaults: list[GateInterface] = []

    gates: Sequence[tuple[Sequence[str], Callable[[Any], GateInterface], str, str]] = (
        (
            (
                "octa.core.gates.global_regime.gate.GlobalRegimeGate",
                "octa.core.gates.global_gate.GlobalRegimeGate",
            ),
            GlobalRegimeGateAdapter,
            "global_regime",
            "1D",
        ),
        (
            (
                "octa.core.gates.structure_filter.gate.StructureGate",
                "octa.core.gates.structure_gate.StructureGate",
            ),
            StructureGateAdapter,
            "structure",
            "30M",
        ),
        (
            (
                "octa.core.gates.signal_engine.gate.SignalGate",
                "octa.core.gates.signal_gate.SignalGate",
            ),
            SignalGateAdapter,
            "signal",
            "1H",
        ),
        (
            (
                "octa.core.gates.execution_engine.gate.ExecutionGate",
                "octa.core.gates.execution_gate.ExecutionGate",
            ),
            ExecutionGateAdapter,
            "execution",
            "5M",
        ),
        (
            (
                "octa.core.gates.micro_optimization.gate.MicroGate",
                "octa.core.gates.micro_gate.MicroGate",
            ),
            MicroGateAdapter,
            "micro",
            "1M",
        ),
    )

    noop_entries: list[dict] = []

    for paths, adapter, name, timeframe in gates:
        built = _try_build_gate_paths(paths, adapter, ohlcv_provider)
        if built is None:
            defaults.append(SafeNoopGate(name=name, timeframe=timeframe))
            logger.warning(
                "cascade_noop_gate_inserted",
                extra={"gate": name, "timeframe": timeframe, "paths_tried": list(paths)},
            )
            noop_entries.append({"gate": name, "timeframe": timeframe, "paths_tried": list(paths)})
            continue
        defaults.append(built)

    if noop_entries:
        _write_noop_artifact(noop_entries)

    return defaults


def _write_noop_artifact(entries: list[dict]) -> None:
    """Write noop_gates.json to the run evidence directory if configured."""
    out_dir = os.environ.get("OCTA_CASCADE_RUN_DIR", "")
    if not out_dir:
        return
    try:
        p = Path(out_dir) / "noop_gates.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps({"noop_gates_present": True, "gates": entries}, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("cascade_noop_artifact_write_failed", extra={"error": str(exc)})
