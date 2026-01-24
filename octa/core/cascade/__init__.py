"""Core cascade orchestration package."""

from .adapters import (
    ExecutionGateAdapter,
    GlobalRegimeGateAdapter,
    MicroGateAdapter,
    SafeNoopGate,
    SignalGateAdapter,
    StructureGateAdapter,
)
from .contracts import GateDecision, GateOutcome, GateSpec, GateInterface
from .context import CascadeContext
from .controller import CascadeController
from .policies import CascadePolicy, DEFAULT_TIMEFRAMES
from .registry import build_default_gate_stack

__all__ = [
    "GateDecision",
    "GateOutcome",
    "GateSpec",
    "GateInterface",
    "CascadeContext",
    "CascadeController",
    "CascadePolicy",
    "DEFAULT_TIMEFRAMES",
    "GlobalRegimeGateAdapter",
    "StructureGateAdapter",
    "SignalGateAdapter",
    "ExecutionGateAdapter",
    "MicroGateAdapter",
    "SafeNoopGate",
    "build_default_gate_stack",
]
