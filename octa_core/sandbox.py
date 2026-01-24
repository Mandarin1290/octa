from __future__ import annotations

import multiprocessing
import traceback
from multiprocessing import Pipe
from typing import Any, Callable, Optional

from .strategy import StrategyInput, StrategyOutput, StrategySpec


def _worker_run(fn, spec, inp, conn, mem_bytes: int | None):
    try:
        # apply memory limit on POSIX
        try:
            import resource

            if mem_bytes is not None:
                resource.setrlimit(resource.RLIMIT_AS, (mem_bytes, mem_bytes))
        except Exception:
            pass

        out = fn(spec, inp, {})
        # normalize output
        if hasattr(out, "to_dict"):
            outd = out.to_dict()
        else:
            outd = out
        conn.send((True, outd))
    except Exception as e:
        tb = traceback.format_exc()
        conn.send((False, {"error": str(e), "trace": tb}))


class StrategySandbox:
    def __init__(self, timeout_sec: float = 2.0, memory_mb: int = 200):
        self.timeout = timeout_sec
        self.memory_mb = memory_mb

    def run(
        self,
        fn: Callable,
        spec: StrategySpec,
        inp: StrategyInput,
        ledger_api: Optional[Any] = None,
    ) -> StrategyOutput:
        parent_conn, child_conn = Pipe()
        mem_bytes = self.memory_mb * 1024 * 1024
        # Prefer 'fork' context to avoid pickling issues with local callables

        ctx: Any = multiprocessing.get_context()
        try:
            ctx = multiprocessing.get_context("fork")
        except (ValueError, RuntimeError):
            ctx = multiprocessing.get_context()

        p = ctx.Process(
            target=_worker_run, args=(fn, spec, inp, child_conn, mem_bytes), daemon=True
        )
        p.start()
        p.join(self.timeout)
        if p.is_alive():
            p.terminate()
            raise TimeoutError("Strategy execution exceeded time limit")
        if not parent_conn.poll():
            raise RuntimeError("No response from strategy process")
        ok, payload = parent_conn.recv()
        if not ok:
            raise RuntimeError(f"Strategy error: {payload}")

        # basic validation
        if not isinstance(payload, dict):
            raise ValueError("Strategy output must be a dict-like or StrategyOutput")

        # reject forbidden keys (orders, broker, execution)
        forbidden = {"orders", "broker", "execution"}
        if any(k in payload for k in forbidden):
            raise ValueError("Strategy must not emit orders or execution directives")

        ex = payload.get("exposures")
        if ex is None or not isinstance(ex, dict):
            raise ValueError("Strategy output must include exposures dict")

        # exposures must be within [-1,1]
        for k, v in ex.items():
            if not isinstance(v, (int, float)):
                raise ValueError("Exposure values must be numeric")
            if v < -1.0 or v > 1.0:
                raise ValueError("Exposure values must be between -1 and 1")
            if k not in spec.universe:
                raise ValueError("Exposure symbol not in strategy universe")

        conf = payload.get("confidence")
        if conf is None or not (0.0 <= float(conf) <= 1.0):
            raise ValueError("Confidence must be between 0 and 1")

        out = StrategyOutput(
            exposures=ex, confidence=float(conf), rationale=payload.get("rationale", {})
        )

        # audit inputs/outputs if ledger provided
        if ledger_api is not None:
            try:
                ledger_api.audit_or_fail(
                    actor="sandbox", action="strategy_input", payload=inp.to_dict()
                )
                ledger_api.audit_or_fail(
                    actor="sandbox", action="strategy_output", payload=out.to_dict()
                )
            except Exception as e:
                raise RuntimeError(f"Audit failed: {e}") from e

        return out
