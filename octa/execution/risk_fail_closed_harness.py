from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from .runner import ExecutionConfig, run_execution


def _stage(tf: str) -> Dict[str, Any]:
    return {"timeframe": tf, "status": "PASS", "metrics_summary": {"n_trades": 1, "sharpe": 1.0}}


def _prepare_minimal_training_evidence(base_dir: Path) -> None:
    run = base_dir / "run_gate"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    (run / "results").mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": "SYM",
        "asset_class": "equities",
        "stages": [_stage("1D"), _stage("1H"), _stage("30M"), _stage("5M"), _stage("1M")],
    }
    (run / "results" / "SYM.json").write_text(json.dumps(payload), encoding="utf-8")


def run_case(*, simulated_case: str, root_dir: Path) -> Dict[str, Any]:
    if simulated_case not in {"exception", "invalid_return"}:
        raise ValueError("simulated_case must be one of: exception, invalid_return")

    base_evidence = root_dir / "base_evidence"
    exec_evidence = root_dir / f"exec_{simulated_case}"
    _prepare_minimal_training_evidence(base_evidence)

    calls = {"broker_router_called": False}

    def _fake_place_order(self, *, strategy, order):  # type: ignore[no-untyped-def]
        calls["broker_router_called"] = True
        return {"status": "SIMULATED", "strategy": strategy, "order_id": str(order.get("order_id", ""))}

    old = os.environ.get("OCTA_TEST_RISK_FAIL_CLOSED_CASE")
    os.environ["OCTA_TEST_RISK_FAIL_CLOSED_CASE"] = simulated_case
    try:
        with patch("octa.execution.broker_router.BrokerRouter.place_order", new=_fake_place_order):
            summary = run_execution(
                ExecutionConfig(
                    mode="dry-run",
                    evidence_dir=exec_evidence,
                    base_evidence_dir=base_evidence,
                    max_symbols=1,
                    max_cycles=1,
                    loop=False,
                )
            )
    finally:
        if old is None:
            os.environ.pop("OCTA_TEST_RISK_FAIL_CLOSED_CASE", None)
        else:
            os.environ["OCTA_TEST_RISK_FAIL_CLOSED_CASE"] = old

    incidents = sorted((exec_evidence / "risk_incidents").glob("*.json"))
    incident_payload: Dict[str, Any] = {}
    if incidents:
        incident_payload = json.loads(incidents[0].read_text(encoding="utf-8"))

    blocked = bool(summary.get("blocks", 0) >= 1)
    broker_called = bool(calls["broker_router_called"])
    return {
        "simulated_case": simulated_case,
        "blocked": blocked,
        "broker_router_called": broker_called,
        "fail_closed_enforced": blocked and not broker_called,
        "stack_or_error": str(incident_payload.get("stack_or_error", "")),
        "incident_path": str(incidents[0]) if incidents else "",
        "sha256_path": str(incidents[0].with_suffix(incidents[0].suffix + ".sha256")) if incidents else "",
        "execution_evidence_dir": str(exec_evidence),
    }

