from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.core.execution.ibkr.client import IBKRClient
from octa.execution.runner import ExecutionConfig, run_execution
from octa_vertex.broker.ibkr_ib_insync import IBKRIBInsyncAdapter, IBKRIBInsyncConfig


def test_run_execution_blocks_live_mode_in_foundation_scope(tmp_path: Path) -> None:
    evidence_dir = tmp_path / "evidence"
    with pytest.raises(RuntimeError, match="live_execution_blocked_in_v0_0_0_foundation_scope"):
        run_execution(
            ExecutionConfig(
                mode="live",
                enable_live=True,
                i_understand_live_risk=True,
                evidence_dir=evidence_dir,
            )
        )

    payload = json.loads((evidence_dir / "scope_enforcement.json").read_text(encoding="utf-8"))
    assert payload["blocked"] is True
    assert payload["reason"] == "foundation_scope_blocks_real_execution"


def test_run_execution_blocks_live_arming_flags_even_in_dry_run(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="live_execution_blocked_in_v0_0_0_foundation_scope"):
        run_execution(
            ExecutionConfig(
                mode="dry-run",
                enable_live=True,
                evidence_dir=tmp_path / "armed_dry_run",
            )
        )


def test_ibkr_ib_insync_adapter_is_hard_blocked() -> None:
    with pytest.raises(RuntimeError, match="real_order_blocked_in_v0_0_0_foundation_scope"):
        IBKRIBInsyncAdapter(IBKRIBInsyncConfig())


def test_ibkr_client_connect_and_order_calls_are_hard_blocked() -> None:
    client = IBKRClient()
    with pytest.raises(RuntimeError, match="real_order_blocked_in_v0_0_0_foundation_scope"):
        client.connect()
    with pytest.raises(RuntimeError, match="real_order_blocked_in_v0_0_0_foundation_scope"):
        client.place_order(object())
