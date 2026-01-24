from __future__ import annotations

from datetime import datetime
from pathlib import Path

from octa.core.governance.audit_chain import AuditChain
from octa.core.governance.hashing import stable_hash
from octa.core.governance.kill_switch import KillSwitchConfig, KillSwitchState, evaluate_kill_switch


def test_audit_chain_detects_tamper(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    chain = AuditChain(ledger)

    chain.append({"event": "one"}, ts=datetime(2024, 1, 1))
    chain.append({"event": "two"}, ts=datetime(2024, 1, 2))
    assert chain.verify() is True

    lines = ledger.read_text(encoding="utf-8").splitlines()
    tampered = lines[1].replace("two", "X")
    ledger.write_text("\n".join([lines[0], tampered]) + "\n", encoding="utf-8")

    assert chain.verify() is False


def test_kill_switch_triggers() -> None:
    state = KillSwitchState(
        execution_failures=3,
        slippage=0.0,
        daily_loss=0.0,
        system_health=1.0,
    )
    decision = evaluate_kill_switch(state, KillSwitchConfig())
    assert decision.triggered is True
    assert decision.reason == "EXECUTION_FAILURES"


def test_stable_hash_deterministic() -> None:
    payload = {"a": 1, "b": 2}
    assert stable_hash(payload) == stable_hash({"b": 2, "a": 1})
