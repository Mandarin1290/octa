from __future__ import annotations

from octa_core.control_plane.safety_stop import ExecutionFacade, safe_stop
from octa_core.kill_switch import kill_switch


class MockExec(ExecutionFacade):
    def __init__(self):
        self.cancelled = 0
        self.flattened = 0
        self.hedged = 0

    def cancel_open_orders(self) -> None:
        self.cancelled += 1

    def flatten_positions(self) -> None:
        self.flattened += 1

    def hedge_to_neutral(self) -> None:
        self.hedged += 1


def test_safe_stop_cancels_and_optionally_flattens(tmp_path):
    audit = tmp_path / "audit.jsonl"
    ex = MockExec()

    # Ensure isolation from other tests.
    kill_switch.clear(actor_role="admin", actor="tests")

    res = safe_stop(mode="SAFE", exec_api=ex, audit_path=str(audit), flatten_positions=True)
    assert res["cancelled"] is True
    assert res["flattened"] is True
    assert ex.cancelled == 1
    assert ex.flattened == 1

    kill_switch.clear(actor_role="admin", actor="tests")


def test_immediate_stop_only_cancels(tmp_path):
    audit = tmp_path / "audit.jsonl"
    ex = MockExec()

    kill_switch.clear(actor_role="admin", actor="tests")

    res = safe_stop(mode="IMMEDIATE", exec_api=ex, audit_path=str(audit), flatten_positions=True)
    assert res["cancelled"] is True
    assert res["flattened"] is False
    assert ex.cancelled == 1
    assert ex.flattened == 0

    kill_switch.clear(actor_role="admin", actor="tests")
