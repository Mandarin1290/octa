from pathlib import Path

from octa.core.orchestration.state import RunState


def test_run_state_resume(tmp_path: Path) -> None:
    state_path = tmp_path / "run_state.json"
    state = RunState.load(state_path, run_id="r1")
    assert state.status_for("L1", "AAA") is None

    state.mark_symbol(layer="L1", symbol="AAA", status="PASS", decision="PASS", reason="ok")
    reloaded = RunState.load(state_path, run_id="r1")

    assert reloaded.status_for("L1", "AAA") == "PASS"
    assert reloaded.is_complete("L1", "AAA")

