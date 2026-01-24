from octa_reports.wargame_dashboard import WarGameDashboard
from octa_wargames.scoring import SimulationRun


def make_run(id, loss, recovery, incidents, activated, success):
    return SimulationRun(
        id=str(id),
        loss=loss,
        recovery_time=recovery,
        incident_count=incidents,
        gates_activated=activated,
        gates_success=success,
        meta={},
    )


def test_dashboard_reconciles_with_results():
    d = WarGameDashboard()
    d.start_simulation("s1", {"name": "run1"})
    r1 = make_run("s1", 0.1, 10.0, 1, 1, 1)
    d.finalize_simulation("s1", r1)
    # cannot finalize twice
    try:
        d.finalize_simulation("s1", r1)
        raise AssertionError("expected error")
    except ValueError:
        pass

    d.start_simulation("s2", {"name": "run2"})
    r2 = make_run("s2", 10.0, 200.0, 5, 2, 0)
    d.finalize_simulation("s2", r2)

    outcomes = d.scenario_outcomes()
    assert "s1" in outcomes and "s2" in outcomes

    score = d.resilience_score()
    # score should be numeric and finite
    assert isinstance(score, float)

    weaknesses = d.uncovered_weaknesses(gate_success_threshold=0.5, loss_threshold=1.0)
    # s2 should appear as weakness due to high loss and low gate success
    assert any(w["sim_id"] == "s2" for w in weaknesses)
