from octa_wargames.scoring import ScoringEngine, SimulationRun


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


def test_metrics_deterministic():
    runs = [make_run("a", 10, 100, 2, 2, 2), make_run("b", 5, 50, 1, 1, 1)]
    engine = ScoringEngine()
    m1 = engine.compute_metrics(runs)
    m2 = engine.compute_metrics(list(runs))
    assert m1 == m2
    # hash deterministic
    assert m1["hash"] == m2["hash"]


def test_scoring_consistent():
    engine = ScoringEngine()
    good = [make_run("g1", 1, 10, 0, 1, 1)]
    bad = [make_run("b1", 100, 1000, 10, 5, 0)]
    sg = engine.score(good)
    sb = engine.score(bad)
    assert sg > sb
