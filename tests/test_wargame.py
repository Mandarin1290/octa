from octa_wargames.framework import WarGameFramework, WarGameResult


def sample_scenario(payload, rng):
    # nondestructive read-only test: sample deterministic modification
    base = dict(payload)
    base["random_draw"] = rng.randint(1, 100)
    return base


def test_wargame_replay_deterministic():
    fw = WarGameFramework()
    fw.register_scenario("sample", sample_scenario)

    context = {"a": 1, "b": 2}
    r1: WarGameResult = fw.run_scenario("sample", context, seed=12345)
    r2: WarGameResult = fw.replay_result(r1, context)

    assert r1.seed == r2.seed
    assert r1.scenario == r2.scenario
    assert r1.output == r2.output
    assert r1.hash == r2.hash


def test_wargame_isolation():
    fw = WarGameFramework()
    fw.register_scenario("sample", sample_scenario)

    context = {"x": []}

    def mutating_scenario(payload, rng):
        payload["x"].append(rng.randint(0, 10))
        return payload

    fw.register_scenario("mutate", mutating_scenario)

    res = fw.run_scenario("mutate", context, seed=42)
    # original context should not be mutated
    assert context["x"] == []
    # produced output should have the mutation
    assert isinstance(res.output, dict)
    assert res.output["x"] != []
