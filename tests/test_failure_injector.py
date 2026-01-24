from octa_chaos.failure_injector import FailureInjector


class DummySubsystem:
    def __init__(self, name, live=False):
        self.name = name
        self.live = live
        self.restarted = False

    def restart(self):
        self.restarted = True

    def restart_complete(self):
        self.restarted = False

    def keys(self):
        return ["a", "b"]


def test_failure_isolated_from_live():
    injector = FailureInjector(seed=42)
    live = DummySubsystem("live", live=True)
    nonlive = DummySubsystem("test", live=False)
    targets = {"live": live, "test": nonlive}

    # try to inject restart into both; live should be refused
    res = injector.random_inject(targets, ["restart"], max_events=2)
    # at least one record should exist only for non-live
    assert any(r.target == "test" for r in res)
    assert all(r.target != "live" for r in res)

    # ensure nonlive was marked restarted
    assert getattr(nonlive, "_restarted", False) is True

    # recover
    injector.recover_all(targets)
    assert not hasattr(nonlive, "_restarted")


def test_recovery_confirmed():
    injector = FailureInjector(seed=1)
    s = DummySubsystem("x", live=False)
    targets = {"x": s}
    injector.inject_delay(s, "x", 0.5)
    assert hasattr(s, "_delayed_for")
    injector.recover_all(targets)
    assert not hasattr(s, "_delayed_for")
