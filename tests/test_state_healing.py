import copy

import pytest

from octa_resilience.state_healing import (
    PersistenceSimulator,
    StateCorruptionError,
    StateManager,
)


def test_corruption_detected_and_healed():
    initial = {"positions": {"A": 100}, "cash": 100000}
    mgr = StateManager(copy.deepcopy(initial))

    # corrupt in-memory state
    mgr.state["cash"] = 0
    assert mgr.detect_corruption() is True

    # heal deterministically
    mgr.heal()
    assert mgr.state == mgr.snapshot
    assert mgr.state == initial


def test_stale_cache_refused_and_fallback():
    initial = {"positions": {"A": 100}, "cash": 100000}
    mgr = StateManager(copy.deepcopy(initial))

    # set cache to an older snapshot and mark stale
    old = {"positions": {"A": 50}, "cash": 90000}
    mgr.cache.set(old)
    mgr.cache.mark_stale()

    # read_from_cache should refuse stale cache and return snapshot
    result = mgr.read_from_cache()
    assert result == mgr.snapshot
    assert any(e["action"] == "cache_stale_refused" for e in mgr.audit_log)


def test_partial_persistence_failure_rolls_back():
    initial = {"positions": {"A": 100}, "cash": 100000}
    persistence = PersistenceSimulator()
    mgr = StateManager(copy.deepcopy(initial), persistence=persistence)

    # prepare a failing persistence that writes a partial snapshot
    persistence.fail_next_write = True
    persistence.partial_write = {"positions": {"A": 100}}  # missing cash

    new_state = {"positions": {"A": 50}, "cash": 80000}
    with pytest.raises(StateCorruptionError):
        mgr.persist_state(new_state, actor="tester")

    # ensure rollback to last snapshot
    assert mgr.state == mgr.snapshot
    assert mgr.state == initial
    assert any(e["action"] == "persistence_failed" for e in mgr.audit_log)
