from datetime import timedelta

import pytest

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.state_machine import LifecycleState, TransitionError


def test_illegal_transition_blocked():
    s = StrategyLifecycle("S1")
    # attempt to skip to LIVE directly from IDEA
    with pytest.raises(TransitionError):
        s.transition_to(LifecycleState.LIVE, doc="Attempted direct go-live")


def test_lifecycle_enforced_before_execution():
    s = StrategyLifecycle("S2")
    # IDEA -> SHADOW (shadow before paper — governance requirement)
    s.transition_to(LifecycleState.SHADOW, doc="Shadow run parameters")
    assert s.current_state == LifecycleState.SHADOW
    with pytest.raises(TransitionError):
        s.assert_can_execute()

    # proceed through normal flow: SHADOW -> PAPER -> LIVE
    s.transition_to(LifecycleState.PAPER, doc="Paper testing plan")
    s.transition_to(LifecycleState.LIVE, doc="Go live approval")
    assert s.can_execute() is True


def test_time_in_state_and_docs():
    s = StrategyLifecycle("S3")
    s.transition_to(LifecycleState.SHADOW, doc="Doc1")
    # time in state should be a small timedelta
    ti = s.time_in_state()
    assert isinstance(ti, timedelta)
    assert s.require_documentation(LifecycleState.SHADOW) == "Doc1"
