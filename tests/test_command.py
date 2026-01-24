from datetime import datetime, timedelta, timezone

from octa_ops.command import CommandManager
from octa_ops.incidents import IncidentManager, Severity


class FixedClock:
    def __init__(self, start: datetime):
        self._now = start

    def now(self):
        return self._now

    def advance(self, seconds: int):
        self._now = self._now + timedelta(seconds=seconds)


def test_single_authority_enforced():
    im = IncidentManager()
    inc = im.record_incident("x", "desc", "alice", severity=Severity.S2)
    clock = FixedClock(datetime.now(timezone.utc))
    cm = CommandManager(im, role_to_user={"oncall_engineer": "bob"}, now_fn=clock.now)
    cm.start_command(inc.id, initial_timeout=60)
    cm.assign_commander(
        inc.id, commander="alice", role="oncall_engineer", actor="alice"
    )
    # try assign another commander without override
    try:
        cm.assign_commander(
            inc.id, commander="charlie", role="oncall_engineer", actor="charlie"
        )
        raise AssertionError("expected RuntimeError")
    except RuntimeError:
        pass


def test_escalation_occurs_on_timeout():
    im = IncidentManager()
    inc = im.record_incident("y", "desc", "dave", severity=Severity.S2)
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(start)
    cm = CommandManager(
        im,
        role_to_user={"trading_desk_lead": "lead1", "oncall_engineer": "eng1"},
        now_fn=clock.now,
    )
    cm.start_command(inc.id, initial_timeout=10)
    cm.assign_commander(inc.id, commander="eng1", role="oncall_engineer", actor="eng1")
    # advance beyond timeout
    clock.advance(11)
    actions = cm.check_escalations()
    assert len(actions) >= 1
    assert (
        actions[0]["new_commander"].startswith("lead")
        or actions[0]["role"] == "trading_desk_lead"
    )
