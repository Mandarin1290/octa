from datetime import datetime, timedelta

from octa_reg.record_retention import RetentionManager


def test_premature_deletion_blocked():
    now = datetime(2025, 1, 1, 0, 0, 0)
    rm = RetentionManager(now_fn=lambda: now)
    rec = rm.create_record("logs", {"msg": "x"}, retention_days=30, actor="u")

    # attempt delete before expiry should raise
    try:
        rm.attempt_delete(rec.id, justification="cleanup", actor="u")
        raise AssertionError("expected RuntimeError for premature deletion")
    except RuntimeError:
        pass

    # record still present and not deleted
    r2 = rm.get_record(rec.id)
    assert r2.deleted is False


def test_retention_expiry_enforced_and_purge():
    # start at time 0, create record, advance time beyond retention and delete
    base = datetime(2025, 1, 1, 0, 0, 0)
    times = {"t": base}

    def now():
        return times["t"]

    rm = RetentionManager(now_fn=now)
    rec = rm.create_record("tx", {"amt": 100}, retention_days=1, actor="u")

    # advance time by 2 days -> expired
    times["t"] = base + timedelta(days=2)
    # now deletion should succeed (mark deleted)
    rm.attempt_delete(rec.id, justification="retention_expired", actor="u")
    r3 = rm.get_record(rec.id)
    assert r3.deleted is True

    # purge should remove the record permanently
    purged = rm.purge_expired()
    assert rec.id in purged
