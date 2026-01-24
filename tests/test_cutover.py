import pytest

from octa_core.cutover import CutoverManager, IrreversibleError


def test_cutover_sequence_and_gating():
    m = CutoverManager()
    assert m.get_state() == m.STATE_PAPER

    # cannot go to LIVE from PAPER
    with pytest.raises(Exception):
        m.transition_to_live()

    # advance to SHADOW
    s = m.transition_to_shadow()
    assert s == m.STATE_SHADOW

    # final checks must be run and pass before live
    def bad_checks():
        return {"ok": False, "reason": "not_ready"}

    m.run_final_checks(bad_checks)
    with pytest.raises(Exception):
        m.transition_to_live()

    # run passing checks
    def good_checks():
        return {"ok": True, "detail": "all good"}

    res = m.run_final_checks(good_checks)
    assert res["ok"] is True
    l = m.transition_to_live()
    assert l == m.STATE_LIVE
    assert m.is_live() is True
    assert m.capital_unlocked is True


def test_irreversible_switch():
    m = CutoverManager()
    m.transition_to_shadow()

    def ok():
        return {"ok": True}

    m.run_final_checks(ok)
    m.transition_to_live()
    # attempt revert must raise and be explicit
    with pytest.raises(IrreversibleError):
        m.attempt_revert()
