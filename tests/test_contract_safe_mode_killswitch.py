from datetime import datetime, timezone

from octa_ops.safe_mode import SafeModeManager
from octa_sentinel.kill_switch import KillSwitch


def test_safe_mode_blocks_entries_allows_exits():
    sm = SafeModeManager()
    instrument = "XYZ"
    # start with no halt
    sm.set_halt(False, actor="ops")
    # entries allowed when not halted (delta positive)
    ok, _ = sm.allow_trade(instrument, delta=1.0, trade_type="entry")
    assert ok

    # set global halt and existing position
    sm.positions[instrument] = 10.0
    sm.set_halt(True, actor="ops", reason="test")
    ok_e, reason_e = sm.allow_trade(instrument, delta=1.0, trade_type="entry")
    ok_x, reason_x = sm.allow_trade(instrument, delta=-1.0, trade_type="exit")
    assert not ok_e and reason_e == "global_halt_blocks_entries"
    assert ok_x and reason_x == "exit_reduces_exposure"


def test_kill_switch_manual_release_requires_two_operators():
    operators = {"alice": "alice-secret", "bob": "bob-secret"}
    ks = KillSwitch(operator_keys=operators)

    ks.trigger(reason="test")
    # construct a release payload timestamp we control
    ts = datetime.now(timezone.utc).isoformat()
    reason = "manual"
    payload = f"manual_release|{ts}|{reason}"
    sig1 = ks._sign(payload, operators["alice"])
    sig2 = ks._sign(payload, operators["bob"])

    # invalid single signature (bob given alice's sig) should fail
    ok_single = ks.manual_release(
        "alice", sig1, "bob", sig1, reason=reason, payload_ts=ts
    )
    assert not ok_single

    # correct two signatures should succeed
    ok = ks.manual_release("alice", sig1, "bob", sig2, reason=reason, payload_ts=ts)
    assert ok
