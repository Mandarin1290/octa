from octa_chaos.red_team import DefenseSystem, RedTeamSimulator


def test_privilege_misuse_blocked_and_alerted():
    defense = DefenseSystem(
        baseline_privileges={"alice": ["ops"]},
        allowed_ranges={"max_trade_size": (0, 10000)},
        rate_limit_per_minute=1000,
    )
    red = RedTeamSimulator(defense)

    blocked = red.privilege_misuse("mallory", "alice", ["admin"])  # no approvals
    assert blocked is False
    # alert recorded
    assert any(
        a.action == "privilege_change" and a.reason == "insufficient_approvals"
        for a in defense.alerts
    )


def test_parameter_manipulation_blocked_for_boundary_violation():
    defense = DefenseSystem(
        baseline_privileges={"ops": ["ops"]},
        allowed_ranges={"max_trade_size": (0, 10000)},
        rate_limit_per_minute=1000,
    )
    red = RedTeamSimulator(defense)

    ok = red.parameter_manipulation("attacker", "max_trade_size", 200000)
    assert ok is False
    assert any(
        a.action == "param_change" and a.reason == "boundary_violation"
        for a in defense.alerts
    )


def test_timing_attack_detected_and_blocked():
    # low rate limit to trigger detection
    defense = DefenseSystem(
        baseline_privileges={}, allowed_ranges={}, rate_limit_per_minute=5
    )
    red = RedTeamSimulator(defense)

    # send burst larger than rate limit
    result = red.timing_attack("attacker", burst_count=10)
    assert result is False
    assert any(
        a.action == "high_frequency_actions" and a.reason == "rate_limit_exceeded"
        for a in defense.alerts
    )
