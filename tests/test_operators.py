from octa_ops.cli import OperatorCLI
from octa_ops.operators import Action, ActionRegistry, Operator, OperatorRegistry, Role


def test_permission_enforcement_and_dual_control():
    events = []

    def audit(e, p):
        events.append((e, p))

    ops = OperatorRegistry(audit_fn=audit)
    # register operators
    ops.register(Operator(operator_id="viewer", role=Role.VIEW, key="k_view"))
    ops.register(Operator(operator_id="inc", role=Role.INCIDENT, key="k_inc"))
    ops.register(Operator(operator_id="em1", role=Role.EMERGENCY, key="k_em1"))
    ops.register(Operator(operator_id="em2", role=Role.EMERGENCY, key="k_em2"))

    def harmless(ctx):
        return {"ok": True}

    def dangerous(ctx):
        return {"done": True}

    reg = ActionRegistry(operator_registry=ops, audit_fn=audit)
    reg.register_action(
        Action(
            name="view_stats",
            handler=harmless,
            allowed_roles=[Role.VIEW, Role.INCIDENT, Role.EMERGENCY],
            dangerous=False,
        )
    )
    reg.register_action(
        Action(
            name="unlock_kill",
            handler=dangerous,
            allowed_roles=[Role.EMERGENCY],
            dangerous=True,
        )
    )

    cli = OperatorCLI(operators=ops, actions=reg)

    # viewer cannot run unlock_kill
    payload = "unlock_kill|viewer|now"
    sig = ops.sign("viewer", payload)
    r = cli.execute_command("viewer", "unlock_kill", {}, signature=sig)
    assert r["ok"] is False
    assert r["error"] == "permission_denied"

    # emergency operator requires second signature
    ts_payload = "unlock_kill|em1|now"
    sig1 = ops.sign("em1", ts_payload)
    sig2 = ops.sign("em2", ts_payload)
    r2 = cli.execute_command(
        "em1",
        "unlock_kill",
        {"second_operator": "em2", "payload": ts_payload},
        signature=sig1,
        signature2=sig2,
    )
    assert r2["ok"] is True
