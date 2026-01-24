from octa_ops.runbooks import StepResult


def step_notify_broker_disconnect(ctx: dict) -> StepResult:
    # example step: notify ops and attempt reconnect (simulated)
    ops_contacted = ctx.get("notify_ops", lambda: True)()
    return StepResult(
        name="notify_broker_disconnect",
        success=bool(ops_contacted),
        info={"notified": ops_contacted},
    )


def step_reconnect_broker(ctx: dict) -> StepResult:
    reconnect = ctx.get("reconnect", lambda: True)()
    return StepResult(
        name="reconnect_broker",
        success=bool(reconnect),
        info={"reconnected": reconnect},
    )
