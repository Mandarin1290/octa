from octa_ops.runbooks import StepResult


def step_verify_feed(ctx: dict) -> StepResult:
    ok = ctx.get("feed_ok", False)
    return StepResult(name="verify_feed", success=bool(ok), info={"feed_ok": ok})


def step_switch_to_failover(ctx: dict) -> StepResult:
    switched = ctx.get("switch_failover", lambda: True)()
    return StepResult(
        name="switch_to_failover", success=bool(switched), info={"switched": switched}
    )
