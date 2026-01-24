from octa_ops.runbooks import StepResult


def step_assess_drawdown(ctx: dict) -> StepResult:
    drawdown = ctx.get("drawdown", 0.0)
    needs_action = drawdown > 0.1
    return StepResult(
        name="assess_drawdown",
        success=True,
        info={"drawdown": drawdown, "needs_action": needs_action},
    )


def step_execute_drawdown_playbook(ctx: dict) -> StepResult:
    executed = ctx.get("execute_playbook", lambda: True)()
    return StepResult(
        name="execute_drawdown_playbook",
        success=bool(executed),
        info={"executed": executed},
    )
