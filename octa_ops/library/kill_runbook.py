from octa_ops.runbooks import StepResult


def step_confirm_kill_activation(ctx: dict) -> StepResult:
    # ensure kill-switch is recorded and ops notified
    notified = ctx.get("notify_ops", lambda: True)()
    return StepResult(
        name="confirm_kill_activation",
        success=bool(notified),
        info={"notified": notified},
    )


def step_isolate_execution(ctx: dict) -> StepResult:
    isolated = ctx.get("isolate", lambda: True)()
    return StepResult(
        name="isolate_execution", success=bool(isolated), info={"isolated": isolated}
    )
