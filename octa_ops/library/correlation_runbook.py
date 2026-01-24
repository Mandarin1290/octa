from octa_ops.runbooks import StepResult


def step_analyze_corr_shock(ctx: dict) -> StepResult:
    severity = ctx.get("severity", 0.0)
    action = severity > 0.5
    return StepResult(
        name="analyze_corr_shock",
        success=True,
        info={"severity": severity, "action": action},
    )


def step_apply_risk_reduction(ctx: dict) -> StepResult:
    reduced = ctx.get("reduce_risk", lambda: True)()
    return StepResult(
        name="apply_risk_reduction", success=bool(reduced), info={"reduced": reduced}
    )
