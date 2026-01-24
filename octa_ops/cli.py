from typing import Any, Dict, Optional

from octa_ops.operators import ActionRegistry, OperatorRegistry


class OperatorCLI:
    """Simple CLI interface for operators to execute predefined actions.

    For dangerous actions, two operator signatures are required; the second operator id
    must be provided in `ctx["second_operator"]` and `signature2`.
    """

    def __init__(self, operators: OperatorRegistry, actions: ActionRegistry):
        self.operators = operators
        self.actions = actions

    def execute_command(
        self,
        operator_id: str,
        action_name: str,
        ctx: Dict[str, Any],
        signature: Optional[str] = None,
        signature2: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.actions.execute(
            operator_id, action_name, ctx, signature=signature, signature2=signature2
        )
