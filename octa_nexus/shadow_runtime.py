from typing import Any, Dict


class ShadowRuntime:
    """Manage runtime-wide shadow mode configuration and metrics.

    Simple coordinator that holds the global `shadow_mode` flag and exposes helpers
    used by other runtime components.
    """

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = dict(config or {})
        self.config.setdefault("shadow_mode", True)
        self.metrics = {"shadow_pnl": 0.0, "paper_pnl": 0.0}

    def enabled(self) -> bool:
        return bool(self.config.get("shadow_mode", False))

    def update_metrics(self, shadow_delta: float = 0.0, paper_delta: float = 0.0):
        self.metrics["shadow_pnl"] += float(shadow_delta)
        self.metrics["paper_pnl"] += float(paper_delta)

    def snapshot(self) -> Dict[str, Any]:
        return dict(self.metrics)
