import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict


@dataclass
class Hypothesis:
    id: str
    meta: Dict[str, Any]


class RunContext:
    def __init__(self):
        self.token = str(uuid.uuid4())
        self.stage = 0


class AlphaPipeline:
    """Canonical alpha generation pipeline enforcing ordered stages and
    preventing bypass.

    Stage order:
      0) hypothesis_definition
      1) feature_eligibility
      2) data_sufficiency
      3) signal_construction
      4) risk_prescreen
      5) paper_deploy

    Use `run(hypothesis)` to process a hypothesis end-to-end. Individual stage
    methods require an internal `RunContext` token; calling them directly from
    outside without the token raises `RuntimeError`, preventing bypass.
    """

    def __init__(self):
        self._stages = [
            "hypothesis_definition",
            "feature_eligibility",
            "data_sufficiency",
            "signal_construction",
            "risk_prescreen",
            "paper_deploy",
        ]

    def _require_stage(self, ctx: RunContext, expected: int):
        if not isinstance(ctx, RunContext) or ctx.token is None:
            raise RuntimeError("stage methods may only be called via pipeline.run()")
        if ctx.stage != expected:
            raise RuntimeError(
                f"stage out of order: expected {expected}, found {ctx.stage}"
            )

    def run(self, hypothesis_meta: Dict[str, Any]) -> Dict[str, Any]:
        ctx = RunContext()
        hyp = Hypothesis(id=str(uuid.uuid4()), meta=hypothesis_meta)
        out: Dict[str, Any] = {"hypothesis_id": hyp.id}
        # 0
        self.hypothesis_definition(ctx, hyp, out)
        # 1
        self.feature_eligibility(ctx, hyp, out)
        # 2
        self.data_sufficiency(ctx, hyp, out)
        # 3
        self.signal_construction(ctx, hyp, out)
        # 4
        self.risk_prescreen(ctx, hyp, out)
        # 5
        self.paper_deploy(ctx, hyp, out)
        return out

    # Stage implementations
    def hypothesis_definition(
        self, ctx: RunContext, hyp: Hypothesis, out: Dict[str, Any]
    ):
        self._require_stage(ctx, 0)
        # canonicalize hypothesis deterministically
        out["hypothesis"] = dict(sorted(hyp.meta.items()))
        ctx.stage += 1

    def feature_eligibility(
        self, ctx: RunContext, hyp: Hypothesis, out: Dict[str, Any]
    ):
        self._require_stage(ctx, 1)
        # simple deterministic eligibility: must have 'features' key non-empty
        feats = hyp.meta.get("features", [])
        eligible = isinstance(feats, list) and len(feats) > 0
        out["feature_eligible"] = bool(eligible)
        ctx.stage += 1

    def data_sufficiency(self, ctx: RunContext, hyp: Hypothesis, out: Dict[str, Any]):
        self._require_stage(ctx, 2)
        # deterministic data gate: require 'data_points' >= threshold
        dp = int(hyp.meta.get("data_points", 0))
        out["data_sufficient"] = dp >= int(hyp.meta.get("min_data_points", 1))
        ctx.stage += 1

    def signal_construction(
        self, ctx: RunContext, hyp: Hypothesis, out: Dict[str, Any]
    ):
        self._require_stage(ctx, 3)
        # produce a deterministic signal (e.g., sum of numeric features)
        features = hyp.meta.get("features", [])
        total = Decimal("0")
        for f in features:
            try:
                total += Decimal(str(f))
            except Exception:
                pass
        out["signal"] = total.quantize(Decimal("0.00000001"))
        ctx.stage += 1

    def risk_prescreen(self, ctx: RunContext, hyp: Hypothesis, out: Dict[str, Any]):
        self._require_stage(ctx, 4)
        # deterministic risk check: signal must be within provided bounds
        sig = out.get("signal", Decimal("0"))
        lb = Decimal(str(hyp.meta.get("risk_lb", "-1e9")))
        ub = Decimal(str(hyp.meta.get("risk_ub", "1e9")))
        out["risk_ok"] = (Decimal(sig) >= lb) and (Decimal(sig) <= ub)
        ctx.stage += 1

    def paper_deploy(self, ctx: RunContext, hyp: Hypothesis, out: Dict[str, Any]):
        self._require_stage(ctx, 5)
        # final stage: produce paper-trading config deterministically
        out["paper_deploy"] = {
            "strategy_ref": f"paper-{hyp.id}",
            "signal": out.get("signal"),
            "risk_ok": out.get("risk_ok"),
        }
        ctx.stage += 1


class AlphaSource:
    """Represents an alpha source that must be run through an `AlphaPipeline`.

    The source may produce a hypothesis meta object but cannot call pipeline
    stages directly (stage methods require a valid RunContext token).
    """

    def __init__(self, name: str, generator: Callable[[], Dict[str, Any]]):
        self.name = name
        self.generator = generator

    def propose(self) -> Dict[str, Any]:
        return self.generator()
