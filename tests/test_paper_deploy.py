from decimal import Decimal

from octa_alpha.paper_deploy import LifecycleEngine, PaperDeploymentManager


def test_lifecycle_state_correct_and_reproducible():
    le = LifecycleEngine()
    events = []

    def audit(event, payload):
        events.append((event, payload))

    mgr = PaperDeploymentManager(le, audit_fn=audit)
    hyp = "hyp-123"
    sig = Decimal("0.5")
    capital = Decimal("100000")
    rec = mgr.deploy(hyp, sig, capital)
    assert rec.state == "PAPER"
    # compare deterministic 8-decimal representation
    assert rec.capital == Decimal(f"{capital:.8f}")
    # reproducible: deploying again with same inputs returns same id in lifecycle
    rec2 = mgr.deploy(hyp, sig, capital)
    assert rec.id == rec2.id
    # audit event emitted
    assert any(
        e[0] == "paper.deployed" and e[1]["hypothesis_id"] == hyp for e in events
    )
