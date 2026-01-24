from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore
from octa_sentinel.paper_gates import PaperGates


def test_failing_metric_blocks_promotion(tmp_path):
    lp = tmp_path / "ledger"
    ls = LedgerStore(str(lp))

    # insufficient nav days (only 10 days)
    for i in range(10):
        d = f"2025-01-{i + 1:02d}T00:00:00+00:00"
        ev = AuditEvent.create(
            actor="perf",
            action="performance.nav",
            payload={"date": d, "nav": 100.0 - i},
        )
        ls.append(ev)

    # add slippage forecast/observed minimal to avoid that gate
    ev = AuditEvent.create(
        actor="sim", action="slippage.forecast", payload={"slippage": 0.001}
    )
    ls.append(ev)
    ev = AuditEvent.create(
        actor="sim", action="slippage.observed", payload={"slippage": 0.002}
    )
    ls.append(ev)

    # no incidents

    pg = PaperGates(min_trading_days=60)
    res = pg.evaluate_promotion(ls)
    assert not res["passed"]
    # ledger must have evaluation event
    evs = ls.by_action("paper_gate.evaluation")
    assert evs


def test_audit_proof_exists_and_blocks_on_incident(tmp_path):
    lp = tmp_path / "ledger2"
    ls = LedgerStore(str(lp))

    # create enough nav days
    for i in range(70):
        d = f"2025-01-{(i % 30) + 1:02d}T00:00:00+00:00"
        ev = AuditEvent.create(
            actor="perf",
            action="performance.nav",
            payload={"date": d, "nav": 100.0 + i},
        )
        ls.append(ev)

    # slippage data present
    ev = AuditEvent.create(
        actor="sim", action="slippage.forecast", payload={"slippage": 0.001}
    )
    ls.append(ev)
    ev = AuditEvent.create(
        actor="sim", action="slippage.observed", payload={"slippage": 0.0012}
    )
    ls.append(ev)

    # create a critical incident (severity 2) without resolution
    inc = AuditEvent.create(
        actor="inc",
        action="incident.created",
        payload={"type": "RISK", "severity": 2, "title": "test"},
    )
    ls.append(inc)

    pg = PaperGates(min_trading_days=60)
    res = pg.evaluate_promotion(ls)
    assert not res["passed"]
    # audit chain should still verify (events appended)
    assert ls.verify_chain()
    evs = ls.by_action("paper_gate.evaluation")
    assert evs
