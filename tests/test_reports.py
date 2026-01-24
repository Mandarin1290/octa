from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore
from octa_reports.generator import generate_all_reports


def test_reports_reproducible_and_reconcile(tmp_path):
    lp = tmp_path / "ledger"
    ls = LedgerStore(str(lp))

    # create synthetic monthly NAVs (first of each month)
    base_dates = [f"2025-{m:02d}-01T00:00:00+00:00" for m in range(1, 7)]
    navs = [100.0, 102.0, 101.0, 105.0, 107.0, 110.0]
    for d, n in zip(base_dates, navs, strict=False):
        ev = AuditEvent.create(
            actor="perf", action="performance.nav", payload={"date": d, "nav": n}
        )
        ls.append(ev)

    # add trade.executed events for strategy contributions
    trades = [
        {"strategy_id": "s1", "pnl": 10.0},
        {"strategy_id": "s2", "pnl": 5.0},
        {"strategy_id": "s1", "pnl": -2.0},
    ]
    for t in trades:
        ev = AuditEvent.create(actor="exec", action="trade.executed", payload=t)
        ls.append(ev)

    reports1 = generate_all_reports(ls)
    reports2 = generate_all_reports(ls)
    # reproducible
    assert reports1 == reports2

    # reconcile: sum of strategy contribution CSV equals total pnl
    strat_md, strat_csv = reports1["strategy_contribution"]
    # parse csv
    lines = strat_csv.strip().splitlines()
    assert lines[0].startswith("strategy_id")
    total = 0.0
    for row in lines[1:]:
        parts = row.split(",")
        total += float(parts[1])
    expected = sum(t["pnl"] for t in trades)
    assert abs(total - expected) < 1e-6
