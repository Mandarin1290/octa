from decimal import Decimal

from octa_fund.gates import RedemptionGateManager
from octa_fund.investor_accounts import InvestorAccount
from octa_reports.fund_dashboard import FundDashboard


def test_dashboard_reconciles_with_ledger():
    a1 = InvestorAccount("inv-1", "Alice")
    a2 = InvestorAccount("inv-2", "Bob")
    # Alice: deposit 100, buy 5 shares at 10
    a1.deposit(Decimal("100"))
    a1.buy_shares("class-A", Decimal("5"), Decimal("10"))  # cost 50
    # Bob: deposit 200, buy 10 shares at 10
    a2.deposit(Decimal("200"))
    a2.buy_shares("class-A", Decimal("10"), Decimal("10"))  # cost 100

    navs = {"class-A": Decimal("12")}
    db = FundDashboard([a1, a2], navs)

    # compute expected AUM: sum of cash + shares*nav
    # Alice: cash 50 (100-50) + 5*12 = 60 -> total 110
    # Bob: cash 100 (200-100) + 10*12 = 120 -> total 220
    expected = Decimal("110") + Decimal("220")
    assert db.fund_aum() == expected.quantize(Decimal("0.00000001"))
    # per-investor totals match dashboard breakdown
    inv_balances = db.investor_balances()
    assert inv_balances["inv-1"]["total"] == Decimal("110").quantize(
        Decimal("0.00000001")
    )
    assert inv_balances["inv-2"]["total"] == Decimal("220").quantize(
        Decimal("0.00000001")
    )


def test_fee_accruals_detected():
    a1 = InvestorAccount("inv-3", "Carol")
    a1.deposit(Decimal("100"))
    # simulate fee by withdraw with memo
    a1.withdraw(Decimal("2"), memo="management_fee")
    db = FundDashboard([a1], {"class-A": Decimal("1")})
    assert db.fee_accruals() == Decimal("2").quantize(Decimal("0.00000001"))


def test_liquidity_and_gate_status():
    a1 = InvestorAccount("inv-4", "Dan")
    a1.deposit(Decimal("100"))
    db = FundDashboard([a1], {})
    gm = RedemptionGateManager(Decimal("0.1"))
    status = db.liquidity_and_gate_status(gm, Decimal("50"))
    assert status["aum"] == Decimal("100").quantize(Decimal("0.00000001"))
    assert status["gate_allowed"] == Decimal("10").quantize(Decimal("0.00000001"))
