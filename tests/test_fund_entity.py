from dataclasses import FrozenInstanceError

from octa_capital.aum_state import AUMState
from octa_fund.fund_entity import FundEntity
from octa_ledger.core import AuditChain


class DummyNAV:
    def compute_nav(self, share_classes):
        # simple NAV: sum shares_outstanding * 1.0
        total_shares = sum(
            [v.get("shares_outstanding", 0.0) for v in share_classes.values()]
        )
        return {"nav_per_share": 1.0, "total": total_shares}


def test_entity_core_fields_immutable():
    fund = FundEntity(
        fund_id="F1",
        name="Test Fund",
        base_currency="USD",
        inception_date="2020-01-01T00:00:00Z",
        accounting_calendar="monthly",
    )
    try:
        fund.name = "Other"
        raise AssertionError("should have raised")
    except Exception as e:
        assert isinstance(e, FrozenInstanceError)


def test_linkage_to_aum_state_and_audit():
    ledger = AuditChain()
    fund = FundEntity(
        fund_id="F2",
        name="LnkFund",
        base_currency="USD",
        inception_date="2021-01-01T00:00:00Z",
        accounting_calendar="monthly",
        _audit_fn=lambda e, p: ledger.append({"event": e, **p}),
    )
    aum = AUMState(
        initial_internal=5000.0, audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    fund.attach_aum_state(aum)
    assert fund.get_current_aum() == aum.get_current_total()

    # attach nav engine and compute nav
    nav = DummyNAV()
    fund.attach_nav_engine(nav)
    res = fund.compute_nav()
    assert res["nav_per_share"] == 1.0
