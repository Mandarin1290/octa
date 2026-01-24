from datetime import date, timedelta

from octa_assets.futures.basis import compute_basis
from octa_assets.futures.contracts import ContractRegistry, FuturesContract
from octa_assets.futures.rolls import RollManager


class SentinelMock:
    def __init__(self):
        self.last = None

    def set_gate(self, level, reason):
        self.last = (level, reason)


def test_roll_deterministic():
    # setup two contracts: front expires in 2 days, next in 60 days
    today = date.today()
    c1 = FuturesContract(
        symbol="FUT1",
        root="FUT",
        expiry=today + timedelta(days=2),
        multiplier=50,
        tick_size=0.25,
        currency="USD",
        initial_margin=0.05,
        maintenance_margin=0.04,
    )
    c2 = FuturesContract(
        symbol="FUT2",
        root="FUT",
        expiry=today + timedelta(days=60),
        multiplier=50,
        tick_size=0.25,
        currency="USD",
        initial_margin=0.05,
        maintenance_margin=0.04,
    )

    # candidates volumes/open interest
    candidates = {
        "FUT1": {"volume": 100, "open_interest": 200},
        "FUT2": {"volume": 200, "open_interest": 400},
    }
    contracts_meta = {"FUT1": {"expiry": c1.expiry}, "FUT2": {"expiry": c2.expiry}}

    rm = RollManager(roll_window_days=5, oi_multiplier_trigger=1.1)
    next_sym = rm.decide_roll(
        current_symbol="FUT1", candidates=candidates, contracts_meta=contracts_meta
    )
    assert next_sym == "FUT2"  # because dte <= roll_window


def test_margin_applied_correctly():
    today = date.today()
    c = FuturesContract(
        symbol="FUTX",
        root="FX",
        expiry=today + timedelta(days=30),
        multiplier=10,
        tick_size=0.01,
        currency="USD",
        initial_margin=0.1,
        maintenance_margin=0.08,
    )
    reg = ContractRegistry()
    reg.register(c)

    # qty 2 contracts at price 100 => notional = 2 * 10 * 100 = 2000; initial margin = 0.1 => 200
    m = reg.margin_required("FUTX", qty=2, price=100.0)
    assert abs(m - 200.0) < 1e-6


def test_missing_contract_freezes_trading():
    sentinel = SentinelMock()
    reg = ContractRegistry(sentinel_api=sentinel)
    ok = reg.enforce_exists("NO_SUCH")
    assert ok is False
    assert sentinel.last is not None and sentinel.last[0] == 3


def test_basis_computation():
    b = compute_basis(101.0, 100.0, multiplier=50)
    assert b == 50.0
