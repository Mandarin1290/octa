from octa_tests.drills.helpers import AuditStub, IncidentLog
from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter
from octa_vertex.broker.ibkr_sandbox import IBKRSandboxDriver, SandboxScript


class SentinelMock:
    def __init__(self):
        self.last = None

    def set_gate(self, level, reason):
        self.last = (level, reason)


def test_rejection_handling(tmp_path):
    audit = AuditStub(str(tmp_path / "audit.log"))
    IncidentLog(str(tmp_path / "inc.log"))
    sentinel = SentinelMock()

    cfg = IBKRConfig(rate_limit_per_minute=10, supported_instruments=["AAPL", "FUT_ES"])
    adapter = IBKRContractAdapter(cfg, audit_fn=audit.audit, sentinel_api=sentinel)

    # missing field
    res = adapter.submit_order({"order_id": "o1"})
    assert res["status"] == "REJECTED"

    # unsupported instrument
    res2 = adapter.submit_order(
        {
            "order_id": "o2",
            "instrument": "UNKNOWN",
            "qty": 1,
            "side": "BUY",
            "order_type": "MKT",
        }
    )
    assert res2["status"] == "REJECTED"


def test_rate_limit_freeze(tmp_path):
    audit = AuditStub(str(tmp_path / "audit.log"))
    IncidentLog(str(tmp_path / "inc.log"))
    sentinel = SentinelMock()

    cfg = IBKRConfig(
        rate_limit_per_minute=2, supported_instruments=["AAPL"]
    )  # very low
    adapter = IBKRContractAdapter(cfg, audit_fn=audit.audit, sentinel_api=sentinel)

    o = {
        "order_id": "o1",
        "instrument": "AAPL",
        "qty": 1,
        "side": "BUY",
        "order_type": "MKT",
    }
    assert adapter.submit_order(o)["status"] == "PENDING"
    assert adapter.submit_order({**o, "order_id": "o2"})["status"] == "PENDING"
    # third within minute triggers rate limit
    res = adapter.submit_order({**o, "order_id": "o3"})
    assert res["status"] == "REJECTED"
    assert sentinel.last[0] == 2


def test_sandbox_replay_and_ambiguity(tmp_path):
    audit = AuditStub(str(tmp_path / "audit.log"))
    IncidentLog(str(tmp_path / "inc.log"))
    sentinel = SentinelMock()

    script = SandboxScript(
        events={
            "o1": ["ACK", "SUBMITTED", "FILLED"],
            "o2": ["ACK", "REJECTED:INSUFFICIENT_MARGIN"],
        }
    )
    cfg = IBKRConfig(rate_limit_per_minute=100, supported_instruments=["FUT_ES"])
    driver = IBKRSandboxDriver(
        script, config=cfg, audit_fn=audit.audit, sentinel_api=sentinel
    )

    o1 = {
        "order_id": "o1",
        "instrument": "FUT_ES",
        "qty": 1,
        "side": "BUY",
        "order_type": "MKT",
    }
    r1 = driver.submit_order(o1)
    assert r1["status"] in ("ACK", "SUBMITTED") or r1["status"] == "PENDING"
    # advance to filled
    r1b = driver.advance_order("o1")
    assert r1b["status"] in ("SUBMITTED", "FILLED")

    o2 = {
        "order_id": "o2",
        "instrument": "FUT_ES",
        "qty": 1000,
        "side": "SELL",
        "order_type": "MKT",
    }
    r2 = driver.submit_order(o2)
    assert r2["status"] == "REJECTED"
