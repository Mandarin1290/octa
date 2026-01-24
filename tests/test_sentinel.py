from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from octa_ledger.store import LedgerStore
from octa_sentinel.engine import SentinelEngine
from octa_sentinel.policies import SentinelPolicy


def make_policy(tmp_path):
    return SentinelPolicy(schema_version=1, name="default", policy_version="v1")


def test_audit_failure_triggers_level(tmp_path):
    # create ledger with empty/corrupt log to simulate failure
    d = str(tmp_path / "ledger")
    ledger = LedgerStore(d)
    # tamper: create a bad log file
    with open(ledger.log_path, "wb") as fh:
        fh.write(b"notjson\n")
    engine = SentinelEngine(
        make_policy(tmp_path),
        state_path=str(tmp_path / "state.json"),
        ledger_store=ledger,
    )
    dec = engine.evaluate({})
    assert dec.level >= 2


def test_drawdown_breach_triggers_freeze(tmp_path):
    policy = make_policy(tmp_path)
    engine = SentinelEngine(policy, state_path=str(tmp_path / "state2.json"))
    inputs = {"pnl": {"current_nav": 90.0, "peak_nav": 100.0, "daily_loss": 0.0}}
    dec = engine.evaluate(inputs)
    assert dec.level >= 2


def test_policy_change_logged(tmp_path):
    # use ledger with signing key
    d = str(tmp_path / "ledger2")
    priv = Ed25519PrivateKey.generate()
    priv_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    ledger = LedgerStore(d, signing_key_bytes=priv_bytes)
    engine = SentinelEngine(
        make_policy(tmp_path),
        state_path=str(tmp_path / "state3.json"),
        ledger_store=ledger,
    )
    engine.evaluate(
        {"pnl": {"current_nav": 100.0, "peak_nav": 100.0, "daily_loss": 0.0}}
    )
    # ledger should have recorded a gate_event
    cur = ledger._conn.cursor()
    cur.execute("SELECT action FROM events WHERE action='gate_event'")
    rows = cur.fetchall()
    assert rows
