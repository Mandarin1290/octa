import os

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from octa_ledger.api import LedgerAPI, TradingBlocked
from octa_ledger.store import LedgerStore


def test_append_and_verify(tmp_path):
    # create signing key
    priv = Ed25519PrivateKey.generate()
    priv_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    d = str(tmp_path / "ledger")
    store = LedgerStore(d, batch_size=2, signing_key_bytes=priv_bytes)
    api = LedgerAPI(d, signing_key=priv_bytes)
    # append two events to trigger a batch signature
    api.audit_or_fail(actor="test", action="create_order", payload={"qty": 1})
    api.audit_or_fail(actor="test", action="cancel_order", payload={"qty": 1})
    assert store.verify_chain() is True


def test_tamper_detection(tmp_path):
    d = str(tmp_path / "ledger")
    store = LedgerStore(d)
    api = LedgerAPI(d)
    api.audit_or_fail(actor="a", action="x", payload={})
    # tamper with the file: change a byte
    p = os.path.join(d, "ledger.log")
    data = open(p, "rb").read()
    tampered = data.replace(b"x", b"y", 1)
    open(p, "wb").write(tampered)
    assert store.verify_chain() is False


def test_audit_failure_blocks(tmp_path, monkeypatch):
    # simulate write permission error by pointing ledger path to a file
    tmp = tmp_path / "file"
    tmp.write_text("notadir")

    # make it a file so os.makedirs will fail
    # monkeypatch LedgerStore to raise on append
    class BadLedger(LedgerStore):
        def append(self, event):
            raise Exception("disk full")

    from octa_ledger.api import LedgerAPI as API

    api = API.__new__(API)
    api.store = BadLedger(str(tmp_path / "dummy"))
    try:
        api.audit_or_fail(actor="a", action="b", payload={})
        blocked = False
    except TradingBlocked:
        blocked = True
    assert blocked
