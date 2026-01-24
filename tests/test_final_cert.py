from octa_reports.final_cert import FinalCertManager


def test_cert_and_freeze_and_reopen():
    provenance = {
        "ip_registry": {"evidence_hash": "abc"},
        "audit": {"evidence_hash": "def"},
    }
    mgr = FinalCertManager(provenance=provenance)
    cert = mgr.certify(scope=["core", "governance"], notes="Tier-1 complete")
    assert mgr.is_frozen() is True
    assert cert.cert_hash

    # request reopen
    req = mgr.request_reopen("auditor", "needs update")
    assert req["status"] == "pending"

    # approve reopen
    apr = mgr.approve_reopen(0, "board")
    assert apr["status"] == "approved"
    assert mgr.is_frozen() is False
