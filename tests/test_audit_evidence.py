from octa_reg.audit_evidence import (
    create_evidence,
    export_evidence_json,
    load_evidence_json,
    verify_evidence,
)


def test_evidence_reproducible():
    state = {"positions": {"A": 100.0}, "incidents": []}
    controls = ["C-1", "C-2"]

    e1 = create_evidence(state, controls)
    e2 = create_evidence(state, controls)

    # different ids and timestamps but hash should be same because hash excludes ts/id
    assert e1.hash == e2.hash
    assert verify_evidence(e1)
    assert verify_evidence(e2)

    # export/load preserves verifiability
    j = export_evidence_json(e1)
    loaded = load_evidence_json(j)
    assert loaded.hash == e1.hash
    assert verify_evidence(loaded)


def test_tampering_detected():
    state = {"positions": {"A": 100.0}, "incidents": []}
    controls = ["C-1"]
    e = create_evidence(state, controls)

    # tamper with snapshot
    e.snapshot["positions"]["A"] = 9999.0
    assert not verify_evidence(e)
