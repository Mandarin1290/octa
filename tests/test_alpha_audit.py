from octa_alpha.audit import AuditChain


def test_lineage_reproducible():
    hyp = {"name": "h1", "params": {"window": 10}}
    data = {"snapshot_id": "s1", "rows": [1, 2, 3]}
    h1 = AuditChain.compute_lineage_hash(hyp, data)
    h2 = AuditChain.compute_lineage_hash(hyp, data)
    assert h1 == h2


def test_tampering_detected():
    chain = AuditChain()
    chain.append("stage.1", {"detail": "a"})
    chain.append("stage.2", {"detail": "b"})
    assert chain.verify()
    # simulate tampering by replacing a block with altered payload
    blocks = chain.blocks()
    altered = blocks[1]
    # create a new block object with different payload but same index
    from dataclasses import replace

    newblk = replace(altered, payload={"detail": "tampered"})
    # directly inject into internal list to simulate tamper
    chain._blocks[1] = newblk
    assert not chain.verify()
