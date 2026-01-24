import pytest

from octa_ip.ip_lineage import LineageTracker


def test_history_and_author_attribution():
    lt = LineageTracker()
    lt.add_change("mod.alpha", "alice", "initial commit", "print(1)")
    hist = lt.history("mod.alpha")
    assert len(hist) == 1
    assert hist[0].author == "alice"
    assert lt.verify_module("mod.alpha") is True


def test_tampering_detected():
    lt = LineageTracker()
    lt.add_change("mod.beta", "bob", "v1", "code v1")
    lt.add_change("mod.beta", "carol", "v2", "code v2")

    # simulate tampering by modifying stored content_hash of first entry
    lt._store["mod.beta"][0].content_hash = "deadbeef"

    with pytest.raises(RuntimeError):
        lt.verify_module("mod.beta")
