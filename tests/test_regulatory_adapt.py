import pytest

from octa_compliance.regulatory_adapt import RegulatoryAdaptation, Rule


def test_rule_versioning_and_lineage():
    ra = RegulatoryAdaptation()
    r1 = Rule(
        rule_id="KYC",
        version="1.0",
        jurisdiction="DE",
        effective_date="2026-01-01",
        content={"name": True, "id": True},
        metadata={"required_fields": ["name", "id"]},
    )
    h1 = ra.add_rule(user="compliance", rule=r1)
    assert h1
    # add compatible new version
    r2 = Rule(
        rule_id="KYC",
        version="1.1",
        jurisdiction="DE",
        effective_date="2026-06-01",
        content={"name": True, "id": True, "email": False},
        metadata={"required_fields": ["name", "id"]},
        parent="1.0",
    )
    h2 = ra.add_rule_version(
        user="compliance", rule_id="KYC", new_rule=r2, compatibility_mode="strict"
    )
    assert h2
    # verify latest
    assert ra.latest_version("KYC") == "1.1"
    assert ra.verify_evolution_log() is True


def test_backward_incompatibility_rejected():
    ra = RegulatoryAdaptation()
    r1 = Rule(
        rule_id="TRX",
        version="1.0",
        jurisdiction="US",
        effective_date="2026-01-01",
        content={"a": True, "b": True},
        metadata={"required_fields": ["a", "b"]},
    )
    ra.add_rule(user="compliance", rule=r1)
    # new version removes required field 'b' -> should be rejected in strict mode
    r2 = Rule(
        rule_id="TRX",
        version="1.1",
        jurisdiction="US",
        effective_date="2026-03-01",
        content={"a": True},
        metadata={"required_fields": ["a"]},
        parent="1.0",
    )
    with pytest.raises(ValueError):
        ra.add_rule_version(
            user="compliance", rule_id="TRX", new_rule=r2, compatibility_mode="strict"
        )

    # lenient mode allows
    h = ra.add_rule_version(
        user="compliance", rule_id="TRX", new_rule=r2, compatibility_mode="lenient"
    )
    assert h
