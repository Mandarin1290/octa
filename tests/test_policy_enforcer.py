import pytest

from octa_ip.ip_classifier import CORE_PROPRIETARY, OPEN_SOURCE_DERIVED, IPClassifier
from octa_ip.module_map import ModuleMap
from octa_ip.policy_enforcer import PolicyEnforcer


def make_sample_module_map():
    # Create a tiny module graph to exercise policies
    mm = ModuleMap()

    # core proprietary owned by team_core
    mm.add_module("core.mod", owner="team_core", classification="internal")
    mm.add_module("lib.open", owner="team_ext", classification="internal")
    mm.add_dependency("core.mod", "lib.open")

    return mm


def test_forbidden_usage_blocked():
    mm = make_sample_module_map()
    classifier = IPClassifier()

    # Set classifications explicitly
    classifier.set_classification("core.mod", CORE_PROPRIETARY)
    classifier.set_classification("lib.open", OPEN_SOURCE_DERIVED)

    enforcer = PolicyEnforcer()

    with pytest.raises(RuntimeError):
        enforcer.enforce(mm, classifier)


def test_violation_logged():
    mm = make_sample_module_map()
    classifier = IPClassifier()

    classifier.set_classification("core.mod", CORE_PROPRIETARY)
    classifier.set_classification("lib.open", OPEN_SOURCE_DERIVED)
    enforcer = PolicyEnforcer()

    with pytest.raises(RuntimeError):
        enforcer.enforce(mm, classifier)

    # Ensure a violation was logged
    errors = [e for e in enforcer.audit_log if e["level"] == "error"]
    assert len(errors) >= 1
    assert any(e["msg"] == "policy_violation" for e in errors)
