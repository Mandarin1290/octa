import pytest

from octa_ip.ip_classifier import (
    CORE_PROPRIETARY,
    INTERNAL_ONLY,
    OPEN_SOURCE_DERIVED,
    IPClassifier,
)
from octa_ip.module_map import ModuleMap


def test_misclassified_module_rejected():
    mm = ModuleMap()
    mm.add_module("octa_core", owner="CoreTeam", classification="core")
    mm.add_module("octa_open", owner="OSS", classification="licensable")
    # add dependency from core -> open (open should be classified as OPEN_SOURCE_DERIVED to trigger violation)
    mm.add_dependency("octa_core", "octa_open")

    ip = IPClassifier()
    ip.set_classification("octa_core", CORE_PROPRIETARY)
    ip.set_classification("octa_open", OPEN_SOURCE_DERIVED)

    with pytest.raises(RuntimeError) as exc:
        ip.enforce_runtime(mm)
    assert "core-depends-on-open-source-derived" in str(exc.value)


def test_valid_classification_passes():
    mm = ModuleMap()
    mm.add_module("octa_core", owner="CoreTeam", classification="core")
    mm.add_module("octa_alpha", owner="AlphaTeam", classification="internal")
    mm.add_dependency("octa_core", "octa_alpha")

    ip = IPClassifier()
    ip.set_classification("octa_core", CORE_PROPRIETARY)
    ip.set_classification("octa_alpha", INTERNAL_ONLY)

    # ModuleMap.detect_violations will flag cross-owner-internal, so assign same owner to avoid that
    mm.modules["octa_alpha"].owner = "CoreTeam"

    # should not raise
    ip.enforce_runtime(mm)
