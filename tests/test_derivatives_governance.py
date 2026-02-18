"""Tests for derivatives governance blocking (LEI + EMIR)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from octa.core.governance.derivatives_gate import DerivativesGate, DerivativesGateResult
from octa.core.governance.emir import (
    EMIRConfig,
    check_emir_compliance,
    is_derivative,
    is_equity,
    load_emir_config,
)
from octa.core.governance.lei_registry import LEIEntry, LEIRegistry


# ---- LEI Registry ----

def test_lei_valid() -> None:
    reg = LEIRegistry([
        LEIEntry("529900T8BM49AURSDO55", "Acme", "ACTIVE", "2027-01-15"),
    ])
    result = reg.check("529900T8BM49AURSDO55", as_of=date(2026, 6, 1))
    assert result.valid is True
    assert result.reason == "LEI_VALID"


def test_lei_missing() -> None:
    reg = LEIRegistry([])
    result = reg.check(None)
    assert result.valid is False
    assert result.reason == "LEI_MISSING"


def test_lei_not_in_registry() -> None:
    reg = LEIRegistry([])
    result = reg.check("529900NOTFOUND")
    assert result.valid is False
    assert result.reason == "LEI_NOT_IN_REGISTRY"


def test_lei_expired() -> None:
    reg = LEIRegistry([
        LEIEntry("529900EXPIRED", "OldCo", "ACTIVE", "2025-01-01"),
    ])
    result = reg.check("529900EXPIRED", as_of=date(2026, 1, 1))
    assert result.valid is False
    assert result.reason == "LEI_EXPIRED"


def test_lei_lapsed_status() -> None:
    reg = LEIRegistry([
        LEIEntry("529900LAPSED", "BadCo", "LAPSED", "2028-01-01"),
    ])
    result = reg.check("529900LAPSED")
    assert result.valid is False
    assert "LEI_STATUS_LAPSED" in result.reason


def test_lei_from_file(tmp_path: Path) -> None:
    data = {
        "entities": [
            {"lei": "LEI001", "legal_name": "Test GmbH", "status": "ACTIVE", "expiry_date": "2028-12-31"}
        ]
    }
    path = tmp_path / "lei.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    reg = LEIRegistry.from_file(path)
    assert reg.check("LEI001", as_of=date(2026, 1, 1)).valid is True


def test_lei_from_missing_file(tmp_path: Path) -> None:
    reg = LEIRegistry.from_file(tmp_path / "no_such.json")
    assert reg.check("ANYTHING").valid is False


# ---- EMIR ----

def test_emir_equity_exempt() -> None:
    result = check_emir_compliance("equity", None)
    assert result.compliant is True
    assert result.reason == "EQUITY_EXEMPT"


def test_emir_derivative_no_config() -> None:
    result = check_emir_compliance("options", None)
    assert result.compliant is False
    assert result.reason == "EMIR_CONFIG_MISSING"


def test_emir_derivative_valid() -> None:
    cfg = EMIRConfig(
        clearing_obligation="delegated",
        reporting_obligation="delegated",
        risk_mitigation="self",
        delegation_entity="ClearCo",
        delegation_lei="LEI123",
        asset_class_scope=frozenset({"options"}),
    )
    result = check_emir_compliance("options", cfg)
    assert result.compliant is True
    assert result.reason == "EMIR_COMPLIANT"


def test_emir_delegation_unknown() -> None:
    cfg = EMIRConfig(
        clearing_obligation="unknown",
        reporting_obligation="delegated",
        risk_mitigation="self",
        delegation_entity=None,
        delegation_lei=None,
        asset_class_scope=frozenset(),
    )
    result = check_emir_compliance("futures", cfg)
    assert result.compliant is False
    assert result.reason == "EMIR_DELEGATION_UNKNOWN"


def test_is_derivative() -> None:
    assert is_derivative("options") is True
    assert is_derivative("futures") is True
    assert is_derivative("equity") is False
    assert is_derivative("stocks") is False


def test_is_equity() -> None:
    assert is_equity("equity") is True
    assert is_equity("equities") is True
    assert is_equity("options") is False


def test_load_emir_config(tmp_path: Path) -> None:
    data = {
        "delegation": {
            "clearing_obligation": "delegated",
            "reporting_obligation": "self",
            "risk_mitigation": "delegated",
        },
        "asset_class_scope": ["options", "futures"],
    }
    path = tmp_path / "emir.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    cfg = load_emir_config(path)
    assert cfg is not None
    assert cfg.clearing_obligation == "delegated"
    assert cfg.reporting_obligation == "self"


def test_load_emir_config_missing(tmp_path: Path) -> None:
    assert load_emir_config(tmp_path / "no_such.json") is None


# ---- Combined Derivatives Gate ----

def test_gate_equity_always_allowed() -> None:
    gate = DerivativesGate(LEIRegistry(), None)
    result = gate.check("equity")
    assert result.allowed is True
    assert result.reason == "EQUITY_EXEMPT"


def test_gate_derivative_blocked_no_lei_no_emir() -> None:
    gate = DerivativesGate(LEIRegistry(), None)
    result = gate.check("options")
    assert result.allowed is False
    assert "LEI" in result.reason
    assert "EMIR" in result.reason


def test_gate_derivative_blocked_valid_lei_no_emir() -> None:
    reg = LEIRegistry([
        LEIEntry("LEI001", "Acme", "ACTIVE", "2028-01-01"),
    ])
    gate = DerivativesGate(reg, None)
    result = gate.check("options", lei="LEI001", as_of=date(2026, 1, 1))
    assert result.allowed is False
    assert "EMIR" in result.reason


def test_gate_derivative_blocked_no_lei_valid_emir() -> None:
    cfg = EMIRConfig("delegated", "delegated", "self", "ClearCo", "LEI", frozenset())
    gate = DerivativesGate(LEIRegistry(), cfg)
    result = gate.check("futures")
    assert result.allowed is False
    assert "LEI" in result.reason


def test_gate_derivative_allowed_both_valid() -> None:
    reg = LEIRegistry([
        LEIEntry("LEI001", "Acme", "ACTIVE", "2028-01-01"),
    ])
    cfg = EMIRConfig("delegated", "delegated", "self", "ClearCo", "LEI001", frozenset())
    gate = DerivativesGate(reg, cfg)
    result = gate.check("options", lei="LEI001", as_of=date(2026, 1, 1))
    assert result.allowed is True
    assert result.reason == "DERIVATIVES_ALLOWED"


def test_gate_from_config(tmp_path: Path) -> None:
    lei_data = {"entities": [
        {"lei": "LEI001", "legal_name": "Test", "status": "ACTIVE", "expiry_date": "2028-12-31"}
    ]}
    emir_data = {
        "delegation": {
            "clearing_obligation": "delegated",
            "reporting_obligation": "delegated",
            "risk_mitigation": "self",
        },
        "asset_class_scope": ["options"],
    }
    lei_path = tmp_path / "lei.json"
    emir_path = tmp_path / "emir.json"
    lei_path.write_text(json.dumps(lei_data), encoding="utf-8")
    emir_path.write_text(json.dumps(emir_data), encoding="utf-8")

    gate = DerivativesGate.from_config(lei_path, emir_path)
    result = gate.check("options", lei="LEI001", as_of=date(2026, 1, 1))
    assert result.allowed is True


def test_gate_status() -> None:
    reg = LEIRegistry([LEIEntry("L1", "X", "ACTIVE", None)])
    cfg = EMIRConfig("delegated", "delegated", "self", "E", "L", frozenset())
    gate = DerivativesGate(reg, cfg)
    s = gate.status()
    assert s["lei_entries"] == 1
    assert s["emir_configured"] is True
