from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.core.governance.model_registry import (
    append_entry,
    build_registry_entry,
    canonical_dumps,
    compute_deps_fingerprint,
    compute_model_id,
    get_latest,
    validate_entry,
)


def _entry(*, symbol: str = "AAA", timeframe: str = "1D", created_at: str = "2026-02-24T00:00:00+00:00") -> dict:
    return build_registry_entry(
        symbol=symbol,
        timeframe=timeframe,
        artifact_path="artifacts/runs/r1/AAA.pkl",
        artifact_sha256="a" * 64,
        artifact_size_bytes=123,
        feature_code_hash="b" * 64,
        config_hash="c" * 64,
        stage="research",
        run_id="r1",
        evidence_dir="artifacts/runs/r1",
        inputs_hash="d" * 64,
        outputs_hash="e" * 64,
        gates={"structural": "PASS", "risk": "PASS", "performance": "PASS", "drift": "HOLD"},
        promotion_status="CANDIDATE",
        promotion_reason="unit_test",
        deps_fingerprint="f" * 64,
        created_at=created_at,
        asset_class="stock",
        training_data_hash="1" * 64,
        hyperparam_hash="2" * 64,
        seed=42,
    )


def test_canonical_hash_is_stable() -> None:
    obj = {"b": 2, "a": 1, "nested": {"z": 1, "x": 2}}
    s1 = canonical_dumps(obj)
    s2 = canonical_dumps({"nested": {"x": 2, "z": 1}, "a": 1, "b": 2})
    assert s1 == s2


def test_model_id_deterministic() -> None:
    m1 = compute_model_id("AAA", "1D", "a" * 64, "c" * 64)
    m2 = compute_model_id("AAA", "1D", "a" * 64, "c" * 64)
    assert m1 == m2
    assert len(m1) == 16


def test_append_only_and_get_latest(tmp_path: Path) -> None:
    reg = tmp_path / "registry.jsonl"
    ev = tmp_path / "evidence"
    ctx = {"mode": "research", "stage": "research", "service": "autopilot", "execution_active": False, "entrypoint": "autopilot", "run_id": "r1"}

    e1 = _entry(created_at="2026-02-24T00:00:00+00:00")
    e2 = _entry(created_at="2026-02-24T00:00:01+00:00")
    assert append_entry(ctx, e1, reg, ev) is True
    assert append_entry(ctx, e2, reg, ev) is True

    lines = [ln for ln in reg.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 2
    latest = get_latest("AAA", "1D", reg)
    assert latest is not None
    assert latest["created_at"] == "2026-02-24T00:00:01+00:00"


def test_validate_entry_sha256() -> None:
    e = _entry()
    assert validate_entry(e) is True
    bad = dict(e)
    bad["promotion"] = dict(e["promotion"])
    bad["promotion"]["status"] = "REJECTED"
    assert validate_entry(bad) is False


def test_production_context_blocks_writes(tmp_path: Path) -> None:
    reg = tmp_path / "registry.jsonl"
    ev = tmp_path / "evidence"
    e = _entry()
    ctx = {"mode": "paper", "stage": "research", "service": "autopilot", "execution_active": True, "entrypoint": "execution_service", "run_id": "exec_run"}
    with pytest.raises(RuntimeError, match="IMMUTABLE_PROD_BLOCK"):
        append_entry(ctx, e, reg, ev)


def test_shadow_context_warn_skip(tmp_path: Path) -> None:
    reg = tmp_path / "registry.jsonl"
    ev = tmp_path / "evidence"
    e = _entry()
    ctx = {"mode": "shadow", "stage": "research", "service": "autopilot", "execution_active": True, "entrypoint": "execution_service", "run_id": "shadow_run"}
    ok = append_entry(ctx, e, reg, ev)
    assert ok is False
    assert not reg.exists()
    payload = json.loads((ev / "registry_append.json").read_text(encoding="utf-8"))
    assert payload["reason"] == "IMMUTABLE_PROD_BLOCK"


def test_deps_fingerprint_injected_provider() -> None:
    fp = compute_deps_fingerprint(provider=lambda: ["b==2.0", "a==1.0", "a==1.0"])
    assert len(fp) == 64
    fp2 = compute_deps_fingerprint(provider=lambda: ["a==1.0", "b==2.0"])
    assert fp == fp2
