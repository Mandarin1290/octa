from __future__ import annotations

import time

from octa.core.data.sources.nlp.fingpt import FinGPTClient, NullFinGPT


def test_null_fingpt_returns_neutral() -> None:
    client = NullFinGPT()
    result = client.classify_event("test", {})
    assert result.label == "neutral"
    assert result.risk_score == 0.0


def test_fingpt_circuit_breaker(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OCTA_FINGPT_CACHE_ROOT", str(tmp_path / "cache"))
    monkeypatch.setenv("OCTA_FINGPT_AUDIT_ROOT", str(tmp_path / "audit"))

    def failing_request(url: str, payload: dict, timeout_s: float) -> dict:
        raise RuntimeError("boom")

    cfg = {
        "enabled": True,
        "endpoint_url": "http://localhost/ignore",
        "timeout_ms": 10,
        "max_latency_ms": 10,
        "cache_ttl_seconds": 1,
        "circuit_breaker": {"failures": 2, "cooldown_seconds": 60},
    }
    client = FinGPTClient(config=cfg, requester=failing_request)
    r1 = client.classify_event("a", {})
    r2 = client.classify_event("b", {})
    r3 = client.classify_event("c", {})
    assert r1.health.ok is False
    assert r2.health.ok is False
    assert r3.health.error == "circuit_breaker_open"


def test_fingpt_latency_budget(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OCTA_FINGPT_CACHE_ROOT", str(tmp_path / "cache"))
    monkeypatch.setenv("OCTA_FINGPT_AUDIT_ROOT", str(tmp_path / "audit"))

    def slow_request(url: str, payload: dict, timeout_s: float) -> dict:
        time.sleep(0.01)
        return {"label": "neutral", "risk_score": 0.1, "confidence": 0.5}

    cfg = {
        "enabled": True,
        "endpoint_url": "http://localhost/ignore",
        "timeout_ms": 100,
        "max_latency_ms": 1,
        "cache_ttl_seconds": 1,
        "circuit_breaker": {"failures": 3, "cooldown_seconds": 1},
    }
    client = FinGPTClient(config=cfg, requester=slow_request)
    result = client.classify_event("slow", {})
    assert result.health.ok is False
    assert result.health.error == "latency_budget_exceeded"
