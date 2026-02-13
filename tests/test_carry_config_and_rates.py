from __future__ import annotations

import json
from pathlib import Path

from octa.execution.carry import resolve_carry_rates


def test_rates_resolution_prefers_config_then_file(tmp_path: Path) -> None:
    cfg = {"rates": {"USD": 0.05, "JPY": 0.01}}
    out = resolve_carry_rates(carry_cfg=cfg, rates_file_path=tmp_path / "carry_rates.json")
    assert out["enabled"] is True
    assert out["source"] == "carry_config.rates"

    cfg2 = {}
    p = tmp_path / "carry_rates.json"
    p.write_text(json.dumps({"rates": {"USD": 0.04, "EUR": 0.02}}), encoding="utf-8")
    out2 = resolve_carry_rates(carry_cfg=cfg2, rates_file_path=p)
    assert out2["enabled"] is True
    assert out2["source"] == str(p)


def test_rates_resolution_fail_closed_without_sources(tmp_path: Path) -> None:
    out = resolve_carry_rates(carry_cfg={}, rates_file_path=tmp_path / "missing.json")
    assert out["enabled"] is False
    assert out["disabled_reason"] == "no_deterministic_carry_rate_source"
