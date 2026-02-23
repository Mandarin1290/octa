from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_module():
    path = Path("scripts/octa_ibkr_supervisor.py")
    spec = importlib.util.spec_from_file_location("octa_ibkr_supervisor", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_load_config_missing_uses_deterministic_defaults(tmp_path: Path, monkeypatch):
    mod = _load_module()
    monkeypatch.delenv("OCTA_IBKR_HOST", raising=False)
    monkeypatch.delenv("OCTA_IBKR_PORT", raising=False)
    monkeypatch.delenv("OCTA_IBKR_MODE", raising=False)
    monkeypatch.delenv("OCTA_IBKR_PROCESS_MATCH", raising=False)
    monkeypatch.delenv("OCTA_IBKR_CLIENT_ID", raising=False)
    monkeypatch.delenv("OCTA_IBKR_TIMEOUT", raising=False)
    cfg = mod._load_config(tmp_path / "missing.yaml", tmp_path / "evidence")
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 7497
    assert cfg.mode == "tws"
    assert cfg.process_match == "tws"
    assert cfg.config_present is False
    assert cfg.generated_config_path is not None
    assert cfg.generated_config_path.exists()


def test_load_config_parses_ibkr_block(tmp_path: Path):
    mod = _load_module()
    cfg_file = tmp_path / "execution_ibkr.yaml"
    cfg_file.write_text(
        "ibkr:\n"
        "  host: 127.0.0.1\n"
        "  port: 4002\n"
        "  mode: gateway\n"
        "  process_match: ibgateway\n"
        "  client_id: 902\n"
        "  timeout: 9\n",
        encoding="utf-8",
    )
    cfg = mod._load_config(cfg_file, tmp_path / "evidence")
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 4002
    assert cfg.mode == "gateway"
    assert cfg.process_match == "ibgateway"
    assert cfg.client_id == 902
    assert cfg.timeout_sec == 9.0
    assert cfg.config_present is True
    assert isinstance(cfg.launch_providers, list)
    assert cfg.launch_providers


def test_load_config_invalid_mode_raises(tmp_path: Path):
    mod = _load_module()
    cfg_file = tmp_path / "execution_ibkr.yaml"
    cfg_file.write_text("ibkr:\n  mode: live\n", encoding="utf-8")
    with pytest.raises(ValueError):
        mod._load_config(cfg_file, tmp_path / "evidence")


def test_port_probe_closed_localhost():
    mod = _load_module()
    ok, reason = mod._port_open("127.0.0.1", 1, timeout=0.1)
    assert ok is False
    assert isinstance(reason, str) and reason


def test_load_config_missing_uses_env_fallbacks(tmp_path: Path, monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OCTA_IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("OCTA_IBKR_PORT", "4002")
    monkeypatch.setenv("OCTA_IBKR_MODE", "gateway")
    monkeypatch.setenv("OCTA_IBKR_PROCESS_MATCH", "ibgateway")
    monkeypatch.setenv("OCTA_IBKR_CLIENT_ID", "911")
    monkeypatch.setenv("OCTA_IBKR_TIMEOUT", "9")
    cfg = mod._load_config(tmp_path / "missing.yaml", tmp_path / "evidence")
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 4002
    assert cfg.mode == "gateway"
    assert cfg.process_match == "ibgateway"
    assert cfg.client_id == 911
    assert cfg.timeout_sec == 9.0
    assert isinstance(cfg.launch_providers, list)
    assert cfg.launch_providers


def test_load_config_parses_explicit_launch_providers(tmp_path: Path):
    mod = _load_module()
    cfg_file = tmp_path / "execution_ibkr.yaml"
    cfg_file.write_text(
        "ibkr:\n"
        "  mode: gateway\n"
        "  host: 127.0.0.1\n"
        "  port: 4002\n"
        "  process_match: ibgateway\n"
        "launch:\n"
        "  providers:\n"
        "    - provider: direct\n"
        "      command: ['/tmp/fake_ibg']\n"
        "      cwd: '/tmp'\n"
        "      log_file: 'logs/x.log'\n"
        "    - provider: systemd-user\n"
        "      unit: octa-ibkr.service\n",
        encoding="utf-8",
    )
    cfg = mod._load_config(cfg_file, tmp_path / "evidence")
    assert len(cfg.launch_providers) == 2
    assert cfg.launch_providers[0]["provider"] == "direct"
    assert cfg.launch_providers[1]["provider"] == "systemd-user"
