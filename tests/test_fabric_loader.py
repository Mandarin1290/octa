from octa_fabric.loader import build_settings
from octa_fabric.settings import Mode


def test_build_settings_dev(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "base.yaml").write_text(
        "schema_version: 1\nmode: DEV\nenv: base\nservice_name: test\n"
    )
    (cfg_dir / "dev.yaml").write_text("mode: DEV\nenv: dev\nservice_name: test-dev\n")
    monkeypatch.setenv("OCTA_CFG_service_name", "overridden")
    s = build_settings(str(cfg_dir), Mode.DEV)
    assert s.env == "dev"
    assert s.service_name == "overridden"
    # DEV must not allow trading
    assert s.allow_trading is False


def test_live_requires_operator(monkeypatch, tmp_path):
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "base.yaml").write_text(
        "schema_version: 1\nmode: LIVE\nenv: base\nservice_name: test\n"
    )
    (cfg_dir / "live.yaml").write_text(
        "mode: LIVE\nenv: live\nservice_name: test-live\n"
    )
    # ensure missing operator token causes LoaderError
    monkeypatch.delenv("OCTA_OPERATOR_TOKEN", raising=False)
    monkeypatch.delenv("OCTA_SIGNED_FINGERPRINT", raising=False)
    try:
        build_settings(str(cfg_dir), Mode.LIVE)
        raised = False
    except Exception:
        raised = True
    assert raised
