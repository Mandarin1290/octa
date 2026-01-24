from octa_fabric.config import Config, ConfigurationError, load_config


def test_config_load_env(monkeypatch) -> None:
    monkeypatch.setenv("OCTA_ENV", "test")
    cfg = load_config()
    assert isinstance(cfg, Config)
    assert cfg.env == "test"


def test_config_missing_raises(monkeypatch) -> None:
    # ensure no OCTA_ENV
    monkeypatch.delenv("OCTA_ENV", raising=False)
    try:
        load_config()
        raised = False
    except ConfigurationError:
        raised = True
    assert raised
