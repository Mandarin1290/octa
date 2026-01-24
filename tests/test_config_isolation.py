import pytest

from octa_ip.config_isolation import Config, ConfigValidator, InMemorySecretVault


def test_secret_leakage_blocked():
    cfg = Config.from_dict({"db_password": "s3cr3tvalue"})
    with pytest.raises(ValueError):
        ConfigValidator.validate_no_secrets_in_code(cfg)


def test_placeholder_requires_vault_and_resolves():
    cfg = Config.from_dict({"db_password": "${secret:db_pass}", "host": "db.local"})
    # validation should not raise (placeholder is allowed), but direct secrets are blocked
    ConfigValidator.validate_no_secrets_in_code(cfg)
    vault = InMemorySecretVault()
    with pytest.raises(KeyError):
        ConfigValidator.resolve_placeholders(cfg, vault)
    vault.set("db_pass", "mysecret")
    resolved = ConfigValidator.resolve_placeholders(cfg, vault)
    assert resolved["db_password"] == "mysecret"


def test_no_strategy_logic_in_config():
    cfg = Config.from_dict({"algo": "def f():\n    return 1"})
    with pytest.raises(ValueError):
        ConfigValidator.validate_no_strategy_logic(cfg)
