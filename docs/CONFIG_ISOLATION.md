# Configuration & Secret Isolation

This document describes the policy and runtime helpers to keep secrets and configuration
separate from IP and strategy logic.

Goals
- No secrets in code. Config files must not include literal secrets (passwords, tokens, keys).
- Config must not encode strategy logic (no functions, lambdas, imports inside values).
- Externalizable modules should run without embedded secrets; secrets are injected at runtime via a vault.

Key components
- `octa_ip.config_isolation.SecretVault` — abstract interface for secret backends.
- `octa_ip.config_isolation.InMemorySecretVault` — lightweight in-memory vault for tests and local resolution.
- `octa_ip.config_isolation.Config` — config abstraction which can detect placeholders and direct secrets.
- `octa_ip.config_isolation.ConfigValidator` — validation helpers to enforce rules and resolve placeholders.

Usage
1. Keep actual secret values out of source-controlled config files. Use `${secret:NAME}` placeholders instead.
2. Provide a vault (e.g., a real secrets manager) at runtime and resolve placeholders via `ConfigValidator.resolve_placeholders()`.
3. Validate configs during CI and at runtime with `validate_no_secrets_in_code()` and `validate_no_strategy_logic()`.

Example
```
cfg = Config.from_dict({"db_host": "db.example", "db_password": "${secret:db_pass}"})
vault = InMemorySecretVault()
vault.set("db_pass", "supersecret")
ConfigValidator.validate_no_secrets_in_code(cfg)
resolved = ConfigValidator.resolve_placeholders(cfg, vault)
```

Limitations
- This module provides conservative heuristics and an interface; real-world deployments should
  integrate hardened secret backends and more advanced static analysis for strategy-code detection.
