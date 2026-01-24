Configuration & Secrets Policy

Principles

- No secrets stored in plaintext within repository or config overlays.
- Secrets are provided via environment, encrypted file, or OS keyring.
- Missing secrets during critical modes (LIVE) cause safe-fail: no trading.
- All configs are versioned via `schema_version` and validated using pydantic.

Hierarchy

1. `configs/base.yaml` — non-secret defaults maintained in repo.
2. `configs/<mode>.yaml` — mode-specific overlays (dev/paper/live).
3. Environment variables prefixed with `OCTA_CFG_` override values at runtime.
4. Secrets from `OCTA_SECRET_<KEY>` or an encrypted secrets file (`OCTA_SECRETS_FILE`) or keyring.

LIVE Safety Controls

- To start in LIVE mode the operator must provide an operator token (`OCTA_OPERATOR_TOKEN`) and a signed fingerprint (`OCTA_SIGNED_FINGERPRINT`).
- The loader verifies the signed fingerprint by computing HMAC-SHA256(fingerprint, operator_token).

Secrets Backends

- `EnvSecretsBackend`: for CI and local dev (less secure). Reads `OCTA_SECRET_<KEY>`.
- `EncryptedFileBackend`: AES-GCM encrypted file; key supplied via `OCTA_SECRETS_KEY` (base64 or hex). File format: nonce(12) + ciphertext; contents are newline-separated `KEY=VALUE` pairs.
- `KeyringBackend`: preferred when available; falls back if not present.

Safe Defaults

- DEV: `allow_trading` is False, ensuring accidental trading is blocked.
- PAPER: trading disabled by default; broker secrets optional.
- LIVE: trading allowed only when operator token & signed fingerprint verified.
