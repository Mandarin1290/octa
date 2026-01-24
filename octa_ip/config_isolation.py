import re
from dataclasses import dataclass
from typing import Any, Dict


class SecretVault:
    """Abstract secret vault interface."""

    def get(self, name: str) -> str:
        raise NotImplementedError()

    def set(self, name: str, value: str) -> None:
        raise NotImplementedError()

    def has(self, name: str) -> bool:
        raise NotImplementedError()


class InMemorySecretVault(SecretVault):
    """A minimal in-memory secret vault for testing and local resolution.

    NOTE: Production deployments should use a real secrets manager.
    """

    def __init__(self):
        self._store: Dict[str, str] = {}

    def get(self, name: str) -> str:
        return self._store[name]

    def set(self, name: str, value: str) -> None:
        self._store[name] = value

    def has(self, name: str) -> bool:
        return name in self._store


SECRET_PLACEHOLDER_RE = re.compile(r"\$\{secret:([a-zA-Z0-9_\-\.]+)\}")
SUSPICIOUS_KEYWORDS = ["secret", "password", "passwd", "token", "key", "api_key"]
CODE_PATTERNS = [
    re.compile(r"\bdef\s+\w+\s*\("),
    re.compile(r"\blambda\b"),
    re.compile(r"\bimport\b"),
]


@dataclass
class Config:
    data: Dict[str, Any]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        return cls(data=dict(d))

    def contains_secret_placeholders(self) -> bool:
        for _k, v in self.data.items():
            if isinstance(v, str) and SECRET_PLACEHOLDER_RE.search(v):
                return True
        return False

    def contains_direct_secrets(self) -> bool:
        # conservative: key names containing secret-like words or values that look secret
        for k, v in self.data.items():
            kl = k.lower()
            if any(tok in kl for tok in SUSPICIOUS_KEYWORDS):
                # allow secret placeholder values (e.g., ${secret:name}) but not direct literals
                if isinstance(v, str) and SECRET_PLACEHOLDER_RE.search(v):
                    continue
                return True
            if isinstance(v, str):
                if SECRET_PLACEHOLDER_RE.search(v):
                    continue
                # if value is long and non-human, treat as secret
                if len(v) >= 16 and re.fullmatch(r"[A-Za-z0-9_\-\\+/=]+", v):
                    return True
        return False


class ConfigValidator:
    """Validation rules ensuring no secrets in code and no embedded strategy logic."""

    @staticmethod
    def validate_no_secrets_in_code(cfg: Config) -> None:
        if cfg.contains_direct_secrets():
            raise ValueError(
                "Config contains direct secret values or secret-like keys; move secrets to a vault."
            )

    @staticmethod
    def validate_no_strategy_logic(cfg: Config) -> None:
        for k, v in cfg.data.items():
            if isinstance(v, str):
                for pat in CODE_PATTERNS:
                    if pat.search(v):
                        raise ValueError(
                            f"Config key '{k}' appears to contain executable code or imports; remove strategy logic from config."
                        )

    @staticmethod
    def resolve_placeholders(cfg: Config, vault: SecretVault) -> Dict[str, Any]:
        """Resolve `${secret:NAME}` placeholders using the provided vault.

        Raises KeyError if a required secret is not present in the vault.
        """

        resolved = {}
        for k, v in cfg.data.items():
            if isinstance(v, str):
                m = SECRET_PLACEHOLDER_RE.search(v)
                if m:
                    name = m.group(1)
                    if not vault.has(name):
                        raise KeyError(f"missing secret in vault: {name}")
                    resolved[k] = SECRET_PLACEHOLDER_RE.sub(vault.get(name), v)
                    continue
            resolved[k] = v
        return resolved


__all__ = ["SecretVault", "InMemorySecretVault", "Config", "ConfigValidator"]
