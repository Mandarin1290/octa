from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(Exception):
    pass


@dataclass
class Config:
    env: str
    service_name: str = "octa-foundation"
    enable_risk: bool = True


def load_config() -> Config:
    env = os.getenv("OCTA_ENV")
    if not env:
        raise ConfigurationError("OCTA_ENV is required")
    svc = os.getenv("OCTA_SERVICE_NAME", "octa-foundation")
    enable_risk = os.getenv("OCTA_ENABLE_RISK", "true").lower() == "true"
    return Config(env=env, service_name=svc, enable_risk=enable_risk)


def get_config() -> Config:
    return load_config()
