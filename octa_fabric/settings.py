from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Mode(str, Enum):
    DEV = "DEV"
    PAPER = "PAPER"
    LIVE = "LIVE"


class OperatorInfo(BaseModel):
    operator_token: Optional[str] = Field(None, min_length=8)


class BrokerConfig(BaseModel):
    broker_name: Optional[str] = Field(None)
    # secrets should not be stored in plaintext configs
    requires_secret: bool = Field(True)


class FabricSettings(BaseModel):
    schema_version: int = Field(..., ge=1)
    mode: Mode
    env: str
    service_name: str
    operator: OperatorInfo = Field(
        default_factory=lambda: OperatorInfo(operator_token=None)
    )
    broker: BrokerConfig = Field(
        default_factory=lambda: BrokerConfig(broker_name=None, requires_secret=True)
    )
    allow_trading: Optional[bool] = None
    signed_fingerprint: Optional[str] = None

    @field_validator("service_name")
    def service_name_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("service_name must be set")
        return v

    @model_validator(mode="after")
    def defaults_by_mode(self):
        if self.mode == Mode.DEV:
            if self.allow_trading is None:
                object.__setattr__(self, "allow_trading", False)
        elif self.mode == Mode.PAPER:
            if self.allow_trading is None:
                object.__setattr__(self, "allow_trading", False)
        elif self.mode == Mode.LIVE:
            if self.allow_trading is None:
                object.__setattr__(self, "allow_trading", True)
        return self
