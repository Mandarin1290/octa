from typing import Any, Dict, List, Optional, Protocol

from octa.core.data.sources.stream.manifest import AssetManifest


class ValidationResult:
    def __init__(
        self,
        eligible: bool,
        reasons: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.eligible = eligible
        self.reasons = reasons or []
        self.metadata = metadata or {}


class Validator(Protocol):
    def validate(self, manifest: AssetManifest) -> ValidationResult: ...
