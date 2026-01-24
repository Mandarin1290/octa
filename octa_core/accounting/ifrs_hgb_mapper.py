from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class AccountingMapping:
    """Config-driven mapping helper.

    This module does not provide legal/tax advice. It simply applies a user-provided mapping
    (e.g. chart-of-accounts codes) to internal categories.
    """

    coa: Dict[str, str]

    def account(self, key: str) -> str:
        v = self.coa.get(key)
        if not v:
            raise KeyError(f"missing_account_mapping:{key}")
        return str(v)


__all__ = ["AccountingMapping"]
