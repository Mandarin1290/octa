from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict


class BrokerAdapter(ABC):
    """Abstract broker adapter interface.

    Implementations MUST be sandbox-only unless explicitly requested.
    """

    @abstractmethod
    def submit_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Submit an order. Return a dict with at least `order_id` and `status`.
        May raise exceptions for validation errors.
        """

    @abstractmethod
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Attempt to cancel an order; return cancel status dict."""

    @abstractmethod
    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Return current status for `order_id`."""

    @abstractmethod
    def account_snapshot(self) -> Dict[str, Any]:
        """Return account positions, margin and buying power snapshot."""
