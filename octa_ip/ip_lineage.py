import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass
class LineageEntry:
    module: str
    ts: str
    author: str
    description: str
    content_hash: str
    prev_hash: str
    entry_hash: str


class LineageTracker:
    """Track provenance and immutable lineage for modules using a hash chain.

    Features:
    - add_change(module, author, description, content_blob)
    - history(module) -> List[LineageEntry]
    - verify_module(module) -> True or raises RuntimeError if tampering detected
    """

    GENESIS = "GENESIS"

    def __init__(self):
        # module -> ordered list of LineageEntry
        self._store: Dict[str, List[LineageEntry]] = {}

    def _sha256(self, s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def _content_hash(self, content: str) -> str:
        return self._sha256(content)

    def _compute_entry_hash(
        self,
        prev_hash: str,
        module: str,
        ts: str,
        author: str,
        description: str,
        content_hash: str,
    ) -> str:
        payload = "|".join([prev_hash, module, ts, author, description, content_hash])
        return self._sha256(payload)

    def add_change(
        self, module: str, author: str, description: str, content: str
    ) -> LineageEntry:
        """Record a change for `module`. Content is hashed; only hashes are stored in the chain."""

        content_h = self._content_hash(content)
        prev = self._store.get(module)
        prev_hash = prev[-1].entry_hash if prev and len(prev) > 0 else self.GENESIS
        ts = datetime.now(timezone.utc).isoformat()
        entry_h = self._compute_entry_hash(
            prev_hash, module, ts, author, description, content_h
        )
        entry = LineageEntry(
            module=module,
            ts=ts,
            author=author,
            description=description,
            content_hash=content_h,
            prev_hash=prev_hash,
            entry_hash=entry_h,
        )
        self._store.setdefault(module, []).append(entry)
        return entry

    def history(self, module: str) -> List[LineageEntry]:
        return list(self._store.get(module, []))

    def verify_module(self, module: str) -> bool:
        """Verify the chain for `module`. Raises RuntimeError on any mismatch (tampering).

        The verification recomputes each entry hash and ensures prev_hash links match.
        """

        entries = self._store.get(module, [])
        prev_hash = self.GENESIS
        for idx, e in enumerate(entries):
            # recompute content hash and entry hash from stored fields
            recomputed = self._compute_entry_hash(
                e.prev_hash, e.module, e.ts, e.author, e.description, e.content_hash
            )
            if recomputed != e.entry_hash:
                raise RuntimeError(
                    f"tampering detected: entry_hash mismatch at index {idx} for module {module}"
                )
            # ensure prev linkage (consistency): for idx==0 prev_hash should equal GENESIS
            if e.prev_hash != prev_hash:
                raise RuntimeError(
                    f"tampering detected: prev_hash mismatch at index {idx} for module {module}"
                )
            prev_hash = e.entry_hash
        return True

    def export_chain(self, module: str) -> List[Dict[str, Any]]:
        return [asdict(e) for e in self.history(module)]


__all__ = ["LineageTracker", "LineageEntry"]
