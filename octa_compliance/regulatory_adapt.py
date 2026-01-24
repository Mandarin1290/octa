from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
    )


def _sha256(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class Rule:
    rule_id: str
    version: str
    jurisdiction: str
    effective_date: str
    content: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent: Optional[str] = None
    evidence_hash: Optional[str] = None

    def to_canonical(self) -> Dict:
        return {
            "rule_id": self.rule_id,
            "version": self.version,
            "jurisdiction": self.jurisdiction,
            "effective_date": self.effective_date,
            "content": self.content,
            "metadata": self.metadata,
            "parent": self.parent,
        }


@dataclass
class EvolutionEntry:
    index: int
    timestamp: str
    user: str
    action: str
    details: Dict[str, Any]
    prev_hash: Optional[str]
    entry_hash: str


class RegulatoryAdaptation:
    """Layer to manage regulatory rules, versions and a compliance evolution log.

    - Rule abstraction with jurisdiction tagging and version lineage.
    - Compatibility checks before accepting new rule versions.
    - Append-only evolution log with hash chain and verification.
    """

    def __init__(self) -> None:
        # rule_id -> version -> Rule
        self._rules: Dict[str, Dict[str, Rule]] = {}
        self._evolution_log: List[EvolutionEntry] = []

    def _append_log(
        self, user: str, action: str, details: Dict[str, Any]
    ) -> EvolutionEntry:
        prev = self._evolution_log[-1].entry_hash if self._evolution_log else None
        idx = len(self._evolution_log)
        ts = _now_iso()
        payload = {
            "index": idx,
            "timestamp": ts,
            "user": user,
            "action": action,
            "details": details,
            "prev_hash": prev,
        }
        ehash = _sha256(payload)
        entry = EvolutionEntry(
            index=idx,
            timestamp=ts,
            user=user,
            action=action,
            details=details,
            prev_hash=prev,
            entry_hash=ehash,
        )
        self._evolution_log.append(entry)
        return entry

    def add_rule(self, user: str, rule: Rule) -> str:
        """Add a new root rule (no parent). Returns evidence_hash."""
        if rule.rule_id not in self._rules:
            self._rules[rule.rule_id] = {}
        if rule.version in self._rules[rule.rule_id]:
            raise ValueError("rule version already exists")
        rule.evidence_hash = _sha256({"rule_id": rule.rule_id, **rule.to_canonical()})
        self._rules[rule.rule_id][rule.version] = rule
        self._append_log(
            user=user,
            action="add_rule",
            details={
                "rule_id": rule.rule_id,
                "version": rule.version,
                "evidence_hash": rule.evidence_hash,
            },
        )
        return rule.evidence_hash

    def add_rule_version(
        self,
        user: str,
        rule_id: str,
        new_rule: Rule,
        compatibility_mode: str = "strict",
    ) -> str:
        """Add a new version for existing rule_id with compatibility check.

        compatibility_mode: 'strict' enforces required_fields preserved; 'lenient' allows additions.
        """
        if rule_id not in self._rules:
            raise ValueError("unknown rule_id")
        # parent must exist if specified
        parent_ver = new_rule.parent
        if parent_ver is None:
            raise ValueError("new version must specify parent")
        if parent_ver not in self._rules[rule_id]:
            raise ValueError("parent version not found")

        parent = self._rules[rule_id][parent_ver]
        # perform compatibility check
        if not self._is_compatible(parent, new_rule, mode=compatibility_mode):
            raise ValueError("new rule version is not backward compatible")

        new_rule.evidence_hash = _sha256(
            {"rule_id": rule_id, **new_rule.to_canonical()}
        )
        self._rules[rule_id][new_rule.version] = new_rule
        self._append_log(
            user=user,
            action="add_rule_version",
            details={
                "rule_id": rule_id,
                "version": new_rule.version,
                "parent": parent_ver,
                "evidence_hash": new_rule.evidence_hash,
            },
        )
        return new_rule.evidence_hash

    def _is_compatible(
        self, parent: Rule, new_rule: Rule, mode: str = "strict"
    ) -> bool:
        # Compatibility heuristic: if parent.metadata contains 'required_fields', new_rule.content must still include them
        req = parent.metadata.get("required_fields") or []
        if mode == "lenient":
            return True
        # strict mode
        for f in req:
            if f not in new_rule.content:
                return False
        return True

    def get_rule(self, rule_id: str, version: str) -> Rule:
        return self._rules[rule_id][version]

    def latest_version(self, rule_id: str) -> Optional[str]:
        if rule_id not in self._rules:
            return None
        # assume lexical sort of version keys is acceptable for simplicity
        return sorted(self._rules[rule_id].keys())[-1]

    def evolution_log(self) -> List[Dict[str, Any]]:
        return [asdict(e) for e in self._evolution_log]

    def verify_evolution_log(self) -> bool:
        prev = None
        for e in self._evolution_log:
            payload = {
                "index": e.index,
                "timestamp": e.timestamp,
                "user": e.user,
                "action": e.action,
                "details": e.details,
                "prev_hash": e.prev_hash,
            }
            if _sha256(payload) != e.entry_hash:
                return False
            if e.prev_hash != prev:
                return False
            prev = e.entry_hash
        return True
