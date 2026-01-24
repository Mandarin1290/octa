from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


def _parse_version(v: str) -> Tuple[int, int, int]:
    parts = (v or "").split(".")
    a = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
    b = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    c = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    return (a, b, c)


def _version_to_str(t: Tuple[int, int, int]) -> str:
    return f"{t[0]}.{t[1]}.{t[2]}"


@dataclass
class Contract:
    name: str
    version: str
    input_schema: Dict[
        str, Dict[str, object]
    ]  # field -> {'type': 'int', 'required': True}
    output_schema: Dict[str, Dict[str, object]]

    def parsed_version(self) -> Tuple[int, int, int]:
        return _parse_version(self.version)


class ContractRegistry:
    def __init__(self):
        self._contracts: Dict[str, List[Contract]] = {}

    def register(self, contract: Contract) -> None:
        lst = self._contracts.setdefault(contract.name, [])
        lst.append(contract)

    def latest(self, name: str) -> Optional[Contract]:
        lst = self._contracts.get(name, [])
        if not lst:
            return None
        # choose highest version by parsed tuple
        return max(lst, key=lambda c: c.parsed_version())

    def is_compatible(self, old: Contract, new: Contract) -> Tuple[bool, str, str]:
        """Return (compatible, reason, required_bump)

        Rules:
        - For each required input field in old, new must contain same field with same type.
        - For outputs, required outputs must remain with same type.
        - Adding optional fields is non-breaking (minor bump suggested).
        - Changing type or removing required field is breaking (major bump suggested).
        - If only metadata or patch-level changes, return patch.
        """
        # inputs
        for f, meta in old.input_schema.items():
            if meta.get("required", False):
                if f not in new.input_schema:
                    return False, f"required input '{f}' removed", "major"
                if new.input_schema[f].get("type") != meta.get("type"):
                    return False, f"input '{f}' type changed", "major"

        # outputs
        for f, meta in old.output_schema.items():
            if meta.get("required", False):
                if f not in new.output_schema:
                    return False, f"required output '{f}' removed", "major"
                if new.output_schema[f].get("type") != meta.get("type"):
                    return False, f"output '{f}' type changed", "major"

        # if we reach here, no breaking removal/type changes
        # detect additions
        added_inputs = set(new.input_schema.keys()) - set(old.input_schema.keys())
        added_outputs = set(new.output_schema.keys()) - set(old.output_schema.keys())
        if added_inputs or added_outputs:
            return True, "added optional fields", "minor"

        return True, "compatible (patch)", "patch"


__all__ = ["Contract", "ContractRegistry"]
