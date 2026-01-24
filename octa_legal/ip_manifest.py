import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class IPItem:
    name: str
    category: str
    owner: str
    included: bool
    version: str
    description: str
    evidence_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


class IPManifest:
    """Machine‑readable IP manifest for audit, valuation and licensing.

    Example usage:

        item = IPItem(name='alpha_decay', category='core_algorithms', owner='Quant', included=True, version='v1.0', description='alpha decay detector')
        manifest = IPManifest(generated_by='legal@octa')
        manifest.add_item(item)
        m = manifest.build()

    The manifest contains canonical evidence_hash computed over the manifest content.
    """

    def __init__(self, generated_by: str):
        self.generated_by = generated_by
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.items: List[IPItem] = []

    def add_item(self, item: IPItem) -> None:
        # compute evidence hash for the item (name+version+category+description)
        content = {
            "name": item.name,
            "version": item.version,
            "category": item.category,
            "description": item.description,
            "owner": item.owner,
        }
        item.evidence_hash = canonical_hash(content)
        self.items.append(item)

    def build(self) -> Dict[str, Any]:
        payload = {
            "generated_by": self.generated_by,
            "generated_at": self.generated_at,
            "items": [it.to_dict() for it in self.items],
        }
        manifest_hash = canonical_hash(payload)
        payload["manifest_hash"] = manifest_hash
        return payload

    def to_json(self) -> str:
        return json.dumps(self.build(), sort_keys=True, separators=(",", ":"))
