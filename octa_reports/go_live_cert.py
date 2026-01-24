import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class GoLiveCert:
    system: str
    timestamp: str
    capital_status: Dict[str, Any]
    nav_validation: Dict[str, Any]
    fee_validation: Dict[str, Any]
    risk_governance: Dict[str, Any]
    open_risks: List[Dict[str, Any]]
    evidence_hash: str

    def to_json(self) -> str:
        # canonical JSON
        return json.dumps(
            asdict(self), sort_keys=True, separators=(",", ":"), default=str
        )


def generate_go_live_cert(
    system: str,
    capital_status: Dict[str, Any],
    nav_validation: Dict[str, Any],
    fee_validation: Dict[str, Any],
    risk_governance: Dict[str, Any],
    open_risks: Optional[List[Dict[str, Any]]] = None,
    timestamp: Optional[str] = None,
) -> GoLiveCert:
    """Generate an evidence-based go-live certification report.

    All inputs must be evidence-driven (prefer dicts containing `evidence_hash` keys).
    The certificate contains a deterministic `evidence_hash` that ties all inputs together.
    """
    open_risks = open_risks or []
    ts = timestamp or datetime.now(timezone.utc).isoformat()

    # Collect inputs for hashing; prefer provided evidence_hash values if present
    inputs = {
        "system": system,
        "timestamp": ts,
        "capital_status": capital_status,
        "nav_validation": nav_validation,
        "fee_validation": fee_validation,
        "risk_governance": risk_governance,
        "open_risks": open_risks,
    }

    # Build list of source evidence hashes when available
    source_hashes: List[str] = []
    for k in ("capital_status", "nav_validation", "fee_validation", "risk_governance"):
        v = inputs.get(k, {})
        if isinstance(v, dict) and "evidence_hash" in v:
            source_hashes.append(str(v["evidence_hash"]))
        else:
            # fall back to hashing the content
            source_hashes.append(canonical_hash(v))

    # include open_risks in hash
    if open_risks:
        source_hashes.append(canonical_hash(open_risks))

    # deterministic combined evidence hash
    combined = {"system": system, "timestamp": ts, "sources": sorted(source_hashes)}
    evidence_hash = canonical_hash(combined)

    cert = GoLiveCert(
        system=system,
        timestamp=ts,
        capital_status=capital_status,
        nav_validation=nav_validation,
        fee_validation=fee_validation,
        risk_governance=risk_governance,
        open_risks=open_risks,
        evidence_hash=evidence_hash,
    )

    return cert


def validate_certificate(cert: GoLiveCert) -> Dict[str, Any]:
    """Basic validation ensuring required evidence fields are present and hashes match content.

    Returns a dict with validation results; does not make policy judgements.
    """
    res: Dict[str, Any] = {"valid": True, "issues": []}

    # check presence of evidence_hash on major inputs (informational)
    for name, val in (
        ("capital_status", cert.capital_status),
        ("nav_validation", cert.nav_validation),
        ("fee_validation", cert.fee_validation),
        ("risk_governance", cert.risk_governance),
    ):
        if not isinstance(val, dict):
            res["valid"] = False
            res["issues"].append({"field": name, "issue": "missing_or_invalid"})
        else:
            # if evidence_hash present, accept; otherwise note
            if "evidence_hash" not in val:
                res["issues"].append(
                    {"field": name, "issue": "no_evidence_hash_present"}
                )

    # verify combined evidence_hash equals computed
    # recompute source_hashes
    source_hashes: List[str] = []
    for v in (
        cert.capital_status,
        cert.nav_validation,
        cert.fee_validation,
        cert.risk_governance,
    ):
        if isinstance(v, dict) and "evidence_hash" in v:
            source_hashes.append(str(v["evidence_hash"]))
        else:
            source_hashes.append(canonical_hash(v))
    if cert.open_risks:
        source_hashes.append(canonical_hash(cert.open_risks))
    combined = {
        "system": cert.system,
        "timestamp": cert.timestamp,
        "sources": sorted(source_hashes),
    }
    recomputed = canonical_hash(combined)
    if recomputed != cert.evidence_hash:
        res["valid"] = False
        res["issues"].append(
            {
                "field": "evidence_hash",
                "expected": recomputed,
                "found": cert.evidence_hash,
            }
        )

    return res
