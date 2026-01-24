from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
    )


def _sha256_obj(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class MasterDossier:
    repo_path: Optional[str] = None
    subsystems: Dict[str, Any] | None = None

    def __post_init__(self):
        self.subsystems = self.subsystems or {}
        self._last_dossier: Optional[Dict[str, Any]] = None

    def _gather_architecture(self) -> Dict[str, Any]:
        arch: Dict[str, Any] = {"generated_at": _now_iso()}
        if not self.repo_path or not os.path.isdir(self.repo_path):
            arch["note"] = "no_repo_path"
            return arch

        files = []
        total_lines = 0
        py_count = 0
        for root, _, filenames in os.walk(self.repo_path):
            for f in filenames:
                if f.endswith(".py"):
                    py_count += 1
                    p = os.path.join(root, f)
                    files.append(os.path.relpath(p, self.repo_path))
                    try:
                        with open(p, "r", encoding="utf-8") as fh:
                            total_lines += sum(1 for _ in fh)
                    except Exception:
                        pass
        arch["python_files_count"] = py_count
        arch["total_loc"] = total_lines
        arch["sample_files"] = files[:50]
        arch["evidence_hash"] = _sha256_obj({"py_count": py_count, "loc": total_lines})
        return arch

    def _gather_ip_registry(self) -> Dict[str, Any]:
        reg = (self.subsystems or {}).get("ip_registry")
        out: Dict[str, Any] = {"generated_at": _now_iso()}
        if not reg:
            out["note"] = "no_registry"
            return out
        try:
            dg = reg.dependency_graph()
            nodes = list(dg.keys())
            out["nodes_count"] = len(nodes)
            out["sample_nodes"] = nodes[:50]
            out["evidence_hash"] = _sha256_obj({"nodes_count": len(nodes)})
        except Exception as e:
            out["error"] = str(e)
        return out

    def _gather_audit_readiness(self) -> Dict[str, Any]:
        audit = (self.subsystems or {}).get("audit")
        out: Dict[str, Any] = {"generated_at": _now_iso()}
        if not audit:
            out["note"] = "no_audit_subsystem"
            return out
        try:
            logs = audit.list_logs()
            out["log_count"] = len(logs)
            out["latest_entry"] = logs[-1] if logs else None
            out["verify"] = getattr(audit, "verify_logs", lambda: False)()
            out["evidence_hash"] = _sha256_obj(
                {"log_count": len(logs), "verify": out["verify"]}
            )
        except Exception as e:
            out["error"] = str(e)
        return out

    def _gather_governance(self) -> Dict[str, Any]:
        gov = (self.subsystems or {}).get("governance")
        out: Dict[str, Any] = {"generated_at": _now_iso()}
        if not gov:
            out["note"] = "no_governance"
            return out
        try:
            trail = gov.audit_trail() if hasattr(gov, "audit_trail") else None
            out["audit_trail_len"] = len(trail) if trail is not None else None
            manifest = (
                gov.export_manifest() if hasattr(gov, "export_manifest") else None
            )
            out["manifest_hash"] = (
                manifest.get("evidence_hash") if isinstance(manifest, dict) else None
            )
            out["evidence_hash"] = _sha256_obj(
                {
                    "trail_len": out["audit_trail_len"],
                    "manifest_hash": out["manifest_hash"],
                }
            )
        except Exception as e:
            out["error"] = str(e)
        return out

    def _gather_longevity(self) -> Dict[str, Any]:
        lon = (self.subsystems or {}).get("longevity")
        out: Dict[str, Any] = {"generated_at": _now_iso()}
        if not lon:
            out["note"] = "no_longevity_subsystem"
            return out
        try:
            if hasattr(lon, "generate_longevity_cert"):
                cert = lon.generate_longevity_cert()
            elif callable(lon):
                cert = lon()
            else:
                cert = getattr(lon, "cert", {})
            out["cert_summary"] = (
                cert if isinstance(cert, dict) else {"note": str(cert)}
            )
            out["evidence_hash"] = _sha256_obj(out["cert_summary"])
        except Exception as e:
            out["error"] = str(e)
        return out

    def generate(self) -> Dict[str, Any]:
        dossier: Dict[str, Any] = {"generated_at": _now_iso(), "sections": {}}
        dossier["sections"]["architecture"] = self._gather_architecture()
        dossier["sections"]["ip_registry_summary"] = self._gather_ip_registry()
        dossier["sections"]["audit_readiness"] = self._gather_audit_readiness()
        dossier["sections"]["governance_proof"] = self._gather_governance()
        dossier["sections"]["longevity_cert"] = self._gather_longevity()

        dossier["dossier_hash"] = _sha256_obj(dossier["sections"])
        # cache last generated dossier so exports remain stable if invoked immediately
        self._last_dossier = dossier
        return dossier

    def export_json(self, path: str) -> str:
        # if a dossier was recently generated, reuse it to keep exported manifest stable
        dossier = self._last_dossier or self.generate()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(dossier, fh, sort_keys=True, indent=2, ensure_ascii=False)
        return path
