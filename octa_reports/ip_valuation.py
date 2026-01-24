from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


def _canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class ValuationReport:
    scores: Dict[str, float]
    composite: float
    evidence: Dict[str, Any]
    report_hash: str


class ValuationFramework:
    """Compute a non-financial valuation profile for OCTA IP.

    Metrics (no revenue / no multiples):
    - `codebase_complexity`: heuristic based on file count and LOC.
    - `autonomy_depth`: degree of automated controls and retrains (heuristic from provided subsystems).
    - `governance_coverage`: presence of governance artifacts and review frequency heuristics.
    - `survivability_evidence`: auditability, evidence hashes, and redundancy signals.

    The class is conservative: missing subsystems lower scores, and no speculative assumptions are made.
    """

    def __init__(
        self,
        repo_path: Optional[str] = None,
        subsystems: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.repo_path = repo_path
        self.subsystems = subsystems or {}

    def _score_codebase(self) -> Dict[str, Any]:
        # Heuristic: count python files and total lines; dampen by large monoliths
        if not self.repo_path or not os.path.isdir(self.repo_path):
            return {"score": 0.0, "meta": {"reason": "no repo_path"}}
        py_files = []
        total_lines = 0
        for root, _, files in os.walk(self.repo_path):
            for f in files:
                if f.endswith(".py"):
                    p = os.path.join(root, f)
                    py_files.append(p)
                    try:
                        with open(p, "r", encoding="utf-8") as fh:
                            lines = fh.readlines()
                    except Exception:
                        lines = []
                    total_lines += len(lines)
        file_count = len(py_files)
        # normalize: ideal range 50-200 files (score plateau), lines 1k-100k
        fc_norm = min(1.0, file_count / 200.0)
        loc_norm = min(1.0, total_lines / 100000.0)
        # complexity penalizes extreme LOC per file (monoliths)
        avg_lines = (total_lines / file_count) if file_count else 0
        monolith_penalty = 0.0
        if avg_lines > 2000:
            monolith_penalty = min(0.5, (avg_lines - 2000) / 10000.0)
        score = max(0.0, (0.6 * fc_norm + 0.4 * loc_norm) - monolith_penalty)
        return {
            "score": round(score * 100, 2),
            "meta": {
                "files": file_count,
                "loc": total_lines,
                "avg_lines": int(avg_lines),
            },
        }

    def _score_autonomy(self) -> Dict[str, Any]:
        # Heuristic from subsystems: presence of `model_refresh`, `sunset`, `continuous_audit` increases autonomy
        keys = set(self.subsystems.keys())
        points = 0
        reasons = []
        if "model_refresh" in keys:
            points += 2
            reasons.append("model_refresh")
        if "sunset" in keys:
            points += 1
            reasons.append("sunset")
        if "continuous_audit" in keys:
            points += 1
            reasons.append("continuous_audit")
        if "governance" in keys:
            points += 1
            reasons.append("governance_integration")
        # normalize to 0-100
        score = min(1.0, points / 5.0)
        return {
            "score": round(score * 100, 2),
            "meta": {"components": reasons, "points": points},
        }

    def _score_governance(self) -> Dict[str, Any]:
        gov = self.subsystems.get("governance")
        if not gov:
            return {"score": 0.0, "meta": {"reason": "no governance subsystem"}}
        # Expect governance to expose `review_frequencies` or `audit_trail` length
        freq = getattr(gov, "review_frequencies", None)
        trail_len = 0
        try:
            if hasattr(gov, "audit_trail"):
                trail = gov.audit_trail()
                trail_len = len(trail) if hasattr(trail, "__len__") else 0
        except Exception:
            trail_len = 0
        # heuristic: more frequent reviews and non-empty trail -> higher coverage
        freq_score = 0.5
        if isinstance(freq, dict):
            # assume keys like daily/weekly/monthly with booleans or ints
            freq_score = min(
                1.0, sum(1.0 for v in freq.values() if v) / max(1.0, len(freq))
            )
        trail_score = min(1.0, trail_len / 100.0)
        score = 0.6 * freq_score + 0.4 * trail_score
        return {"score": round(score * 100, 2), "meta": {"trail_len": trail_len}}

    def _score_survivability(self) -> Dict[str, Any]:
        # Based on audit evidence counts and registry evidence hashes
        audit = self.subsystems.get("audit")
        registry = self.subsystems.get("registry")
        evidence_count = 0
        if audit and hasattr(audit, "list_logs"):
            try:
                evidence_count += len(audit.list_logs())
            except Exception:
                pass
        if registry and hasattr(registry, "dependency_graph"):
            try:
                evidence_count += len(registry.dependency_graph())
            except Exception:
                pass
        score = min(1.0, evidence_count / 200.0)
        return {
            "score": round(score * 100, 2),
            "meta": {"evidence_count": evidence_count},
        }

    def generate_report(self) -> ValuationReport:
        c = self._score_codebase()
        a = self._score_autonomy()
        g = self._score_governance()
        s = self._score_survivability()

        scores = {
            "codebase_complexity": c["score"],
            "autonomy_depth": a["score"],
            "governance_coverage": g["score"],
            "survivability_evidence": s["score"],
        }

        # composite: geometric mean style (conservative) after normalizing to 0-1
        vals = [max(0.0, min(100.0, v)) / 100.0 for v in scores.values()]
        # avoid zeroing everything; if all zero, composite zero
        if any(vals):
            prod = 1.0
            for v in vals:
                prod *= v if v > 0 else 0.0001
            composite = (prod ** (1.0 / len(vals))) * 100.0
        else:
            composite = 0.0

        evidence = {
            "codebase": c["meta"],
            "autonomy": a["meta"],
            "governance": g["meta"],
            "survivability": s["meta"],
        }

        report_obj = {
            "scores": scores,
            "composite": round(composite, 2),
            "evidence": evidence,
        }
        report_hash = _hash(report_obj)

        return ValuationReport(
            scores=scores,
            composite=round(composite, 2),
            evidence=evidence,
            report_hash=report_hash,
        )
