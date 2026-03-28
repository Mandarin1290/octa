from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping


STAGE_PREFIXES = {
    "shadow": "shadow_run_",
    "promotion": "promotion_run_",
    "paper_gate": "paper_gate_",
    "paper_session": "paper_session_",
    "broker_paper": "broker_paper_",
}

STAGE_REQUIRED_FILES = {
    "research_export": ("export_manifest.json", "signals.parquet", "returns.parquet", "metadata.json"),
    "shadow": ("run_manifest.json", "shadow_config.json", "metrics.json", "trades.parquet", "equity_curve.parquet"),
    "promotion": ("decision_report.json", "applied_policy.json", "evidence_manifest.json"),
    "paper_gate": ("paper_gate_report.json", "applied_paper_policy.json", "evidence_manifest.json"),
    "paper_session": ("paper_session_report.json", "session_manifest.json", "session_policy.json", "evidence_manifest.json"),
    "broker_paper": ("broker_paper_report.json", "applied_broker_paper_policy.json", "evidence_manifest.json"),
}


def _iter_stage_dirs(root: Path, prefix: str, marker_file: str | None = None):
    for path in sorted(root.glob(f"{prefix}*")):
        if not path.is_dir():
            continue
        if marker_file is not None and not (path / marker_file).exists():
            continue
        yield path


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _normalize_roots(evidence_roots: Mapping[str, Any] | Iterable[str | Path] | str | Path) -> dict[str, str]:
    if isinstance(evidence_roots, Mapping):
        evidence_root = Path(str(evidence_roots["evidence_root"])).resolve()
        research_export_root = Path(
            str(evidence_roots.get("research_export_root", evidence_root.parent / "research_exports"))
        ).resolve()
        return {
            "evidence_root": str(evidence_root),
            "research_export_root": str(research_export_root),
        }
    if isinstance(evidence_roots, (str, Path)):
        evidence_root = Path(evidence_roots).resolve()
        return {
            "evidence_root": str(evidence_root),
            "research_export_root": str((evidence_root.parent / "research_exports").resolve()),
        }
    roots = [Path(str(item)).resolve() for item in evidence_roots]
    if not roots:
        raise ValueError("evidence_roots must not be empty")
    return {
        "evidence_root": str(roots[0]),
        "research_export_root": str((roots[1] if len(roots) > 1 else roots[0].parent / "research_exports").resolve()),
    }


def _stage_presence(path: str | None, stage: str) -> dict[str, Any]:
    if path is None:
        return {
            "path": None,
            "exists": False,
            "missing_files": list(STAGE_REQUIRED_FILES[stage]),
            "present_files": [],
        }
    resolved = Path(path)
    if stage == "research_export":
        exists = resolved.exists() and resolved.is_dir()
    else:
        exists = resolved.exists() and resolved.is_dir()
    present_files = []
    missing_files = []
    if exists:
        for name in STAGE_REQUIRED_FILES[stage]:
            if (resolved / name).exists():
                present_files.append(name)
            else:
                missing_files.append(name)
    else:
        missing_files = list(STAGE_REQUIRED_FILES[stage])
    return {
        "path": str(resolved) if path is not None else None,
        "exists": exists,
        "present_files": present_files,
        "missing_files": missing_files,
    }


def build_broker_paper_readiness_inventory(
    evidence_roots: Mapping[str, Any] | Iterable[str | Path] | str | Path,
) -> dict[str, Any]:
    roots = _normalize_roots(evidence_roots)
    evidence_root = Path(roots["evidence_root"])
    research_export_root = Path(roots["research_export_root"])
    discovered = {
        "research_exports": sorted(str(path.resolve()) for path in research_export_root.glob("research_bridge_*") if path.is_dir()),
    }
    stage_markers = {
        "shadow": "run_manifest.json",
        "promotion": "decision_report.json",
        "paper_gate": "paper_gate_report.json",
        "paper_session": "paper_session_report.json",
        "broker_paper": "broker_paper_report.json",
    }
    for stage, prefix in STAGE_PREFIXES.items():
        discovered[stage] = sorted(str(path.resolve()) for path in _iter_stage_dirs(evidence_root, prefix, stage_markers[stage]))

    chains: list[dict[str, Any]] = []
    for broker_dir in _iter_stage_dirs(evidence_root, "broker_paper_", "broker_paper_report.json"):
        report_path = broker_dir / "broker_paper_report.json"
        if not report_path.exists():
            chains.append(
                {
                    "broker_paper_evidence_dir": str(broker_dir.resolve()),
                    "status": "unreadable",
                    "chain_complete": False,
                    "error": "missing broker_paper_report.json",
                }
            )
            continue

        report = _load_json(report_path)
        references = report.get("references", {})
        if not isinstance(references, dict):
            references = {}
        chain = {
            "broker_paper_evidence_dir": str(broker_dir.resolve()),
            "status": (
                str(report.get("blocked_reason"))
                if report.get("blocked_reason") is not None
                else str(report.get("session_summary", {}).get("status", report.get("gate_result", {}).get("status", "")))
            ),
            "stages": {
                "research_export": _stage_presence(references.get("research_export_path"), "research_export"),
                "shadow": _stage_presence(references.get("shadow_evidence_dir"), "shadow"),
                "promotion": _stage_presence(references.get("promotion_evidence_dir"), "promotion"),
                "paper_gate": _stage_presence(references.get("paper_gate_evidence_dir"), "paper_gate"),
                "paper_session": _stage_presence(references.get("paper_session_evidence_dir"), "paper_session"),
                "broker_paper": _stage_presence(str(broker_dir.resolve()), "broker_paper"),
            },
            "references": references,
        }
        chain["chain_complete"] = all(
            stage_info["exists"] and not stage_info["missing_files"] for stage_info in chain["stages"].values()
        )
        chains.append(chain)

    return {
        "roots": roots,
        "discovered": discovered,
        "chains": chains,
        "summary": {
            "n_broker_paper_runs": len(chains),
            "n_complete_chains": sum(1 for chain in chains if chain["chain_complete"]),
            "n_incomplete_chains": sum(1 for chain in chains if not chain["chain_complete"]),
        },
    }


__all__ = ["build_broker_paper_readiness_inventory"]
