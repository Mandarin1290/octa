from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import sha256_file


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _check_manifest_hashes(base_dir: Path, manifest_name: str) -> dict[str, Any]:
    manifest_path = base_dir / manifest_name
    if not manifest_path.exists():
        return {"ok": False, "checks": [{"name": f"manifest_exists:{manifest_name}", "status": "fail", "value": False, "threshold": True}]}
    manifest = _load_json(manifest_path)
    checks = [{
        "name": f"manifest_exists:{manifest_name}",
        "status": "pass",
        "value": True,
        "threshold": True,
    }]
    hashes = manifest.get("hashes", {})
    if not isinstance(hashes, dict):
        checks.append(
            {
                "name": f"manifest_hashes_mapping:{manifest_name}",
                "status": "fail",
                "value": type(hashes).__name__,
                "threshold": "dict",
            }
        )
        return {"ok": False, "checks": checks}
    for rel_name, expected_hash in sorted(hashes.items()):
        candidate = base_dir / rel_name
        exists = candidate.exists()
        checks.append(
            {
                "name": f"hashed_file_exists:{base_dir.name}:{rel_name}",
                "status": "pass" if exists else "fail",
                "value": exists,
                "threshold": True,
            }
        )
        if not exists:
            return {"ok": False, "checks": checks}
        actual_hash = sha256_file(candidate)
        ok = actual_hash == expected_hash
        checks.append(
            {
                "name": f"hash_integrity:{base_dir.name}:{rel_name}",
                "status": "pass" if ok else "fail",
                "value": actual_hash,
                "threshold": expected_hash,
            }
        )
        if not ok:
            return {"ok": False, "checks": checks}
    return {"ok": True, "checks": checks, "manifest": manifest}


def _check_export_manifest(export_dir: Path) -> dict[str, Any]:
    manifest_path = export_dir / "export_manifest.json"
    if not manifest_path.exists():
        return {"ok": False, "checks": [{"name": "export_manifest_exists", "status": "fail", "value": False, "threshold": True}]}
    manifest = _load_json(manifest_path)
    checks = [{
        "name": "export_manifest_exists",
        "status": "pass",
        "value": True,
        "threshold": True,
    }]
    files = manifest.get("files", {})
    if not isinstance(files, dict):
        checks.append({"name": "export_manifest_files_mapping", "status": "fail", "value": type(files).__name__, "threshold": "dict"})
        return {"ok": False, "checks": checks}
    for rel_name, meta in sorted(files.items()):
        if not isinstance(meta, dict):
            checks.append({"name": f"export_manifest_entry_type:{rel_name}", "status": "fail", "value": type(meta).__name__, "threshold": "dict"})
            return {"ok": False, "checks": checks}
        path = export_dir / rel_name
        exists = path.exists()
        checks.append({"name": f"export_file_exists:{rel_name}", "status": "pass" if exists else "fail", "value": exists, "threshold": True})
        if not exists:
            return {"ok": False, "checks": checks}
        expected_hash = str(meta.get("sha256", ""))
        actual_hash = sha256_file(path)
        ok = actual_hash == expected_hash
        checks.append({"name": f"export_hash_integrity:{rel_name}", "status": "pass" if ok else "fail", "value": actual_hash, "threshold": expected_hash})
        if not ok:
            return {"ok": False, "checks": checks}
    return {"ok": True, "checks": checks, "manifest": manifest}


def _contains_live_value(payload: Any) -> bool:
    if isinstance(payload, dict):
        return any(_contains_live_value(value) for value in payload.values())
    if isinstance(payload, (list, tuple)):
        return any(_contains_live_value(value) for value in payload)
    if isinstance(payload, str):
        normalized = payload.strip().upper()
        return normalized in {"LIVE", "MODE=LIVE", "LIVE_MODE", "LIVE_TRADING", "LIVE_ACCOUNT", "LIVE_ENDPOINT"}
    return False


def review_broker_paper_governance(
    inventory: dict[str, Any],
    *,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    risks: list[str] = []
    repo_path = Path(repo_root)

    chains = inventory.get("chains", [])
    has_negative_path = False
    has_positive_path = False
    all_chain_complete = True
    no_live_flags = True
    paper_only_enforced = True
    manifests_ok = True

    for chain in chains:
        chain_complete = bool(chain.get("chain_complete", False))
        all_chain_complete = all_chain_complete and chain_complete
        checks.append(
            {
                "name": f"chain_complete:{Path(chain['broker_paper_evidence_dir']).name}",
                "status": "pass" if chain_complete else "fail",
                "value": chain_complete,
                "threshold": True,
            }
        )

        status = str(chain.get("status", ""))
        if status == "gate_status=BROKER_PAPER_BLOCKED":
            has_negative_path = True
        if status == "BROKER_PAPER_SESSION_COMPLETED":
            has_positive_path = True

        references = chain.get("references", {})
        stage_paths = {
            "research_export": Path(str(references.get("research_export_path", ""))),
            "shadow": Path(str(references.get("shadow_evidence_dir", ""))),
            "promotion": Path(str(references.get("promotion_evidence_dir", ""))),
            "paper_gate": Path(str(references.get("paper_gate_evidence_dir", ""))),
            "paper_session": Path(str(references.get("paper_session_evidence_dir", ""))),
            "broker_paper": Path(chain["broker_paper_evidence_dir"]),
        }

        export_review = _check_export_manifest(stage_paths["research_export"])
        manifest_reviews = [
            _check_manifest_hashes(stage_paths["shadow"], "run_manifest.json"),
            _check_manifest_hashes(stage_paths["promotion"], "evidence_manifest.json"),
            _check_manifest_hashes(stage_paths["paper_gate"], "evidence_manifest.json"),
            _check_manifest_hashes(stage_paths["paper_session"], "evidence_manifest.json"),
            _check_manifest_hashes(stage_paths["broker_paper"], "evidence_manifest.json"),
        ]
        for review in [export_review, *manifest_reviews]:
            checks.extend(review["checks"])
            manifests_ok = manifests_ok and review["ok"]

        broker_report = _load_json(stage_paths["broker_paper"] / "broker_paper_report.json")
        broker_policy_path = stage_paths["broker_paper"] / "applied_broker_paper_policy.json"
        paper_session_policy_path = stage_paths["paper_session"] / "session_policy.json"
        broker_policy_payload = _load_json(broker_policy_path) if broker_policy_path.exists() else {}
        paper_session_policy_payload = _load_json(paper_session_policy_path) if paper_session_policy_path.exists() else {}
        if (
            _contains_live_value(broker_report.get("policy", {}))
            or _contains_live_value(broker_policy_payload)
            or _contains_live_value(paper_session_policy_payload)
        ):
            no_live_flags = False
        policy = broker_report.get("policy", {})
        if not isinstance(policy, dict):
            paper_only_enforced = False
        else:
            mode_ok = str(policy.get("require_broker_mode", "")) == "PAPER"
            forbid_live_ok = bool(policy.get("forbid_live_mode", False)) is True
            paper_only_enforced = paper_only_enforced and mode_ok and forbid_live_ok
            checks.append(
                {
                    "name": f"paper_mode_policy:{stage_paths['broker_paper'].name}",
                    "status": "pass" if mode_ok else "fail",
                    "value": policy.get("require_broker_mode"),
                    "threshold": "PAPER",
                }
            )
            checks.append(
                {
                    "name": f"forbid_live_policy:{stage_paths['broker_paper'].name}",
                    "status": "pass" if forbid_live_ok else "fail",
                    "value": policy.get("forbid_live_mode"),
                    "threshold": True,
                }
            )

    code_checks = {
        "adapter_paper_guard": ("octa/core/broker_paper/broker_paper_adapter.py", 'mode != "PAPER"'),
        "session_validation_paper_guard": ("octa/core/broker_paper/broker_paper_session_validation.py", 'require_broker_mode'),
        "kill_switch_path_present": ("octa/core/broker_paper/broker_paper_session.py", "kill_switch"),
        "max_position_path_present": ("octa/core/broker_paper/broker_paper_session_validation.py", "max_open_positions"),
        "kill_switch_test_present": ("tests/test_broker_paper_session.py", "kill_switch"),
        "adapter_mode_test_present": ("tests/test_broker_paper_session.py", "mode=\"LIVE\""),
    }
    code_results: dict[str, bool] = {}
    for name, (rel_path, needle) in code_checks.items():
        path = repo_path / rel_path
        ok = path.exists() and needle in path.read_text(encoding="utf-8")
        code_results[name] = ok
        checks.append(
            {
                "name": name,
                "status": "pass" if ok else "fail",
                "value": ok,
                "threshold": True,
            }
        )

    kill_switch_path_tested = code_results["kill_switch_path_present"] and code_results["kill_switch_test_present"]
    limits_path_tested = code_results["max_position_path_present"]
    paper_only_enforced = paper_only_enforced and code_results["adapter_paper_guard"] and code_results["session_validation_paper_guard"]
    no_live_flags = no_live_flags and code_results["adapter_mode_test_present"]

    checks.append({"name": "negative_path_proof", "status": "pass" if has_negative_path else "fail", "value": has_negative_path, "threshold": True})
    checks.append({"name": "positive_path_proof", "status": "pass" if has_positive_path else "fail", "value": has_positive_path, "threshold": True})
    checks.append({"name": "hash_integrity_all_manifests", "status": "pass" if manifests_ok else "fail", "value": manifests_ok, "threshold": True})
    checks.append({"name": "no_live_flags_detected", "status": "pass" if no_live_flags else "fail", "value": no_live_flags, "threshold": True})
    checks.append({"name": "paper_only_enforced", "status": "pass" if paper_only_enforced else "fail", "value": paper_only_enforced, "threshold": True})
    checks.append({"name": "kill_switch_path_tested", "status": "pass" if kill_switch_path_tested else "fail", "value": kill_switch_path_tested, "threshold": True})
    checks.append({"name": "limits_path_tested", "status": "pass" if limits_path_tested else "fail", "value": limits_path_tested, "threshold": True})

    if not has_negative_path:
        risks.append("No real blocked broker-paper path found.")
    if not has_positive_path:
        risks.append("No real completed broker-paper session found.")
    if not manifests_ok:
        risks.append("At least one referenced evidence manifest failed integrity validation.")
    if not no_live_flags:
        risks.append("A LIVE flag or live-mode marker was detected in broker-paper evidence or policy.")
    if not paper_only_enforced:
        risks.append("Explicit PAPER-only enforcement is not fully evidenced in code and evidence.")
    if not all_chain_complete:
        risks.append("At least one broker-paper evidence chain is incomplete.")

    status = "ok" if all(item["status"] == "pass" for item in checks) else "fail"
    return {
        "status": status,
        "checks": checks,
        "summary": {
            "negative_path_proof": has_negative_path,
            "positive_path_proof": has_positive_path,
            "paper_only_enforced": paper_only_enforced,
            "no_live_flags": no_live_flags,
            "kill_switch_path_tested": kill_switch_path_tested,
            "limits_path_tested": limits_path_tested,
            "all_chain_complete": all_chain_complete,
            "manifests_ok": manifests_ok,
            "critical_risks": risks,
        },
    }


__all__ = ["review_broker_paper_governance"]
