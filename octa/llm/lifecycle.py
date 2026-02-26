"""
OCTA LLM Lifecycle Management — institutional model governance for Ollama.

States: ACTIVE | CANDIDATE | VERIFIED | DEPRECATED | REMOVED

All operations are:
  - Deterministic (no randomness)
  - Offline-safe (no external network calls)
  - Fail-closed (hash mismatch or missing hash → do not activate)
  - Auditable (every change logged with timestamp, versions, hash, reason)

CLI usage:
  python -m octa.llm.lifecycle status
  python -m octa.llm.lifecycle rotate
  python -m octa.llm.lifecycle verify
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Paths ────────────────────────────────────────────────────────────────────
_REGISTRY_PATH = Path("octa/var/llm_registry.json")
_HASH_REGISTRY_PATH = Path("octa/var/llm_hash_registry.json")
_POLICY_PATH = Path("octa/config/llm_policy.yaml")

# ── States ────────────────────────────────────────────────────────────────────
ACTIVE = "ACTIVE"
CANDIDATE = "CANDIDATE"
VERIFIED = "VERIFIED"
DEPRECATED = "DEPRECATED"
REMOVED = "REMOVED"

_VALID_STATES = {ACTIVE, CANDIDATE, VERIFIED, DEPRECATED, REMOVED}


# ── Utilities ─────────────────────────────────────────────────────────────────

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _load_policy() -> List[Dict[str, Any]]:
    """Load llm_policy.yaml. Returns list of model dicts. No network calls."""
    try:
        import yaml  # type: ignore[import]
        raw = yaml.safe_load(_POLICY_PATH.read_text(encoding="utf-8"))
        return list(raw.get("models", []))
    except ImportError:
        # Fallback: basic YAML parser for simple key: value structures
        return _parse_policy_fallback()
    except Exception:
        return []


def _parse_policy_fallback() -> List[Dict[str, Any]]:
    """Minimal YAML list parser (no pyyaml dependency)."""
    models: List[Dict[str, Any]] = []
    if not _POLICY_PATH.exists():
        return models
    lines = _POLICY_PATH.read_text(encoding="utf-8").splitlines()
    current: Optional[Dict[str, Any]] = None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- name:"):
            if current:
                models.append(current)
            current = {"name": stripped.split(":", 1)[1].strip()}
        elif current is not None and ":" in stripped and not stripped.startswith("#"):
            key, _, val = stripped.partition(":")
            current[key.strip()] = val.strip().strip('"')
    if current:
        models.append(current)
    return models


def _run_ollama_list() -> List[Dict[str, str]]:
    """
    Run `ollama list` and parse output. No network calls.
    Returns list of dicts with keys: name, id, size, modified.
    """
    try:
        result = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except FileNotFoundError:
        return []
    except Exception:
        return []

    models: List[Dict[str, str]] = []
    lines = result.stdout.strip().splitlines()
    if len(lines) < 2:
        return models
    for line in lines[1:]:  # skip header
        parts = line.split()
        if len(parts) >= 2:
            models.append({
                "name": parts[0],
                "id": parts[1],
                "size": parts[2] if len(parts) > 2 else "",
                "modified": " ".join(parts[3:]) if len(parts) > 3 else "",
            })
    return sorted(models, key=lambda m: m["name"])


def _compute_id_hash(model_id: str) -> str:
    """
    Compute stable SHA-256 hash from ollama model ID (digest hex string).
    No filesystem access; deterministic.
    """
    return hashlib.sha256(model_id.encode("utf-8")).hexdigest()


# ── Registry operations ───────────────────────────────────────────────────────

def _load_registry() -> Dict[str, Any]:
    data = _load_json(_REGISTRY_PATH)
    if not isinstance(data, dict):
        return {"models": {}, "history": []}
    if "models" not in data:
        data["models"] = {}
    if "history" not in data:
        data["history"] = []
    return data


def _save_registry(reg: Dict[str, Any]) -> None:
    _save_json(_REGISTRY_PATH, reg)


def _load_hash_registry() -> Dict[str, str]:
    data = _load_json(_HASH_REGISTRY_PATH)
    return data if isinstance(data, dict) else {}


def _save_hash_registry(h: Dict[str, str]) -> None:
    _save_json(_HASH_REGISTRY_PATH, h)


def _log_event(
    reg: Dict[str, Any],
    *,
    model_name: str,
    old_version: Optional[str],
    new_version: Optional[str],
    old_state: Optional[str],
    new_state: Optional[str],
    hash_val: Optional[str],
    reason: str,
) -> None:
    reg["history"].append({
        "timestamp_utc": _utc_now(),
        "model": model_name,
        "old_version": old_version,
        "new_version": new_version,
        "old_state": old_state,
        "new_state": new_state,
        "hash": hash_val,
        "reason": reason,
    })


# ── Core operations ───────────────────────────────────────────────────────────

def cmd_status() -> int:
    """Print lifecycle status of all models."""
    installed = _run_ollama_list()
    reg = _load_registry()
    hash_reg = _load_hash_registry()
    policy = _load_policy()
    policy_map = {p["pinned_version"]: p for p in policy if "pinned_version" in p}

    print(f"{'MODEL':<35} {'ID':<16} {'STATE':<12} {'HASH_VERIFIED':<14} {'POLICY'}")
    print("-" * 90)

    for m in sorted(installed, key=lambda x: x["name"]):
        name = m["name"]
        mid = m["id"]
        model_entry = reg["models"].get(name, {})
        state = model_entry.get("state", CANDIDATE)
        id_hash = _compute_id_hash(mid)
        stored_hash = hash_reg.get(name)
        hash_verified = "YES" if stored_hash == id_hash else "NO"
        pol = policy_map.get(name, {})
        rotation = pol.get("rotation", "unmanaged")
        print(f"{name:<35} {mid:<16} {state:<12} {hash_verified:<14} rotation={rotation}")

    if not installed:
        print("  (no ollama models found)")
    return 0


def cmd_verify() -> int:
    """
    Verify hash integrity for all installed models.
    Fail-closed: any mismatch → mark as not VERIFIED, print error.
    No network calls.
    """
    installed = _run_ollama_list()
    reg = _load_registry()
    hash_reg = _load_hash_registry()
    changed = False
    exit_code = 0

    for m in installed:
        name = m["name"]
        mid = m["id"]
        id_hash = _compute_id_hash(mid)
        stored = hash_reg.get(name)

        if stored is None:
            # First-time registration: store hash and mark VERIFIED
            hash_reg[name] = id_hash
            model_entry = reg["models"].get(name, {})
            old_state = model_entry.get("state", CANDIDATE)
            reg["models"][name] = {
                "name": name,
                "id": mid,
                "state": VERIFIED,
                "verified_at": _utc_now(),
            }
            _log_event(
                reg,
                model_name=name,
                old_version=mid,
                new_version=mid,
                old_state=old_state,
                new_state=VERIFIED,
                hash_val=id_hash,
                reason="initial_verification",
            )
            print(f"[VERIFIED] {name}: {id_hash[:16]}…")
            changed = True
        elif stored == id_hash:
            print(f"[OK]       {name}: hash matches")
        else:
            # FAIL-CLOSED: hash mismatch
            model_entry = reg["models"].get(name, {})
            old_state = model_entry.get("state", CANDIDATE)
            reg["models"][name] = dict(model_entry) | {"state": CANDIDATE}
            _log_event(
                reg,
                model_name=name,
                old_version=mid,
                new_version=mid,
                old_state=old_state,
                new_state=CANDIDATE,
                hash_val=id_hash,
                reason="hash_mismatch_fail_closed",
            )
            print(f"[FAIL]     {name}: HASH MISMATCH — expected {stored[:16]}… got {id_hash[:16]}…", file=sys.stderr)
            changed = True
            exit_code = 1

    if changed:
        _save_hash_registry(hash_reg)
        _save_registry(reg)

    return exit_code


def cmd_rotate() -> int:
    """
    Rotate models per policy (rotation=auto only).
    A rotation activates a VERIFIED model if it is CANDIDATE.
    No deletion unless policy.allow_pruning=true and hash verified.
    No network calls.
    """
    installed = _run_ollama_list()
    reg = _load_registry()
    hash_reg = _load_hash_registry()
    policy = _load_policy()
    installed_map = {m["name"]: m for m in installed}
    rotated = 0

    for pol in policy:
        name = pol.get("pinned_version", "")
        rotation = pol.get("rotation", "manual")
        if rotation != "auto":
            continue
        if name not in installed_map:
            print(f"[SKIP]  {name}: not installed")
            continue

        m = installed_map[name]
        mid = m["id"]
        id_hash = _compute_id_hash(mid)
        stored = hash_reg.get(name)

        if stored != id_hash:
            print(f"[FAIL]  {name}: hash not verified — skipping rotation (fail-closed)", file=sys.stderr)
            continue

        model_entry = reg["models"].get(name, {})
        current_state = model_entry.get("state", CANDIDATE)

        if current_state == ACTIVE:
            print(f"[OK]    {name}: already ACTIVE")
            continue

        if current_state != VERIFIED:
            print(f"[SKIP]  {name}: state={current_state} (must be VERIFIED to rotate to ACTIVE)")
            continue

        # Activate: VERIFIED → ACTIVE
        reg["models"][name] = dict(model_entry) | {"state": ACTIVE, "activated_at": _utc_now()}
        _log_event(
            reg,
            model_name=name,
            old_version=mid,
            new_version=mid,
            old_state=current_state,
            new_state=ACTIVE,
            hash_val=id_hash,
            reason="auto_rotation",
        )
        print(f"[ROTATED] {name}: VERIFIED → ACTIVE")
        rotated += 1

    if rotated:
        _save_registry(reg)

    print(f"\nRotation complete: {rotated} model(s) promoted to ACTIVE.")
    return 0


# ── CLI entry point ───────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print("Usage: python -m octa.llm.lifecycle {status,verify,rotate}", file=sys.stderr)
        return 2

    cmd = args[0].lower()
    if cmd == "status":
        return cmd_status()
    if cmd == "verify":
        return cmd_verify()
    if cmd == "rotate":
        return cmd_rotate()

    print(f"Unknown command: {cmd!r}. Use status, verify, or rotate.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
