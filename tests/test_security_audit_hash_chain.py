from __future__ import annotations

import json

from octa_core.security.audit import AuditLog


def test_audit_hash_chain_detects_tamper(tmp_path):
    p = tmp_path / "audit.jsonl"
    al = AuditLog(path=str(p))
    al.append(event_type="a", payload={"x": 1})
    al.append(event_type="b", payload={"x": 2})

    ok, reason = al.verify()
    assert ok is True

    # Tamper with line 2
    lines = p.read_text(encoding="utf-8").splitlines()
    obj = json.loads(lines[1])
    obj["payload"]["x"] = 999
    lines[1] = json.dumps(obj, sort_keys=True)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")

    ok2, reason2 = al.verify()
    assert ok2 is False
    assert reason2 and "hash_mismatch" in reason2
