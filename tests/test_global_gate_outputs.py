from __future__ import annotations

import json
from pathlib import Path


def test_validate_gate_outputs_invariants(tmp_path: Path):
    # Minimal synthetic jsonl + summary that must pass invariants.
    jsonl = tmp_path / "run.jsonl"
    summary = tmp_path / "run.summary.json"

    recs = [
        {
            "type": "symbol",
            "dataset": "indices",
            "symbol": "A",
            "passed": True,
            "error": None,
            "gate": {"passed": True, "status": "PASS_FULL", "reasons": []},
        },
        {
            "type": "symbol",
            "dataset": "fx",
            "symbol": "B",
            "passed": False,
            "error": None,
            "gate": {"passed": False, "status": "FAIL_RISK", "reasons": ["tail_kill_switch: ..."]},
        },
        {
            "type": "symbol",
            "dataset": "indices",
            "symbol": "C",
            "passed": False,
            "error": "exception:boom",
            "gate": {"passed": False, "status": "FAIL_DATA", "reasons": ["exception:boom"]},
        },
    ]
    jsonl.write_text("\n".join(json.dumps(r) for r in recs) + "\n", encoding="utf-8")

    summary_obj = {
        "run_id": "x",
        "datasets": [
            {
                "dataset": "indices",
                "selected": 2,
                "status_counts": {"PASS_FULL": 1, "FAIL_DATA": 1},
            },
            {
                "dataset": "fx",
                "selected": 1,
                "status_counts": {"FAIL_RISK": 1},
            },
        ],
    }
    summary.write_text(json.dumps(summary_obj), encoding="utf-8")

    # Use the validator script as a module-less import via path execution.
    # Keep it simple: re-run its logic inline.
    from scripts.validate_global_gate_outputs import validate_jsonl, validate_summary

    _, jsonl_errs = validate_jsonl(jsonl)
    sum_errs = validate_summary(summary)

    assert jsonl_errs == []
    assert sum_errs == []


def test_validator_catches_pass_status_with_passed_false(tmp_path: Path):
    jsonl = tmp_path / "bad.jsonl"
    summary = tmp_path / "bad.summary.json"

    rec = {
        "type": "symbol",
        "dataset": "indices",
        "symbol": "BAD",
        "passed": False,
        "error": None,
        "gate": {"passed": False, "status": "PASS_FULL", "reasons": []},
    }
    jsonl.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    summary.write_text(json.dumps({"run_id": "x", "datasets": [{"dataset": "indices", "selected": 1, "status_counts": {"PASS_FULL": 1}}]}), encoding="utf-8")

    from scripts.validate_global_gate_outputs import validate_jsonl

    _, errs = validate_jsonl(jsonl)
    assert errs, "Expected invariant violation to be detected"
