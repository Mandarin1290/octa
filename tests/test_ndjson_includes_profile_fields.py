import json
import types

import scripts.global_gate_diagnose as ggd


class _DummyState:
    def record_run_start(self, *args, **kwargs):
        return None

    def record_run_end(self, *args, **kwargs):
        return None

    def update_symbol_state(self, *args, **kwargs):
        return None

    def get_symbol_state(self, *args, **kwargs):
        return {}


def test_global_gate_ndjson_has_profile_fields(tmp_path, monkeypatch):
    # Create a dummy parquet path (content not used; we monkeypatch training).
    (tmp_path / "AAPL_1D.parquet").write_text("", encoding="utf-8")

    def _fake_train_evaluate_package(*args, **kwargs):
        diags = [
            {
                "name": "asset_profile",
                "value": "stock",
                "threshold": None,
                "op": None,
                "passed": True,
                "evaluable": True,
                "confidence": 0.0,
                "reason": None,
            },
            {
                "name": "asset_profile_hash",
                "value": "deadbeef",
                "threshold": None,
                "op": None,
                "passed": True,
                "evaluable": True,
                "confidence": 0.0,
                "reason": None,
            },
            {
                "name": "applied_thresholds",
                "value": {"sharpe_min": 0.7},
                "threshold": None,
                "op": None,
                "passed": True,
                "evaluable": True,
                "confidence": 0.0,
                "reason": None,
            },
        ]
        gate = types.SimpleNamespace(
            passed=True,
            status="PASS_FULL",
            gate_version="v0",
            reasons=[],
            passed_checks=[],
            insufficient_evidence=[],
            robustness=None,
            diagnostics=diags,
        )
        return types.SimpleNamespace(error=None, gate_result=gate)

    monkeypatch.setattr(ggd, "train_evaluate_package", _fake_train_evaluate_package)

    outp = tmp_path / "out.jsonl"
    with outp.open("w", encoding="utf-8") as out_fh:
        ggd._run_dataset(
            dataset="stocks",
            root=tmp_path,
            cfg=types.SimpleNamespace(),
            state=_DummyState(),
            run_id="r1",
            out_fh=out_fh,
            only_1d=False,
            limit=0,
            quiet_symbols=True,
        )

    lines = outp.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])

    assert rec["type"] == "symbol"
    assert rec["asset_profile"] == "stock"
    assert rec["asset_profile_hash"] == "deadbeef"
    assert rec["applied_thresholds"] == {"sharpe_min": 0.7}
