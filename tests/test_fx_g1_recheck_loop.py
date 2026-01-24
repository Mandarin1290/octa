import json
import types
from pathlib import Path

import pandas as pd


def _read_jsonl(path: Path):
    return [json.loads(ln) for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


class DummyRes:
    def __init__(self, gate_result=None, error=None):
        self.gate_result = gate_result
        self.error = error


def _gate_pass():
    from octa_training.core.gates import GateResult

    return GateResult(passed=True, status="PASS_FULL", reasons=[])


def test_loop_requires_pass1_universe_file(tmp_path):
    import scripts.global_gate_diagnose as mod

    missing = tmp_path / "does_not_exist.jsonl"
    try:
        mod._load_fx_pass1_universe_from_ndjson(missing)
        raise AssertionError("Expected fail-closed on missing pass1 NDJSON")
    except FileNotFoundError:
        pass


def test_loop_writes_timestamped_and_latest(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "FX"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # Create a Pass-1 NDJSON with a single selected FX symbol.
    p1d = fx_root / "EURUSD_1D.parquet"
    p1d.write_bytes(b"")

    pass1_out = tmp_path / "pass1.jsonl"
    pass1_out.write_text(
        json.dumps(
            {
                "type": "symbol",
                "dataset": "fx",
                "pass_id": "two_stage",
                "symbol": "EURUSD",
                "parquet": str(p1d),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    # Strict 1H only in fx_parquet_dir.
    (fx_parquet_dir / "EURUSD_1H.parquet").write_bytes(b"")

    def fake_g1(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        return DummyRes(gate_result=_gate_pass(), error=None)

    def fake_load_parquet(path: Path, **kwargs):
        idx = pd.date_range("2020-01-01", periods=10, freq="h", tz="UTC")
        return pd.DataFrame({"close": range(len(idx))}, index=idx)

    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)
    monkeypatch.setattr(mod, "load_parquet", fake_load_parquet)
    monkeypatch.setattr(mod.time, "sleep", lambda *_args, **_kwargs: None)

    cfg = types.SimpleNamespace(
        paths=types.SimpleNamespace(
            fx_parquet_dir=str(fx_parquet_dir),
            reports_dir=str(tmp_path / "reports"),
        )
    )

    loop_dir = tmp_path / "loop"

    mod._run_fx_g1_recheck_loop(
        fx_root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        pass1_out_path=pass1_out,
        loop_dir=loop_dir,
        interval_secs=0,
        max_iters=2,
        quiet_symbols=True,
        append_quarantine=False,
        refresh_universe=False,
    )

    # Two timestamped outputs + latest copies.
    jsonls = sorted(loop_dir.glob("*.g1_recheck.jsonl"))
    assert len(jsonls) == 2

    latest = loop_dir / "latest.g1_recheck.jsonl"
    latest_summary = loop_dir / "latest.summary.json"
    assert latest.exists()
    assert latest_summary.exists()

    # Validate NDJSON contents: meta iteration + symbol record with pass_id g1_recheck.
    lines = _read_jsonl(latest)
    assert lines[0].get("type") == "iteration"
    sym = next(o for o in lines if o.get("type") == "symbol")
    assert sym["dataset"] == "fx"
    assert sym["pass_id"] == "g1_recheck"


def test_loop_respects_no_fallback(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "RAW_GENERIC"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # Pass-1 NDJSON selects USDJPY_1D.
    p1d = fx_root / "USDJPY_1D.parquet"
    p1d.write_bytes(b"")

    pass1_out = tmp_path / "pass1.jsonl"
    pass1_out.write_text(
        json.dumps(
            {
                "type": "symbol",
                "dataset": "fx",
                "pass_id": "two_stage",
                "symbol": "USDJPY",
                "parquet": str(p1d),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    # A misleading 1H next to 1D, but fx_parquet_dir has no 1H -> must SKIP.
    (fx_root / "USDJPY_1H.parquet").write_bytes(b"")

    def fake_g1(*args, **kwargs):
        raise AssertionError("G1 must not run when fx_parquet_dir 1H is missing")

    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)
    monkeypatch.setattr(mod.time, "sleep", lambda *_args, **_kwargs: None)

    cfg = types.SimpleNamespace(
        paths=types.SimpleNamespace(
            fx_parquet_dir=str(fx_parquet_dir),
            reports_dir=str(tmp_path / "reports"),
        )
    )

    loop_dir = tmp_path / "loop"

    mod._run_fx_g1_recheck_loop(
        fx_root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        pass1_out_path=pass1_out,
        loop_dir=loop_dir,
        interval_secs=0,
        max_iters=1,
        quiet_symbols=True,
        append_quarantine=False,
        refresh_universe=False,
    )

    latest = loop_dir / "latest.g1_recheck.jsonl"
    lines = _read_jsonl(latest)
    sym = next(o for o in lines if o.get("type") == "symbol")
    assert sym["symbol"] == "USDJPY"
    assert sym["g1_status"] == "SKIP"
    assert sym["g1_reason"] == "missing_1h_parquet"
    assert sym.get("g1_path") in (None, "")
