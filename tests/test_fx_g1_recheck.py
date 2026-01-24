import io
import types
from pathlib import Path

import pandas as pd


def _parse_ndjson(buf: str):
    lines = [ln for ln in (buf or "").splitlines() if ln.strip()]
    return [__import__("json").loads(ln) for ln in lines]


class DummyRes:
    def __init__(self, gate_result=None, error=None):
        self.gate_result = gate_result
        self.error = error


def _gate_pass():
    from octa_training.core.gates import GateResult

    return GateResult(passed=True, status="PASS_FULL", reasons=[])


def test_fx_g1_recheck_runs_over_full_selected_universe(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "FX"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # Pass-1 selected universe (1D paths)
    p1d_a = fx_root / "EURUSD_1D.parquet"
    p1d_b = fx_root / "AUDCAD_1D.parquet"
    p1d_a.write_bytes(b"")
    p1d_b.write_bytes(b"")

    # Only EURUSD has a strict 1H.
    p1h_a = fx_parquet_dir / "EURUSD_1H.parquet"
    p1h_a.write_bytes(b"")

    g1_called = {"n": 0, "syms": []}

    def fake_g1(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        g1_called["n"] += 1
        g1_called["syms"].append(sym)
        return DummyRes(gate_result=_gate_pass(), error=None)

    def fake_load_parquet(path: Path, **kwargs):
        idx = pd.date_range("2020-01-01", periods=10, freq="1h", tz="UTC")
        return pd.DataFrame({"close": range(len(idx))}, index=idx)

    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)
    monkeypatch.setattr(mod, "load_parquet", fake_load_parquet)

    cfg = types.SimpleNamespace(
        paths=types.SimpleNamespace(
            fx_parquet_dir=str(fx_parquet_dir),
            reports_dir=str(tmp_path / "reports"),
        )
    )

    out = io.StringIO()
    summary = mod._run_fx_g1_recheck(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        selected_1d_paths=[p1d_a, p1d_b],
        quiet_symbols=True,
    )

    assert summary["selected"] == 2
    objs = [o for o in _parse_ndjson(out.getvalue()) if o.get("type") == "symbol"]
    assert len(objs) == 2

    by_sym = {o["symbol"]: o for o in objs}
    assert by_sym["EURUSD"]["pass_id"] == "g1_recheck"
    assert by_sym["EURUSD"]["g1_status"] == "RUN"

    assert by_sym["AUDCAD"]["pass_id"] == "g1_recheck"
    assert by_sym["AUDCAD"]["g1_status"] == "SKIP"
    assert by_sym["AUDCAD"]["g1_reason"] == "missing_1h_parquet"

    assert g1_called["n"] == 1
    assert g1_called["syms"] == ["EURUSD"]


def test_fx_g1_recheck_missing_in_fx_parquet_dir_does_not_fallback(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "RAW_GENERIC"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    p1d = fx_root / "USDJPY_1D.parquet"
    p1d.write_bytes(b"")

    # A misleading local 1H exists next to 1D but MUST NOT be used.
    (fx_root / "USDJPY_1H.parquet").write_bytes(b"")

    def fake_g1(*args, **kwargs):
        raise AssertionError("G1 must not run when preferred 1H is missing")

    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)

    cfg = types.SimpleNamespace(
        paths=types.SimpleNamespace(
            fx_parquet_dir=str(fx_parquet_dir),
            reports_dir=str(tmp_path / "reports"),
        )
    )

    out = io.StringIO()
    mod._run_fx_g1_recheck(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        selected_1d_paths=[p1d],
        quiet_symbols=True,
    )

    objs = [o for o in _parse_ndjson(out.getvalue()) if o.get("type") == "symbol"]
    assert len(objs) == 1
    obj = objs[0]

    assert obj["symbol"] == "USDJPY"
    assert obj["g1_attempted"] is False
    assert obj["g1_status"] == "SKIP"
    assert obj["g1_reason"] == "missing_1h_parquet"
    assert obj.get("g1_path") in (None, "")

    # Transparency schema (mandatory fields)
    assert obj.get("g0_passed") is None
    assert isinstance(obj.get("g0_reasons"), list)
    assert isinstance(obj.get("g0_failed_checks"), list)
    assert isinstance(obj.get("g1_failed_checks"), list)
    assert obj.get("g1_failed_checks"), "SKIP must have an explicit blocker"
    assert isinstance(obj.get("cost_guard"), dict)
    assert "require_nonzero_for_fx" in obj["cost_guard"]
    assert "blocked" in obj["cost_guard"]


def test_fx_g1_recheck_non_hourly_includes_index_debug(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "FX"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    p1d = fx_root / "EURMXN_1D.parquet"
    p1h = fx_parquet_dir / "EURMXN_1H.parquet"
    p1d.write_bytes(b"")
    p1h.write_bytes(b"")

    def fake_load_parquet(path: Path, **kwargs):
        idx = pd.date_range("2020-01-01", periods=10, freq="1D", tz="UTC")
        return pd.DataFrame({"close": range(len(idx))}, index=idx)

    monkeypatch.setattr(mod, "load_parquet", fake_load_parquet)

    cfg = types.SimpleNamespace(
        paths=types.SimpleNamespace(
            fx_parquet_dir=str(fx_parquet_dir),
            reports_dir=str(tmp_path / "reports"),
        )
    )

    out = io.StringIO()
    mod._run_fx_g1_recheck(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        selected_1d_paths=[p1d],
        quiet_symbols=True,
    )

    objs = [o for o in _parse_ndjson(out.getvalue()) if o.get("type") == "symbol"]
    assert len(objs) == 1
    obj = objs[0]
    assert obj["symbol"] == "EURMXN"
    assert obj["g1_status"] == "SKIP"
    assert obj["g1_reason"] == "non_hourly_1h_parquet"
    assert obj.get('g1_index_n') == 10
    assert obj.get('g1_index_unique_n') == 10
    assert obj.get('g1_index_min') is not None
    assert obj.get('g1_index_max') is not None
    assert isinstance(obj.get('g1_head_ts'), list)
    assert isinstance(obj.get('g1_tail_ts'), list)
    assert 'g1_inferred_freq' in obj


def test_fx_two_stage_then_g1_recheck_detects_fix_between_passes(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "FX"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    p1d = fx_root / "EURMXN_1D.parquet"
    p1h = fx_parquet_dir / "EURMXN_1H.parquet"
    p1d.write_bytes(b"")
    p1h.write_bytes(b"")

    def fake_g0(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        return DummyRes(gate_result=_gate_pass(), error=None)

    g1_called = {"n": 0}

    def fake_g1(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        g1_called["n"] += 1
        return DummyRes(gate_result=_gate_pass(), error=None)

    phase = {"fixed": False}

    def fake_load_parquet(path: Path, **kwargs):
        if not phase["fixed"]:
            idx = pd.date_range("2020-01-01", periods=10, freq="1D", tz="UTC")
        else:
            idx = pd.date_range("2020-01-01", periods=10, freq="1h", tz="UTC")
        return pd.DataFrame({"close": range(len(idx))}, index=idx)

    monkeypatch.setattr(mod, "evaluate_fx_g0_risk_overlay_1d", fake_g0)
    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)
    monkeypatch.setattr(mod, "load_parquet", fake_load_parquet)

    cfg = types.SimpleNamespace(
        paths=types.SimpleNamespace(
            fx_parquet_dir=str(fx_parquet_dir),
            reports_dir=str(tmp_path / "reports"),
        )
    )

    out1 = io.StringIO()
    s1 = mod._run_fx_two_stage(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out1,
        limit=0,
        quiet_symbols=True,
    )

    objs1 = [o for o in _parse_ndjson(out1.getvalue()) if o.get("type") == "symbol"]
    assert len(objs1) == 1
    assert objs1[0]["pass_id"] == "two_stage"
    assert objs1[0]["g1_status"] == "SKIP"
    assert objs1[0]["g1_reason"] == "non_hourly_1h_parquet"

    # Simulate a data fix between passes.
    phase["fixed"] = True

    out2 = io.StringIO()
    mod._run_fx_g1_recheck(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out2,
        selected_1d_paths=[Path(x) for x in s1.get("selected_1d_paths")],
        quiet_symbols=True,
    )

    objs2 = [o for o in _parse_ndjson(out2.getvalue()) if o.get("type") == "symbol"]
    assert len(objs2) == 1
    assert objs2[0]["pass_id"] == "g1_recheck"
    assert objs2[0]["g1_status"] == "RUN"

    assert g1_called["n"] == 1
