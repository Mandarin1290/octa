import io
import types
from pathlib import Path

import pandas as pd


def _parse_ndjson(buf: str):
    lines = [ln for ln in (buf or '').splitlines() if ln.strip()]
    return [__import__('json').loads(ln) for ln in lines]


class DummyRes:
    def __init__(self, gate_result=None, error=None):
        self.gate_result = gate_result
        self.error = error


def _gate_pass(mod):
    # GateResult lives in octa_training.core.gates
    from octa_training.core.gates import GateResult

    return GateResult(passed=True, status="PASS_FULL", reasons=[])


def test_fx_two_stage_missing_1h_skips_g1(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "FX"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # Only 1D exists.
    p1d = fx_root / "AUDBRL_1D.parquet"
    p1d.write_bytes(b"")

    g1_called = {"n": 0}

    def fake_g0(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    def fake_g1(*args, **kwargs):
        g1_called["n"] += 1
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    monkeypatch.setattr(mod, "evaluate_fx_g0_risk_overlay_1d", fake_g0)
    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)

    cfg = types.SimpleNamespace(paths=types.SimpleNamespace(fx_parquet_dir=str(fx_parquet_dir), reports_dir=str(tmp_path / 'reports')))

    out = io.StringIO()
    summary = mod._run_fx_two_stage(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        limit=0,
        quiet_symbols=True,
    )

    assert summary["selected"] == 1
    assert g1_called["n"] == 0

    objs = _parse_ndjson(out.getvalue())
    assert objs
    obj = next(o for o in objs if o.get('type') == 'symbol')

    assert obj["g1_attempted"] is False
    assert obj["g1_status"] == "SKIP"
    assert obj["g1_reason"] == "missing_1h_parquet"
    assert obj.get("g1_path") in (None, "")

    # Transparency schema (mandatory fields)
    assert obj["g0_passed"] is True
    assert isinstance(obj.get("g0_reasons"), list)
    assert isinstance(obj.get("g0_failed_checks"), list)
    assert isinstance(obj.get("g1_failed_checks"), list)
    assert obj.get("g1_failed_checks"), "SKIP must have an explicit blocker"
    assert isinstance(obj.get("cost_guard"), dict)
    assert "require_nonzero_for_fx" in obj["cost_guard"]
    assert "blocked" in obj["cost_guard"]

    reasons = "\n".join((obj.get("gate") or {}).get("reasons") or [])
    assert "invalid_1h_data" not in reasons
    assert "missing_1h_parquet" in reasons


def test_fx_two_stage_non_hourly_1h_skips_g1(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "FX"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # 1D + 1H exist, but 1H is daily-like.
    p1d = fx_root / "EURMXN_1D.parquet"
    p1h = fx_parquet_dir / "EURMXN_1H.parquet"
    p1d.write_bytes(b"")
    p1h.write_bytes(b"")

    g1_called = {"n": 0}

    def fake_g0(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    def fake_g1(*args, **kwargs):
        g1_called["n"] += 1
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    # Return a daily-like index for the 1H parquet spacing check.
    def fake_load_parquet(path: Path, **kwargs):
        idx = pd.date_range("2020-01-01", periods=10, freq="1D", tz="UTC")
        return pd.DataFrame({"close": range(len(idx))}, index=idx)

    monkeypatch.setattr(mod, "evaluate_fx_g0_risk_overlay_1d", fake_g0)
    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)
    monkeypatch.setattr(mod, "load_parquet", fake_load_parquet)

    cfg = types.SimpleNamespace(paths=types.SimpleNamespace(fx_parquet_dir=str(fx_parquet_dir), reports_dir=str(tmp_path / 'reports')))

    out = io.StringIO()
    summary = mod._run_fx_two_stage(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        limit=0,
        quiet_symbols=True,
    )

    assert summary["selected"] == 1
    assert g1_called["n"] == 0

    objs = _parse_ndjson(out.getvalue())
    assert objs
    obj = next(o for o in objs if o.get('type') == 'symbol')

    assert obj["g1_attempted"] is False
    assert obj["g1_status"] == "SKIP"
    assert obj["g1_reason"] == "non_hourly_1h_parquet"
    assert obj["g1_path"] is not None

    # Transparency schema (mandatory fields)
    assert obj["g0_passed"] is True
    assert isinstance(obj.get("g0_reasons"), list)
    assert isinstance(obj.get("g0_failed_checks"), list)
    assert isinstance(obj.get("g1_failed_checks"), list)
    assert obj.get("g1_failed_checks"), "SKIP must have an explicit blocker"
    assert isinstance(obj.get("cost_guard"), dict)
    assert "require_nonzero_for_fx" in obj["cost_guard"]
    assert "blocked" in obj["cost_guard"]

    # median should be ~86400 seconds.
    assert obj["g1_median_spacing"] is not None
    assert obj["g1_median_spacing"] >= 80000

    # Index debug fields should be present for non-hourly case (df loaded successfully).
    assert obj.get('g1_index_n') == 10
    assert obj.get('g1_index_unique_n') == 10
    assert obj.get('g1_index_min') is not None
    assert obj.get('g1_index_max') is not None
    assert isinstance(obj.get('g1_head_ts'), list)
    assert isinstance(obj.get('g1_tail_ts'), list)
    # infer_freq may be None depending on pandas version / timezone handling, but key should exist.
    assert 'g1_inferred_freq' in obj

    reasons = "\n".join((obj.get("gate") or {}).get("reasons") or [])
    assert "invalid_1h_data" not in reasons
    assert "non_hourly_1h_parquet" in reasons


def test_fx_two_stage_prefers_cfg_fx_parquet_dir_for_1h_and_runs_g1(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "RAW_GENERIC"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # 1D universe lives under root.
    p1d = fx_root / "EURUSD_1D.parquet"
    p1d.write_bytes(b"")

    # A misleading daily-like 1H exists next to 1D (should be ignored when cfg.fx_parquet_dir is set).
    (fx_root / "EURUSD_1H.parquet").write_bytes(b"")

    # The preferred 1H lives under fx_parquet_dir (hourly-like).
    p1h_pref = fx_parquet_dir / "EURUSD_1H.parquet"
    p1h_pref.write_bytes(b"")

    def fake_g0(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    g1_called = {"n": 0, "path": None}

    def fake_g1(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        g1_called["n"] += 1
        g1_called["path"] = str(parquet_path)
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    # Make spacing check hourly-like ONLY for the preferred path.
    def fake_load_parquet(path: Path, **kwargs):
        if str(path) == str(p1h_pref):
            idx = pd.date_range("2020-01-01", periods=10, freq="1h", tz="UTC")
        else:
            idx = pd.date_range("2020-01-01", periods=10, freq="1D", tz="UTC")
        return pd.DataFrame({"close": range(len(idx))}, index=idx)

    monkeypatch.setattr(mod, "evaluate_fx_g0_risk_overlay_1d", fake_g0)
    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)
    monkeypatch.setattr(mod, "load_parquet", fake_load_parquet)

    cfg = types.SimpleNamespace(paths=types.SimpleNamespace(fx_parquet_dir=str(fx_parquet_dir), reports_dir=str(tmp_path / 'reports')))

    out = io.StringIO()
    summary = mod._run_fx_two_stage(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        limit=0,
        quiet_symbols=True,
    )

    assert summary["selected"] == 1
    assert g1_called["n"] == 1
    assert g1_called["path"] == str(p1h_pref)

    objs = _parse_ndjson(out.getvalue())
    obj = next(o for o in objs if o.get('type') == 'symbol')
    assert obj["g1_attempted"] is True
    assert obj["g1_status"] == "RUN"
    assert obj["g1_path"] == str(p1h_pref)


def test_fx_two_stage_cfg_fx_parquet_dir_missing_does_not_fallback(monkeypatch, tmp_path):
    import scripts.global_gate_diagnose as mod

    fx_root = tmp_path / "RAW_GENERIC"
    fx_root.mkdir(parents=True)

    fx_parquet_dir = tmp_path / "FX_parquet"
    fx_parquet_dir.mkdir(parents=True)

    # 1D universe + a local 1H exist, but preferred fx_parquet_dir is missing the 1H.
    (fx_root / "AUDCAD_1D.parquet").write_bytes(b"")
    (fx_root / "AUDCAD_1H.parquet").write_bytes(b"")

    def fake_g0(sym, cfg, state, run_id, parquet_path, safe_mode=True):
        return DummyRes(gate_result=_gate_pass(mod), error=None)

    def fake_g1(*args, **kwargs):
        return (_ for _ in ()).throw(AssertionError("G1 must not run when preferred 1H is missing"))

    monkeypatch.setattr(mod, "evaluate_fx_g0_risk_overlay_1d", fake_g0)
    monkeypatch.setattr(mod, "evaluate_fx_g1_alpha_1h", fake_g1)

    cfg = types.SimpleNamespace(paths=types.SimpleNamespace(fx_parquet_dir=str(fx_parquet_dir), reports_dir=str(tmp_path / 'reports')))

    out = io.StringIO()
    summary = mod._run_fx_two_stage(
        root=fx_root,
        cfg=cfg,
        state=object(),
        run_id="t",
        out_fh=out,
        limit=0,
        quiet_symbols=True,
    )

    assert summary["selected"] == 1
    objs = _parse_ndjson(out.getvalue())
    obj = next(o for o in objs if o.get('type') == 'symbol')
    assert obj["g1_attempted"] is False
    assert obj["g1_status"] == "SKIP"
    assert obj["g1_reason"] == "missing_1h_parquet"
