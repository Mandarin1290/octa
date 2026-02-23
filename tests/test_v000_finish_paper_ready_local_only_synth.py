from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from octa.support.ops import v000_finish_paper_ready_local_only as mod


def test_v000_finish_paper_ready_local_only_synth(monkeypatch, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    for sym in ("AAA", "BBB", "CCC", "DDD"):
        (raw / "equities" / sym).mkdir(parents=True, exist_ok=True)
    (raw / "equities" / "AAA" / "AAA_1D.parquet").write_bytes(b"x" * 2048)
    (raw / "equities" / "AAA" / "AAA_1H.parquet").write_bytes(b"x" * 2048)
    (raw / "equities" / "BBB" / "BBB_1D.parquet").write_bytes(b"x" * 2048)
    (raw / "equities" / "BBB" / "BBB_1H.parquet").write_bytes(b"")  # zero-byte skip
    (raw / "equities" / "CCC" / "CCC_1D.parquet").write_bytes(b"x" * 2048)
    (raw / "equities" / "CCC" / "CCC_1H.parquet").write_bytes(b"x" * 2048)
    (raw / "equities" / "DDD" / "DDD_1D.parquet").write_bytes(b"x" * 2048)
    (raw / "equities" / "DDD" / "DDD_1H.parquet").write_bytes(b"x" * 2048)

    def fake_resolve(*, symbol: str, tf: str, raw_root: str = "raw"):
        p = Path(raw_root) / "equities" / symbol.upper() / f"{symbol.upper()}_{tf.upper()}.parquet"
        return (str(p), "found_local_real") if p.exists() else (None, "not_found")

    def fake_subprocess_run(cmd, check=False, stdout=None, stderr=None):
        if stdout is not None:
            stdout.write("")
        if stderr is not None:
            stderr.write("")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(mod, "resolve_parquet_for_symbol_tf", fake_resolve)
    monkeypatch.setattr(mod, "subprocess", SimpleNamespace(run=fake_subprocess_run))
    monkeypatch.setattr(mod, "_run_micro_profiles", lambda **kwargs: {"low": {"rc": 0}, "mid": {"rc": 0}, "target": {"rc": 0}})
    monkeypatch.setattr(mod, "_run_paper_shadow", lambda **kwargs: {"rc": 0, "summary": {}})
    monkeypatch.setattr(mod, "_run_altdata_probe", lambda **kwargs: {"counts": {"available": 0, "missing_ignored": 1, "error_fail_closed": 0}})

    out = tmp_path / "out"
    args = SimpleNamespace(
        requested_symbols="AAA,BBB",
        required_tfs="1D,1H",
        min_nonzero_bytes=1024,
        min_symbols=3,
        max_symbols=4,
        runtime_cap_seconds=120,
        profiles="low,mid,target",
        export_series_out=True,
        export_on_timeout=True,
        decision_trace_out=True,
        out=str(out),
    )
    orig_cwd = Path.cwd()
    try:
        # run() uses raw_root='raw'; run in tmp context to point at synth tree
        monkeypatch.chdir(tmp_path)
        result = mod.run(args)
    finally:
        _ = orig_cwd

    summary = result["summary"]
    assert summary["pass"] is True
    assert summary["requested_ok"] == 1
    assert summary["requested_skipped"] == 1
    assert summary["substituted_count"] >= 2
    assert (out / "requested_symbol_status.json").exists()
    assert (out / "requested_symbol_status.csv").exists()
    status = json.loads((out / "requested_symbol_status.json").read_text(encoding="utf-8"))
    by_sym = {r["symbol"]: r["status"] for r in status["rows"]}
    assert by_sym["AAA"] == "OK"
    assert by_sym["BBB"] == "SKIP_zero_byte"
