from __future__ import annotations

import argparse
import gc
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

# Allow running as a standalone script from the scripts/ directory.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _now() -> float:
    return time.time()


def _human_mb(n_bytes: int) -> float:
    return float(n_bytes) / (1024.0 * 1024.0)


def _iter_top_parquets(root: Path, top_n: int) -> list[Path]:
    items: list[tuple[int, Path]] = []
    for p in root.rglob("*.parquet"):
        try:
            items.append((p.stat().st_size, p))
        except OSError:
            continue
    items.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in items[: max(0, int(top_n))]]


def _schema_info(path: Path) -> dict:
    out: dict = {"path": str(path)}
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(str(path))
        out["n_cols"] = len(pf.schema.names)
        out["cols"] = pf.schema.names[:50]
        out["n_row_groups"] = pf.num_row_groups
        out["n_rows"] = pf.metadata.num_rows if pf.metadata is not None else None
    except Exception as e:
        out["schema_error"] = str(e)
    try:
        out["size_mb"] = _human_mb(path.stat().st_size)
    except OSError:
        out["size_mb"] = None
    return out


def _print(obj: dict) -> None:
    import json

    # Write strict JSONL: exactly one JSON object per line.
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _rss_mb(snap: object | None) -> float | None:
    if snap is None:
        return None
    # `MemSnapshot` from octa_training.core.mem_profile has `rss_mb`.
    rss = getattr(snap, "rss_mb", None)
    try:
        return None if rss is None else float(rss)
    except (TypeError, ValueError):
        return None


def _delta_mb(after: float | None, before: float | None) -> float | None:
    if after is None or before is None:
        return None
    return after - before


def _top_allocs(snap: object | None, limit: int = 3) -> list[dict] | None:
    if snap is None:
        return None
    top = getattr(snap, "top", None)
    if not top:
        return None
    out: list[dict] = []
    for row in list(top)[: max(0, int(limit))]:
        if isinstance(row, dict):
            out.append(
                {
                    "file": row.get("file"),
                    "line": row.get("line"),
                    "kb": row.get("kb"),
                    "count": row.get("count"),
                }
            )
        else:
            out.append({"value": str(row)})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="OCTA memory sanity check (parquet load)")
    ap.add_argument("--root", default="raw", help="Root directory to search for parquet")
    ap.add_argument("--top", type=int, default=3, help="How many largest parquet files to test")
    ap.add_argument("--paths", nargs="*", help="Explicit parquet paths (overrides --top)")
    args = ap.parse_args(argv)

    root = Path(args.root)
    if args.paths:
        targets = [Path(p) for p in args.paths]
    else:
        targets = _iter_top_parquets(root, args.top)

    from octa_training.core.io_parquet import load_parquet
    from octa_training.core.mem_profile import maybe_start, snapshot

    os.environ.setdefault("OCTA_MEM_PROFILE", "1")
    maybe_start()

    _print({"event": "mem_sanity:start", "ts": _now(), "root": str(root), "n_targets": len(targets)})

    summaries: list[dict] = []

    for i, p in enumerate(targets, start=1):
        if not p.exists():
            _print({"event": "mem_sanity:missing", "path": str(p)})
            continue

        _print({"event": "mem_sanity:target", "i": i, **_schema_info(p)})

        s0 = snapshot(label=f"mem_sanity:before_load:{p.name}")
        if s0:
            _print({"event": "mem_profile", **asdict(s0)})
        t0 = time.time()
        df = load_parquet(p)
        dt = time.time() - t0
        s1 = snapshot(label=f"mem_sanity:after_load:{p.name}")
        if s1:
            _print({"event": "mem_profile", **asdict(s1)})

        _print(
            {
                "event": "mem_sanity:loaded",
                "i": i,
                "path": str(p),
                "seconds": dt,
                "rows": int(len(df)),
                "cols": list(df.columns)[:50],
                "index_type": type(df.index).__name__,
            }
        )

        # Attempt to drop memory promptly between runs.
        del df
        gc.collect()
        s2 = snapshot(label=f"mem_sanity:after_gc:{p.name}")
        if s2:
            _print({"event": "mem_profile", **asdict(s2)})

        # Emit a per-file summary with RSS deltas and peak observed RSS.
        rss0 = _rss_mb(s0)
        rss1 = _rss_mb(s1)
        rss2 = _rss_mb(s2)
        rss_vals = [v for v in (rss0, rss1, rss2) if v is not None]
        top_after_load = _top_allocs(s1, limit=3)
        _print(
            {
                "event": "mem_sanity:summary",
                "i": i,
                "path": str(p),
                "seconds": dt,
                "rss_before_mb": rss0,
                "rss_after_load_mb": rss1,
                "rss_after_gc_mb": rss2,
                "rss_delta_load_mb": _delta_mb(rss1, rss0),
                "rss_delta_after_gc_mb": _delta_mb(rss2, rss0),
                "rss_peak_mb": max(rss_vals) if rss_vals else None,
                "top_alloc_after_load": top_after_load,
            }
        )

        summaries.append(
            {
                "path": str(p),
                "seconds": dt,
                "rss_delta_load_mb": _delta_mb(rss1, rss0),
                "rss_peak_mb": (max(rss_vals) if rss_vals else None),
                "top_alloc_after_load": top_after_load,
            }
        )

    peak_vals = [s.get("rss_peak_mb") for s in summaries if s.get("rss_peak_mb") is not None]
    delta_vals = [s.get("rss_delta_load_mb") for s in summaries if s.get("rss_delta_load_mb") is not None]
    total_seconds = float(sum(float(s.get("seconds") or 0.0) for s in summaries))
    _print(
        {
            "event": "mem_sanity:overall",
            "ts": _now(),
            "n": len(summaries),
            "total_seconds": total_seconds,
            "rss_peak_max_mb": max(peak_vals) if peak_vals else None,
            "rss_delta_load_max_mb": max(delta_vals) if delta_vals else None,
        }
    )
    _print({"event": "mem_sanity:done", "ts": _now()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
