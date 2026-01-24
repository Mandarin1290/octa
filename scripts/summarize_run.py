from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


def _load_survivors(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "timeframe", "decision", "reason_json"])
    return pd.read_parquet(path)


def _load_metrics(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["run_id", "layer", "symbol", "timeframe", "key", "value"])
    return pd.read_parquet(path)


def _safe_json(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _summarize_rejections(df: pd.DataFrame, metrics_df: pd.DataFrame, layer: str) -> List[str]:
    if df.empty:
        return [f"- {layer}: no data"]
    rejects = df[df.get("decision") != "PASS"]
    if rejects.empty:
        return [f"- {layer}: none"]

    reasons = []
    for _, row in rejects.iterrows():
        payload = _safe_json(row.get("reason_json"))
        reason = payload.get("reason") or "unknown"
        details = payload.get("details") or {}
        reasons.append((str(reason), details))

    counter = Counter([r[0] for r in reasons])
    top = counter.most_common(10)
    lines: List[str] = []

    layer_metrics = metrics_df[metrics_df.get("layer") == layer] if not metrics_df.empty else pd.DataFrame()
    top_keys: List[str] = []
    if not layer_metrics.empty and "key" in layer_metrics.columns:
        top_keys = layer_metrics["key"].value_counts().head(3).index.tolist()

    for reason, count in top:
        metrics_text = ""
        if not layer_metrics.empty and top_keys:
            parts = []
            for key in top_keys:
                vals = layer_metrics[layer_metrics["key"] == key]["value"]
                if not vals.empty:
                    try:
                        parts.append(f"{key}={float(vals.mean()):.4f}")
                    except Exception:
                        continue
            if parts:
                metrics_text = " metrics: " + ", ".join(parts)
        lines.append(f"- {layer}: {reason} ({count}){metrics_text} thresholds: n/a")

    return lines


def summarize_run(run_id: str) -> str:
    run_root = Path("octa") / "var" / "artifacts" / "runs" / run_id
    if not run_root.exists():
        raise SystemExit(f"Run not found: {run_root}")

    survivors_dir = run_root / "survivors"
    metrics_path = run_root / "metrics" / "metrics.parquet"
    metrics_df = _load_metrics(metrics_path)

    layers = [
        ("L1_global_1D", "L1_global_1D.parquet"),
        ("L2_signal_1H", "L2_signal_1H.parquet"),
        ("L3_structure_30M", "L3_structure_30M.parquet"),
        ("L4_exec_5M", "L4_exec_5M.parquet"),
        ("L5_micro_1M", "L5_micro_1M.parquet"),
    ]

    lines: List[str] = [f"run_id: {run_id}", "", "survivor_counts:"]

    survivor_counts: Dict[str, int] = {}
    for layer, fname in layers:
        df = _load_survivors(survivors_dir / fname)
        count = int((df.get("decision") == "PASS").sum()) if not df.empty else 0
        survivor_counts[layer] = count
        lines.append(f"- {layer}: {count}")

    lines.append("")
    lines.append("top_rejection_reasons:")
    for layer, fname in layers:
        df = _load_survivors(survivors_dir / fname)
        lines.extend(_summarize_rejections(df, metrics_df, layer))

    return "\n".join(lines) + "\n"


def _latest_from_duckdb() -> str | None:
    db_path = Path("octa") / "var" / "metrics" / "metrics.duckdb"
    if not db_path.exists():
        return None
    try:
        import duckdb  # type: ignore
    except Exception:
        return None
    try:
        with duckdb.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT run_id FROM runs ORDER BY ts_start DESC LIMIT 1"
            ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _latest_from_fs() -> str | None:
    runs_root = Path("octa") / "var" / "artifacts" / "runs"
    if not runs_root.exists():
        return None
    runs = [p for p in runs_root.iterdir() if p.is_dir()]
    if not runs:
        return None
    latest = max(runs, key=lambda p: p.stat().st_mtime)
    return latest.name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="Run id to summarize")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Auto-detect latest run id (DuckDB runs table if available, else filesystem)",
    )
    args = parser.parse_args()

    run_id = args.run_id
    if not run_id and args.latest:
        run_id = _latest_from_duckdb() or _latest_from_fs()
    if not run_id:
        raise SystemExit("Provide --run-id or --latest")

    summary = summarize_run(run_id)
    out_path = Path("artifacts") / "reports" / f"run_{run_id}_diagnostics.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(summary, encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
