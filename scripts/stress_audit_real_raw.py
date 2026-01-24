#!/usr/bin/env python3
"""Stress-audit the production training pipeline on real raw data.

Runs a representative set of symbols across asset classes and (optionally)
multi-timeframe orchestration. Writes a compact report into cfg.paths.reports_dir.

Usage:
  python scripts/stress_audit_real_raw.py --config configs/e2e_real_raw.yaml

Notes:
- Uses isolated config paths to avoid contaminating prod artifacts/state.
- Packaging only occurs if gates pass (this script does not relax gates).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow running as a script from the repo root without installing the package
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.training_safety_lock import TrainingSafetyLockError, assert_training_armed
from octa_training.core.asset_class import infer_asset_class
from octa_training.core.config import load_config
from octa_training.core.io_parquet import discover_parquets, inspect_parquet
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry

SUFFIXES = {"1H", "30M", "5M", "1M"}


def _build_universe(cfg, state: StateRegistry):
    discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)

    base_to = defaultdict(lambda: {"daily": None, "variants": {}})
    for d in discovered:
        sym = d.symbol
        p = d.path
        parts = sym.split("_")
        if len(parts) >= 2 and parts[-1] in SUFFIXES:
            base = "_".join(parts[:-1])
            base_to[base]["variants"][parts[-1]] = p
        else:
            base_to[sym]["daily"] = p

    classes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for base, info in base_to.items():
        p = info["daily"]
        if p is None and info["variants"]:
            p = next(iter(info["variants"].values()))
        if p is None:
            continue

        cols = []
        try:
            meta = inspect_parquet(p)
            cols = meta.get("columns") or []
        except Exception:
            cols = []

        try:
            ac = infer_asset_class(base, str(p), cols, cfg)
        except Exception:
            ac = "unknown"

        fullset = info["daily"] is not None and all(s in info["variants"] for s in ["1H", "30M", "5M", "1M"])
        classes[str(ac)].append(
            {
                "base": base,
                "asset_class": str(ac),
                "full_multi_tf": bool(fullset),
                "has_daily": bool(info["daily"] is not None),
                "variants": sorted(info["variants"].keys()),
            }
        )

    return classes


def _pick_symbols(classes: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    """Pick a stress set: up to 3 per asset class where available."""

    picks: dict[str, list[str]] = {}

    def pick_first(ac: str, n: int = 3) -> list[str]:
        items = sorted(classes.get(ac, []), key=lambda x: x["base"])
        return [x["base"] for x in items[:n]]

    for ac in sorted(classes.keys()):
        picks[ac] = pick_first(ac, 3)

    # Force include well-known equity proxies if present.
    for must in ["SPX", "SPY", "QQQ"]:
        if "stock" in picks and must in {x["base"] for x in classes.get("stock", [])} and must not in picks["stock"]:
            picks["stock"].append(must)

    # Keep deterministic ordering
    for ac in list(picks.keys()):
        uniq = []
        seen = set()
        for s in picks[ac]:
            if s not in seen:
                uniq.append(s)
                seen.add(s)
        picks[ac] = uniq

    return picks


def _artifact_exists(cfg, symbol: str) -> dict[str, bool]:
    """Check whether tradeable/debug artifacts exist for a symbol."""

    tradeable = (Path(cfg.paths.pkl_dir) / f"{symbol}.pkl").exists()

    debug = False
    try:
        if bool(getattr(cfg.packaging, "save_debug_on_fail", False)):
            dbg_dir = getattr(cfg.packaging, "debug_dir", None)
            if not dbg_dir:
                dbg_dir = str(Path(cfg.paths.pkl_dir) / "_debug_fail")
            debug = (Path(dbg_dir) / f"{symbol}.pkl").exists()
    except Exception:
        debug = False

    return {"tradeable": bool(tradeable), "debug": bool(debug)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/e2e_real_raw.yaml")
    ap.add_argument(
        "--debug-on-fail",
        action="store_true",
        help="Enable debug packaging on gate FAIL (writes debug .pkl into the configured debug_dir)",
    )
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--multi-tf", action="store_true", help="Also run multi-timeframe orchestration for multi-tf candidates")
    args = ap.parse_args()

    cfg_path = args.config
    if args.debug_on_fail and cfg_path == "configs/e2e_real_raw.yaml":
        cfg_path = "configs/e2e_real_raw_debug.yaml"
    cfg = load_config(cfg_path)
    state = StateRegistry(cfg.paths.state_dir)

    run_id = args.run_id or f"stress_audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    classes = _build_universe(cfg, state)
    picks = _pick_symbols(classes)

    base_index: dict[str, dict[str, Any]] = {}
    for ac_items in classes.values():
        for item in ac_items:
            base_index[item["base"]] = item

    # Build execution list (single-symbol runs)
    exec_syms: list[str] = []
    for ac in sorted(picks.keys()):
        exec_syms.extend(picks[ac])

    results: list[dict[str, Any]] = []
    for sym in exec_syms:
        rid = f"{run_id}:{sym}"
        try:
            assert_training_armed(cfg, sym, "1D")
        except TrainingSafetyLockError as e:
            results.append(
                {
                    "context": "single",
                    "symbol": sym,
                    "passed": False,
                    "error": f"LOCK_BLOCKED:{e}",
                    "asset_class": None,
                    "overlay": None,
                    "gate": "LOCK_BLOCKED",
                    "fail_count": None,
                    "pkl_tradeable": False,
                    "pkl_debug": False,
                }
            )
            continue
        res = train_evaluate_package(sym, cfg, state, run_id=rid, safe_mode=False, smoke_test=False)
        st = state.get_symbol_state(sym) or {}
        art = _artifact_exists(cfg, sym)
        results.append(
            {
                "context": "single",
                "symbol": sym,
                "passed": bool(getattr(res, "passed", False)),
                "error": getattr(res, "error", None),
                "asset_class": st.get("asset_class"),
                "overlay": st.get("asset_config_overlay_path"),
                "gate": st.get("last_gate_result"),
                "fail_count": st.get("fail_count"),
                "pkl_tradeable": art["tradeable"],
                "pkl_debug": art["debug"],
            }
        )

        # Optional multi-timeframe orchestration for candidates that have full variants.
        if args.multi_tf and bool(base_index.get(sym, {}).get("full_multi_tf")):
            try:
                from scripts.train_multiframe_symbol import run_sequence

                out = run_sequence(sym, cfg, state, run_id=f"{run_id}:multi_tf:{sym}", force=False)
                for layer_name, layer_res in out.items():
                    # daily layer returns list[TrainResult]
                    if isinstance(layer_res, list):
                        results.append(
                            {
                                "context": f"multi_tf:{layer_name}",
                                "symbol": sym,
                                "passed": None,
                                "error": None,
                                "asset_class": (state.get_symbol_state(sym) or {}).get("asset_class"),
                                "overlay": (state.get_symbol_state(sym) or {}).get("asset_config_overlay_path"),
                                "gate": (state.get_symbol_state(sym) or {}).get("last_gate_result"),
                                "fail_count": (state.get_symbol_state(sym) or {}).get("fail_count"),
                                "pkl_tradeable": False,
                                "pkl_debug": False,
                            }
                        )
                        continue

                    layer_symbol = getattr(layer_res, "symbol", None) or sym
                    lst = state.get_symbol_state(layer_symbol) or {}
                    art2 = _artifact_exists(cfg, layer_symbol)
                    results.append(
                        {
                            "context": f"multi_tf:{layer_name}",
                            "symbol": layer_symbol,
                            "passed": bool(getattr(layer_res, "passed", False)),
                            "error": getattr(layer_res, "error", None),
                            "asset_class": lst.get("asset_class"),
                            "overlay": lst.get("asset_config_overlay_path"),
                            "gate": lst.get("last_gate_result"),
                            "fail_count": lst.get("fail_count"),
                            "pkl_tradeable": art2["tradeable"],
                            "pkl_debug": art2["debug"],
                        }
                    )
            except Exception as e:
                results.append(
                    {
                        "context": "multi_tf",
                        "symbol": sym,
                        "passed": False,
                        "error": f"multi_tf_error:{e}",
                        "asset_class": st.get("asset_class"),
                        "overlay": st.get("asset_config_overlay_path"),
                        "gate": st.get("last_gate_result"),
                        "fail_count": st.get("fail_count"),
                        "pkl_tradeable": False,
                        "pkl_debug": False,
                    }
                )

    # Write report
    out_dir = Path(cfg.paths.reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{run_id}.md"
    json_path = out_dir / f"{run_id}.json"

    lines = []
    lines.append(f"# Stress Audit ({run_id})")
    lines.append("")
    lines.append(f"Config: `{cfg_path}`")
    lines.append(f"State DB: `{Path(cfg.paths.state_dir) / 'state.db'}`")
    lines.append(f"PKL dir: `{cfg.paths.pkl_dir}`")
    lines.append("")

    lines.append("## Universe")
    for ac in sorted(classes.keys()):
        lines.append(f"- {ac}: {len(classes[ac])}")
    lines.append("")

    lines.append("## Picks")
    for ac in sorted(picks.keys()):
        lines.append(f"- {ac}: {', '.join(picks[ac]) if picks[ac] else '(none)'}")
    lines.append("")

    lines.append("## Results")
    lines.append("context | symbol | asset_class | overlay | passed | pkl_tradeable | pkl_debug | gate")
    lines.append("---|---|---|---|---:|---:|---:|---")
    for r in results:
        lines.append(
            f"{r.get('context')} | {r['symbol']} | {r.get('asset_class')} | {r.get('overlay')} | {str(r['passed']).lower()} | {str(r.get('pkl_tradeable')).lower()} | {str(r.get('pkl_debug')).lower()} | {r.get('gate')}"
        )

    md_path.write_text("\n".join(lines), encoding="utf-8")
    json_path.write_text(json.dumps({"run_id": run_id, "picks": picks, "results": results}, indent=2), encoding="utf-8")

    print(f"Wrote report: {md_path}")
    print(f"Wrote json:   {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
