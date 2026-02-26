from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from octa import __version__ as OCTA_VERSION
from octa.core.orchestration.resources import get_paths
from octa.core.orchestration.runner import run_cascade
from octa.support.ops.training_state import TrainingStateRecord, TrainingStateStore

REQUIRED_TFS: Tuple[str, ...] = ("1D", "1H", "30M", "5M", "1M")


@dataclass(frozen=True)
class Thresholds:
    age_threshold_s: int
    new_data_threshold_s: int


@dataclass(frozen=True)
class SymbolDecision:
    symbol: str
    action: str
    bucket: str
    needed_tfs: List[str]
    reason: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _config_hash(config_path: Optional[str]) -> str:
    if not config_path:
        return "default"
    data = Path(config_path).read_bytes()
    return _sha256_bytes(data)


def _sha256_bytes(data: bytes) -> str:
    import hashlib

    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _load_trainable_symbols(preflight_outdir: Path) -> List[str]:
    path = preflight_outdir / "trainable_symbols.txt"
    symbols = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return sorted(dict.fromkeys(symbols))


def _load_inventory(preflight_outdir: Path) -> Dict[str, Dict[str, List[str]]]:
    inv_path = preflight_outdir / "inventory.jsonl"
    inventory: Dict[str, Dict[str, List[str]]] = {}
    for line in inv_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        symbol = str(raw.get("symbol", "")).upper()
        tfs = raw.get("tfs") or {}
        if symbol:
            inventory[symbol] = {str(tf).upper(): list(paths) for tf, paths in tfs.items()}
    return inventory


def _pick_rep_path(paths: Sequence[str]) -> Optional[str]:
    if not paths:
        return None
    return sorted(paths, key=lambda p: (len(p), p))[0]


def _latest_mtime_for_symbol(inventory: Dict[str, Dict[str, List[str]]], symbol: str) -> Optional[float]:
    by_tf = inventory.get(symbol, {})
    mtimes: List[float] = []
    for tf in REQUIRED_TFS:
        rep = _pick_rep_path(by_tf.get(tf, []))
        if rep:
            try:
                mtimes.append(Path(rep).stat().st_mtime)
            except FileNotFoundError:
                continue
    return max(mtimes) if mtimes else None


def _latest_mtime_dt_for_symbol(inventory: Dict[str, Dict[str, List[str]]], symbol: str) -> Optional[datetime]:
    latest = _latest_mtime_for_symbol(inventory, symbol)
    if latest is None:
        return None
    try:
        return datetime.fromtimestamp(latest, tz=timezone.utc)
    except Exception:
        return None


def _artifacts_exist(paths: Optional[Sequence[str]]) -> bool:
    if not paths:
        return False
    for p in paths:
        if not Path(p).exists():
            return False
    return True


def _metrics_hash(metrics_df: pd.DataFrame, symbol: str, timeframe: str) -> Optional[str]:
    if metrics_df.empty:
        return None
    subset = metrics_df[(metrics_df["symbol"] == symbol) & (metrics_df["timeframe"] == timeframe)]
    if subset.empty:
        return None
    recs = subset.sort_values(["key", "value"]).to_dict(orient="records")
    blob = json.dumps(recs, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return _sha256_bytes(blob)


def _load_metrics_df(metrics_path: Path) -> pd.DataFrame:
    if not metrics_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(metrics_path)
    except Exception:
        return pd.DataFrame()


def evaluate_symbol(
    symbol: str,
    inventory: Dict[str, Dict[str, List[str]]],
    store: TrainingStateStore,
    config_hash: str,
    pipeline_version: str,
    thresholds: Thresholds,
    now: datetime,
) -> SymbolDecision:
    present = inventory.get(symbol, {})
    missing_tfs = [tf for tf in REQUIRED_TFS if tf not in present]
    if missing_tfs:
        return SymbolDecision(symbol, "skip", "missing", missing_tfs, "missing_timeframes")

    needed: List[str] = []
    reasons: List[str] = []
    bucket = "skip"

    latest_mtime_dt = _latest_mtime_dt_for_symbol(inventory, symbol)

    for tf in REQUIRED_TFS:
        rec = store.get(symbol, tf, config_hash, pipeline_version)
        if rec is None:
            # Check for prior config hash records to flag stale_config
            prior = store.find_by_symbol_tf(symbol, tf)
            if prior:
                bucket = "stale_config"
                reasons.append("config_hash_mismatch")
            else:
                bucket = "partial"
                reasons.append("missing_state")
            needed.append(tf)
            continue

        if rec.status == "PASS_FULL":
            if not rec.metrics_hash or not _artifacts_exist(rec.artifact_paths):
                rec.status = "FAIL"
                rec.reason = "missing_metrics_or_artifacts"
                store.upsert(rec)
                needed.append(tf)
                bucket = "partial"
                reasons.append("missing_metrics_or_artifacts")
                continue

            if rec.drift_flag:
                needed.append(tf)
                bucket = "drift"
                reasons.append("drift_flag")
                continue

            last_end = _parse_iso(rec.last_train_end_utc)
            if last_end is not None:
                if latest_mtime_dt is not None:
                    if latest_mtime_dt > last_end + timedelta(seconds=thresholds.new_data_threshold_s):
                        needed.append(tf)
                        bucket = "stale_data"
                        reasons.append("new_data")
                        continue
                age_s = (now - last_end).total_seconds()
                if age_s >= thresholds.age_threshold_s:
                    needed.append(tf)
                    bucket = "stale_ageing"
                    reasons.append("age_threshold")
                    continue
            continue

        # Any non-pass status requires retrain
        needed.append(tf)
        bucket = "partial"
        reasons.append("non_pass_status")

    if not needed:
        return SymbolDecision(symbol, "skip", "skip", [], "already_passed")

    reason = ",".join(sorted(set(reasons)))
    return SymbolDecision(symbol, "train", bucket, needed, reason)


def build_worklist(
    symbols: Sequence[str],
    inventory: Dict[str, Dict[str, List[str]]],
    store: TrainingStateStore,
    config_hash: str,
    pipeline_version: str,
    thresholds: Thresholds,
    now: datetime,
) -> Dict[str, List[SymbolDecision]]:
    buckets: Dict[str, List[SymbolDecision]] = {
        "missing": [],
        "partial": [],
        "stale_config": [],
        "stale_ageing": [],
        "stale_data": [],
        "drift": [],
        "skip": [],
    }
    for sym in symbols:
        decision = evaluate_symbol(sym, inventory, store, config_hash, pipeline_version, thresholds, now)
        buckets.setdefault(decision.bucket, []).append(decision)
    return buckets


def _run_symbol(
    symbol: str,
    config_path: Optional[str],
    run_id: str,
    var_root: Optional[Path],
) -> None:
    run_cascade(
        config_path=config_path,
        resume=True,
        run_id=run_id,
        universe_limit=0,
        symbols=[symbol],
        var_root=str(var_root) if var_root else None,
    )


def _update_state_from_run(
    store: TrainingStateStore,
    symbol: str,
    run_id: str,
    config_hash: str,
    pipeline_version: str,
    var_root: Optional[Path],
    inventory: Dict[str, Dict[str, List[str]]],
) -> None:
    paths = get_paths(var_root)
    run_root = paths.runs_root / run_id
    metrics_path = run_root / "metrics" / "metrics.parquet"
    metrics_df = _load_metrics_df(metrics_path)

    survivors_map = {
        "1D": run_root / "survivors" / "L1_global_1D.parquet",
        "1H": run_root / "survivors" / "L2_signal_1H.parquet",
        "30M": run_root / "survivors" / "L3_structure_30M.parquet",
        "5M": run_root / "survivors" / "L4_exec_5M.parquet",
        "1M": run_root / "survivors" / "L5_micro_1M.parquet",
    }

    survivors: Dict[str, bool] = {}
    for tf, path in survivors_map.items():
        if not path.exists():
            survivors[tf] = False
            continue
        try:
            df = pd.read_parquet(path)
            survivors[tf] = symbol in set(df["symbol"].astype(str))
        except Exception:
            survivors[tf] = False

    last_train_end_utc = _utc_now_iso()
    latest_mtime = _latest_mtime_for_symbol(inventory, symbol)

    for tf in REQUIRED_TFS:
        status = "PASS_FULL" if survivors.get(tf) else "FAIL"
        if tf != "1D":
            # If prior layer failed, mark as SKIP to indicate not evaluated.
            prior_tf = REQUIRED_TFS[REQUIRED_TFS.index(tf) - 1]
            if not survivors.get(prior_tf):
                status = "SKIP"
        metrics_hash = _metrics_hash(metrics_df, symbol, tf)
        artifact_paths = [
            str(metrics_path),
            str(survivors_map[tf]),
            str(run_root / "config_snapshot.json"),
            str(run_root / "reports" / "summary.md"),
        ]
        reason = None
        if status == "PASS_FULL" and not metrics_hash:
            status = "FAIL"
            reason = "missing_metrics"
        rec = TrainingStateRecord(
            symbol=symbol,
            timeframe=tf,
            config_hash=config_hash,
            pipeline_version=pipeline_version,
            status=status,
            last_train_end_utc=last_train_end_utc,
            metrics_hash=metrics_hash,
            artifact_paths=artifact_paths,
            report_path=str(run_root / "reports" / "summary.md"),
            drift_flag=False,
            last_data_mtime=latest_mtime,
            reason=reason,
        )
        store.upsert(rec)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--preflight_outdir", required=True)
    p.add_argument("--config", default=None)
    p.add_argument("--var-root", default=None)
    p.add_argument("--max-symbols", type=int, default=0)
    p.add_argument("--max-seconds", type=int, default=0)
    p.add_argument("--age-threshold-hours", type=int, default=24 * 7)
    p.add_argument("--new-data-threshold-minutes", type=int, default=60)
    p.add_argument("--state-path", default=None)
    args = p.parse_args()

    preflight_outdir = Path(args.preflight_outdir)
    config_hash = _config_hash(args.config)
    pipeline_version = OCTA_VERSION
    thresholds = Thresholds(
        age_threshold_s=int(args.age_threshold_hours) * 3600,
        new_data_threshold_s=int(args.new_data_threshold_minutes) * 60,
    )
    var_root = Path(args.var_root) if args.var_root else None

    state_path = Path(args.state_path) if args.state_path else Path("octa") / "var" / "state" / "training_state.jsonl"
    store = TrainingStateStore(state_path)

    symbols = _load_trainable_symbols(preflight_outdir)
    inventory = _load_inventory(preflight_outdir)

    now = _utc_now()
    buckets = build_worklist(symbols, inventory, store, config_hash, pipeline_version, thresholds, now)
    store.save()

    def _count(bucket: str) -> int:
        return len(buckets.get(bucket, []))

    print("Training selector summary")
    print(f"- symbols: {len(symbols)}")
    print(f"- missing: {_count('missing')}")
    print(f"- partial: {_count('partial')}")
    print(f"- stale_config: {_count('stale_config')}")
    print(f"- stale_ageing: {_count('stale_ageing')}")
    print(f"- stale_data: {_count('stale_data')}")
    print(f"- drift: {_count('drift')}")
    print(f"- skip: {_count('skip')}")

    worklist: List[SymbolDecision] = []
    for bucket in ("stale_config", "stale_ageing", "stale_data", "drift", "partial"):
        worklist.extend([d for d in buckets.get(bucket, []) if d.action == "train"])

    if args.max_symbols and args.max_symbols > 0:
        worklist = worklist[: int(args.max_symbols)]

    start = time.time()
    for decision in worklist:
        if args.max_seconds and args.max_seconds > 0 and (time.time() - start) >= args.max_seconds:
            print("Max seconds reached; stopping.")
            break
        run_id = f"cascade_{decision.symbol}_{config_hash[:8]}"
        print(f"[train] {decision.symbol} reason={decision.reason} bucket={decision.bucket} run_id={run_id}")
        _run_symbol(decision.symbol, args.config, run_id, var_root)
        _update_state_from_run(store, decision.symbol, run_id, config_hash, pipeline_version, var_root, inventory)
        store.save()


if __name__ == "__main__":
    main()
