from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore
from octa_training.core.artifact_io import load_tradeable_artifact
from octa_training.core.config import load_config
from octa_training.core.features import build_features
from octa_training.core.io_parquet import load_parquet

from .budgets import ResourceBudgetController
from .registry import ArtifactRegistry
from .types import PaperOrderIntent, now_utc_iso, stable_hash


def _load_broker_adapter(*, mode: str, instruments: List[str], rate_limit_per_minute: int = 60):
    """Return a broker adapter instance.

    Modes:
      - sandbox: local IBKRContractAdapter (no external connectivity)
      - ib_insync: real IBKR TWS/Gateway via ib_insync (paper/live depends on port + live_enable)
    """
    m = str(mode or "sandbox").strip().lower()
    if m == "ib_insync":
        try:
            from octa_vertex.broker.ibkr_ib_insync import (  # type: ignore
                IBKRIBInsyncAdapter,
                IBKRIBInsyncConfig,
            )

            cfg = IBKRIBInsyncConfig.from_env()
            return IBKRIBInsyncAdapter(cfg)
        except Exception as e:
            raise RuntimeError(f"broker_adapter_init_failed:{e}") from e

    # default sandbox adapter
    from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter

    cfg = IBKRConfig(rate_limit_per_minute=int(rate_limit_per_minute), supported_instruments=list(sorted(set(instruments))))
    return IBKRContractAdapter(cfg)


@dataclass
class PaperRiskPolicy:
    max_risk_per_position: float = 0.005
    max_portfolio_exposure: float = 0.35
    daily_loss_limit: float = 0.01
    max_drawdown_stop: float = 0.05
    no_leverage: bool = True


def _ndjson_append(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def _load_recent_features(cfg: Any, asset_class: str, parquet_path: str, last_n: int = 300) -> pd.DataFrame:
    df = load_parquet(Path(parquet_path))
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Parquet loader did not yield DatetimeIndex")
    df = df.sort_index()
    if last_n and len(df) > last_n:
        df = df.iloc[-int(last_n) :]
    eff_settings = cfg  # build_features expects a settings-like object; TrainingConfig is accepted in pipeline usage
    features_res = build_features(df, eff_settings, asset_class)
    return features_res.X


def _intent_to_order(intent: PaperOrderIntent) -> Dict[str, Any]:
    order = {
        "order_id": intent.client_order_id,
        "instrument": intent.symbol,
        "qty": float(intent.qty),
        "side": "BUY" if intent.side.upper() == "BUY" else "SELL",
        "order_type": intent.order_type,
    }
    if intent.limit_price is not None:
        order["limit_price"] = float(intent.limit_price)
    return order


def run_paper(
    *,
    run_id: str,
    config_path: str,
    registry_root: str = "artifacts",
    ledger_dir: str = "artifacts/ledger_paper",
    level: str = "paper",
    live_enable: bool = False,
    last_n_rows: int = 300,
    paper_log_path: str = "artifacts/paper_trade_log.ndjson",
    max_runtime_s: int = 3600,
    max_ram_mb: int = 12000,
    max_threads: int = 4,
) -> Dict[str, Any]:
    """Load promoted PASS artifacts and place PAPER orders.

    Fail-closed: missing artifacts/features/config => no order.
    """

    budget = ResourceBudgetController(max_runtime_s=max_runtime_s, max_ram_mb=max_ram_mb, max_threads=max_threads)
    budget.apply_thread_caps()

    cfg = load_config(config_path)
    reg = ArtifactRegistry(root=registry_root)
    ledger = LedgerStore(ledger_dir)

    # Broker selection is configuration-driven. Default is sandbox.
    broker_mode = str(os.getenv("OCTA_BROKER_MODE") or "sandbox")
    broker_rate = int(os.getenv("OCTA_BROKER_RATE_LIMIT_PER_MIN") or "60")

    promoted = reg.get_promoted_artifacts(level=level)
    out_log = Path(paper_log_path)

    placed: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    # Basic portfolio exposure tracking (positions are fractional sizing from SafeInference).
    exposure_used = 0.0

    # Build broker adapter once (fail-closed if cannot initialize).
    symbols = [str(r.get("symbol")) for r in promoted if r.get("symbol")]
    try:
        broker = _load_broker_adapter(mode=broker_mode, instruments=symbols, rate_limit_per_minute=broker_rate)
    except Exception as e:
        broker = None
        # record once; per-order we will fail-closed
        ledger.append(AuditEvent.create(actor="paper_runner", action="paper.broker_unavailable", payload={"ts": now_utc_iso(), "error": str(e), "mode": broker_mode}, severity="ERROR"))

    for rec in promoted:
        budget.checkpoint("paper:loop")
        symbol = str(rec.get("symbol"))
        timeframe = str(rec.get("timeframe"))
        pkl_path = str(rec.get("path"))
        sha = str(rec.get("sha256"))

        # Fail-closed: required files must exist
        if not (pkl_path and os.path.exists(pkl_path) and os.path.exists(pkl_path.replace(".pkl", ".sha256"))):
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "missing_artifact_files"})
            continue

        try:
            artifact = load_tradeable_artifact(pkl_path, pkl_path.replace(".pkl", ".sha256"))
        except Exception as e:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "artifact_load_failed", "error": str(e)})
            continue

        asset = artifact.get("asset", {}) if isinstance(artifact, dict) else {}
        asset_class = str(asset.get("asset_class") or "unknown")
        bar_size = str(asset.get("bar_size") or timeframe)

        # Resolve parquet path (strict: must exist)
        pq = None
        # common local conventions
        candidates = []
        if asset_class in {"fx"}:
            candidates.append(Path(cfg.paths.fx_parquet_dir) / f"{symbol}_{bar_size}.parquet")
            candidates.append(Path(cfg.paths.raw_dir) / "FX_parquet" / f"{symbol}_{bar_size}.parquet")
        elif asset_class in {"crypto"}:
            candidates.append(Path(cfg.paths.raw_dir) / "Crypto_parquet" / f"{symbol}_{bar_size}.parquet")
        elif asset_class in {"future"}:
            candidates.append(Path(cfg.paths.raw_dir) / "Future_parquet" / f"{symbol}_{bar_size}.parquet")
        else:
            candidates.append(Path(cfg.paths.raw_dir) / "Stock_parquet" / f"{symbol}_{bar_size}.parquet")
            candidates.append(Path(cfg.paths.raw_dir) / "Stock_parquet" / f"{symbol}_1D.parquet")

        for c in candidates:
            if c.exists():
                pq = str(c)
                break
        if not pq:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "missing_parquet"})
            continue

        # Circuit breaker: stale data
        try:
            df_check = load_parquet(Path(pq))
            if isinstance(df_check.index, pd.DatetimeIndex) and len(df_check.index):
                last_ts = df_check.index.max()
                # tolerance: 3 bars by default
                tf_s = 3600
                try:
                    from .types import timeframe_seconds

                    tf_s = int(timeframe_seconds(bar_size) or 3600)
                except Exception:
                    tf_s = 3600
                max_stale_s = int(os.getenv("OCTA_MAX_STALE_SECONDS") or str(3 * tf_s))
                age_s = float((pd.Timestamp.utcnow().tz_localize("UTC") - pd.Timestamp(last_ts).tz_convert("UTC")).total_seconds())
                if age_s > float(max_stale_s):
                    skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "stale_data", "age_s": age_s, "max_stale_s": max_stale_s})
                    reg.set_order_status(order_key=stable_hash({"symbol": symbol, "tf": timeframe, "sha": sha, "ts": now_utc_iso()[:10]}), status="BLOCKED_STALE_DATA")
                    continue
        except Exception:
            # If we cannot validate staleness, fail-closed
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "stale_check_failed"})
            continue

        # Build features and get signal
        X = None
        try:
            X = _load_recent_features(cfg, asset_class, pq, last_n=last_n_rows)
        except Exception as e:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "feature_build_failed", "error": str(e)})
            continue

        infer = artifact.get("safe_inference")
        if infer is None or not hasattr(infer, "predict"):
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "missing_safe_inference"})
            continue

        pred = infer.predict(X)
        sig = int(pred.get("signal", 0) or 0)
        pos = float(pred.get("position", 0.0) or 0.0)

        if sig == 0 or pos == 0.0:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "no_signal", "pred": pred})
            continue

        side = "BUY" if sig > 0 else "SELL"
        qty = abs(pos)

        # Risk gate (fail-closed): per-position and portfolio exposure
        risk = PaperRiskPolicy()
        try:
            max_pos = float(getattr(risk, "max_risk_per_position", 0.005))
            max_port = float(getattr(risk, "max_portfolio_exposure", 0.35))
            if qty > max_pos:
                skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "risk_max_position", "qty": qty, "max": max_pos})
                continue
            if exposure_used + qty > max_port:
                skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "risk_max_portfolio", "exposure_used": exposure_used, "qty": qty, "max": max_port})
                continue
        except Exception:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "risk_check_failed"})
            continue

        # idempotency key per day+symbol+timeframe+artifact
        order_key = stable_hash({"date": now_utc_iso()[:10], "symbol": symbol, "tf": timeframe, "sha": sha, "side": side, "qty": qty})
        client_order_id = f"paper-{order_key[:18]}"
        if not reg.try_reserve_order(run_id=run_id, order_key=order_key, client_order_id=client_order_id, symbol=symbol, timeframe=timeframe, model_id=sha, side=side, qty=qty):
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "idempotent_duplicate"})
            continue

        intent = PaperOrderIntent(symbol=symbol, timeframe=timeframe, model_id=sha, side=side, qty=qty, client_order_id=client_order_id)
        evt = {
            "ts": now_utc_iso(),
            "run_id": run_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "model_id": sha,
            "decision": "PLACE_PAPER_ORDER",
            "intent": intent.__dict__,
            "pred": pred,
        }
        _ndjson_append(out_log, evt)
        ledger.append(AuditEvent.create(actor="paper_runner", action="paper.order_intent", payload=evt, severity="INFO"))

        # Fail-closed: broker must be available
        if broker is None:
            reg.set_order_status(order_key, "BLOCKED_NO_BROKER_ADAPTER")
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "no_broker_adapter", "order_key": order_key})
            continue

        # Submit order (paper/live selection is handled by broker config + live_enable flag)
        try:
            order = _intent_to_order(intent)
            res = broker.submit_order(order)
            reg.set_order_status(order_key, str(res.get("status") or "SUBMITTED"))
            placed.append({"symbol": symbol, "timeframe": timeframe, "order_key": order_key, "order": order, "broker_res": res})
            exposure_used += qty
            ledger.append(AuditEvent.create(actor="paper_runner", action="paper.order_submitted", payload={"ts": now_utc_iso(), "order_key": order_key, "order": order, "broker_res": res}, severity="INFO"))
        except Exception as e:
            reg.set_order_status(order_key, "SUBMIT_FAILED")
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "broker_submit_failed", "error": str(e), "order_key": order_key})
            ledger.append(AuditEvent.create(actor="paper_runner", action="paper.order_rejected", payload={"ts": now_utc_iso(), "order_key": order_key, "error": str(e)}, severity="ERROR"))

    return {"run_id": run_id, "placed": placed, "skipped": skipped, "promoted_count": len(promoted)}
