from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from octa.core.governance.governance_audit import GovernanceAudit
from octa.core.governance.immutability_guard import IMMUTABLE_PROD_BLOCK
from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore
from octa_training.core.artifact_io import load_tradeable_artifact
from octa_training.core.config import load_config
from octa_training.core.features import build_features
from octa_training.core.io_parquet import load_parquet

from .budgets import ResourceBudgetController
from .registry import ArtifactRegistry
from .types import PaperOrderIntent, now_utc_iso, stable_hash
from octa.core.risk.overlay import apply_overlay
from octa.core.governance.drift_monitor import evaluate_drift
from octa_accounting.nav_engine import NAVEngine


_POSITION_STATE_FILE = "position_state.json"


def _position_state_filename(mode: Optional[str] = None) -> str:
    """Return the position-state filename for the given mode.

    Mode-specific files prevent shadow/paper/live positions from
    overwriting each other when multiple modes share a ledger_dir.
    Falls back to the legacy name when mode is None.
    """
    if mode and mode in {"shadow", "paper", "live"}:
        return f"position_state_{mode}.json"
    return _POSITION_STATE_FILE


def _load_position_state(state_dir: Path, mode: Optional[str] = None) -> Dict[str, Any]:
    """Load persistent position state.

    Reads from state_dir/position_state_{mode}.json when mode is provided,
    otherwise falls back to the legacy position_state.json.
    Returns default empty state if file missing or corrupt.
    The returned dict always has keys: positions (dict), exposure_used (float), last_updated.
    """
    p = Path(state_dir) / _position_state_filename(mode)
    if p.exists():
        try:
            s = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(s.get("positions"), dict) and isinstance(s.get("exposure_used"), (int, float)):
                return s
        except Exception:
            pass
    return {"positions": {}, "exposure_used": 0.0, "last_updated": None}


def _save_position_state(state: Dict[str, Any], state_dir: Path, mode: Optional[str] = None) -> None:
    """Atomically write position state via tmp rename.

    Writes to state_dir/position_state_{mode}.json when mode is provided,
    otherwise writes to the legacy position_state.json.
    """
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    p = Path(state_dir) / _position_state_filename(mode)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
    tmp.rename(p)


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

            ib_cfg = IBKRIBInsyncConfig.from_env()
            return IBKRIBInsyncAdapter(ib_cfg)
        except Exception as e:
            raise RuntimeError(f"broker_adapter_init_failed:{e}") from e

    # default sandbox adapter
    from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter

    sandbox_cfg = IBKRConfig(
        rate_limit_per_minute=int(rate_limit_per_minute),
        supported_instruments=list(sorted(set(instruments))),
    )
    return IBKRContractAdapter(sandbox_cfg)


@dataclass
class PaperRiskPolicy:
    max_risk_per_position: float = 0.05
    max_portfolio_exposure: float = 0.35
    daily_loss_limit: float = 0.01
    max_drawdown_stop: float = 0.05
    no_leverage: bool = True


@dataclass
class LiveRiskPolicy:
    """Risk policy for live trading — tighter limits than paper.

    Live requires narrower position sizing, lower portfolio exposure,
    tighter daily loss limit, and a lower slippage-warning threshold.
    """
    max_risk_per_position: float = 0.02   # 2% vs paper 5%
    max_portfolio_exposure: float = 0.20  # 20% vs paper 35%
    daily_loss_limit: float = 0.005       # 0.5% vs paper 1%
    max_drawdown_stop: float = 0.03       # 3% vs paper 5%
    no_leverage: bool = True
    slippage_warn_bps: float = 5.0        # 5bps vs paper 20bps
    min_confidence: float = 0.60
    require_regime_confirm: bool = True


def _load_overlay_config(path: str = "config/risk_overlay.yaml", mode: str = "paper") -> Dict[str, Any]:
    try:
        import yaml

        raw = Path(path).read_text(encoding="utf-8")
        cfg = yaml.safe_load(raw) or {}
        if isinstance(cfg, dict):
            mode_key = str(mode).replace("-", "_")
            if isinstance(cfg.get(mode_key), dict):
                return cfg.get(mode_key) or {}
            return cfg
        return {}
    except Exception:
        return {}


def _regime_state_from_artifact(artifact: Dict[str, Any]) -> Dict[str, Any]:
    gate = artifact.get("gate", {}) if isinstance(artifact, dict) else {}
    label = gate.get("regime_label") or gate.get("label") or os.getenv("OCTA_REGIME_LABEL", "RISK_ON")
    return {"label": str(label)}


def _load_drift_config(path: str = "config/drift.yaml") -> Dict[str, Any]:
    try:
        import yaml

        raw = Path(path).read_text(encoding="utf-8")
        cfg = yaml.safe_load(raw) or {}
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _gate_from_timeframe(tf: str) -> str:
    tf = str(tf).upper()
    return {
        "1D": "global_1d",
        "1H": "signal_1h",
        "30M": "structure_30m",
        "5M": "execution_5m",
        "1M": "micro_1m",
    }.get(tf, "unknown")


def _current_drawdown(ledger: LedgerStore) -> float:
    navs = []
    for e in ledger.by_action("performance.nav"):
        p = e.get("payload", {})
        try:
            navs.append(float(p.get("nav", 0.0)))
        except Exception:
            continue
    if not navs:
        return 0.0
    peak = navs[0]
    max_dd = 0.0
    for nav in navs:
        if nav > peak:
            peak = nav
        dd = (nav / peak) - 1.0
        if dd < max_dd:
            max_dd = dd
    return abs(max_dd)


def _ndjson_append(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def _load_recent_features(cfg: Any, asset_class: str, parquet_path: str, last_n: int = 300, timeframe: Optional[str] = None) -> pd.DataFrame:
    df = load_parquet(Path(parquet_path))
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Parquet loader did not yield DatetimeIndex")
    df = df.sort_index()
    if last_n and len(df) > last_n:
        df = df.iloc[-int(last_n) :]
    # Ensure intraday features (ib_*) are generated for sub-daily timeframes.
    # build_features checks settings.timeframe; wrap cfg to add/override it.
    if timeframe:
        class _TfSettings:
            def __getattr__(self, name: str) -> Any:
                return getattr(cfg, name)
        eff_settings = _TfSettings()
        eff_settings.timeframe = str(timeframe).upper()  # type: ignore[attr-defined]
    else:
        eff_settings = cfg
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


def _run_paper_pre_execution(
    *,
    run_id: str,
    mode: str,
    broker_mode: str,
    broker_cfg_path: Optional[str],
) -> None:
    """Run TWS pre-execution gate before paper/live trading.

    Skipped transparently when:
      - broker_mode is "sandbox" (no real IBKR connection needed), OR
      - no broker config is discoverable, OR
      - pre_execution.enabled is False in the config.

    Fail-closed when broker_mode is "ib_insync" and pre_execution is enabled:
    raises RuntimeError so run_paper() never proceeds with a broken TWS.
    """
    if broker_mode == "sandbox":
        return  # sandbox adapter needs no TWS

    effective_cfg = broker_cfg_path or os.environ.get("OCTA_BROKER_CFG")
    if effective_cfg is None:
        _default = Path("configs/execution_ibkr.yaml")
        if _default.exists():
            effective_cfg = str(_default)
    if effective_cfg is None:
        return  # no config discoverable — skip gracefully

    try:
        from octa.execution.pre_execution import (
            PreExecutionError,
            load_pre_execution_settings,
            run_pre_execution_gate,
        )
        from octa.execution.notifier import ExecutionNotifier
    except ImportError:
        return  # execution module not available in this environment

    try:
        pre_settings = load_pre_execution_settings(Path(effective_cfg), mode=mode)
    except Exception:
        return  # malformed config — do not block

    if not pre_settings.enabled:
        return

    evidence_base = Path("octa") / "var" / "evidence" / f"pre_exec_{run_id}"
    notifier = ExecutionNotifier(evidence_base)
    try:
        run_pre_execution_gate(
            settings=pre_settings,
            evidence_dir=evidence_base,
            notifier=notifier,
            mode=mode,
            run_id=run_id,
        )
    except PreExecutionError as exc:
        raise RuntimeError(
            f"pre_execution_failed:{exc.reason}:{exc.detail}"
        ) from exc


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
    max_disk_mb: int = 10000,
    disk_root: str = "artifacts",
    broker_cfg_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Load promoted PASS artifacts and place PAPER orders.

    Fail-closed: missing artifacts/features/config => no order.
    """
    budget = ResourceBudgetController(max_runtime_s=max_runtime_s, max_ram_mb=max_ram_mb, max_threads=max_threads, max_disk_mb=max_disk_mb, disk_root=disk_root)
    budget.apply_thread_caps()

    # Pre-execution gate — runs before any broker connection is attempted.
    # Broker mode is read from env (set by operator); defaults to sandbox.
    broker_mode = str(os.getenv("OCTA_BROKER_MODE") or "sandbox")
    cfg_tmp_mode = "live" if bool(live_enable) else (str(level).strip().lower() or "paper")
    _run_paper_pre_execution(
        run_id=run_id,
        mode=cfg_tmp_mode,
        broker_mode=broker_mode,
        broker_cfg_path=broker_cfg_path,
    )

    cfg = load_config(config_path)
    mode = "live" if bool(live_enable) else ("shadow" if str(level).strip().lower() == "shadow" else "paper")
    run_ctx: Dict[str, Any] = {
        "mode": mode,
        "service": "autopilot",
        "execution_active": True,
        "run_id": str(run_id),
        "entrypoint": "execution_service",
    }
    gov_audit = GovernanceAudit(run_id=run_id)
    reg = ArtifactRegistry(root=registry_root, ctx=run_ctx)
    ledger = LedgerStore(ledger_dir)

    # Broker selection is configuration-driven. Default is sandbox.
    broker_mode = str(os.getenv("OCTA_BROKER_MODE") or "sandbox")
    broker_rate = int(os.getenv("OCTA_BROKER_RATE_LIMIT_PER_MIN") or "60")

    promoted = reg.get_promoted_artifacts(level=level)
    out_log = Path(paper_log_path)
    overlay_mode = "paper" if not live_enable else "live_shadow"
    overlay_cfg = _load_overlay_config(mode=overlay_mode)
    drift_cfg = _load_drift_config()
    bucket = os.getenv("OCTA_MODEL_BUCKET", "default")
    current_dd = _current_drawdown(ledger)

    placed: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    # Load persistent position state — exposure_used must NOT reset between runs.
    # Stored alongside ledger so ledger_dir parameterisation also isolates state in tests.
    _pstate = _load_position_state(Path(ledger_dir), mode=mode)
    exposure_used = float(_pstate.get("exposure_used", 0.0))
    open_positions: Dict[str, Any] = dict(_pstate.get("positions") or {})

    # NAVEngine tracks per-trade P&L for fills received this run.
    # Fresh instance each run; historical fills live in the ledger (paper.order_filled events).
    nav_engine = NAVEngine()

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
        elif asset_class in {"etf", "etfs"}:
            candidates.append(Path(cfg.paths.raw_dir) / "ETF_Parquet" / f"{symbol}_{bar_size}.parquet")
            candidates.append(Path(cfg.paths.raw_dir) / "ETF_Parquet" / f"{symbol}_1D.parquet")
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
        _last_close: Optional[float] = None
        try:
            df_check = load_parquet(Path(pq))
            if isinstance(df_check.index, pd.DatetimeIndex) and len(df_check.index):
                last_ts = df_check.index.max()
                # Capture last close for slippage tracking (zero overhead — parquet already loaded).
                try:
                    if "close" in df_check.columns and len(df_check):
                        _last_close = float(df_check["close"].iloc[-1])
                except Exception:
                    pass
                # tolerance: 3 bars by default
                tf_s = 3600
                try:
                    from .types import timeframe_seconds

                    tf_s = int(timeframe_seconds(bar_size) or 3600)
                except Exception:
                    tf_s = 3600
                max_stale_s = int(os.getenv("OCTA_MAX_STALE_SECONDS") or str(3 * tf_s))
                _now_utc = pd.Timestamp.utcnow()
                if _now_utc.tzinfo is None:
                    _now_utc = _now_utc.tz_localize("UTC")
                else:
                    _now_utc = _now_utc.tz_convert("UTC")
                age_s = float((_now_utc - pd.Timestamp(last_ts).tz_convert("UTC")).total_seconds())
                if age_s > float(max_stale_s):
                    skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "stale_data", "age_s": age_s, "max_stale_s": max_stale_s})
                    try:
                        reg.set_order_status(order_key=stable_hash({"symbol": symbol, "tf": timeframe, "sha": sha, "ts": now_utc_iso()[:10]}), status="BLOCKED_STALE_DATA")
                    except Exception:
                        pass  # best-effort; stale is already recorded in skipped
                    continue
        except Exception:
            # If we cannot validate staleness, fail-closed
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "stale_check_failed"})
            continue

        # Build features and get signal
        X = None
        try:
            X = _load_recent_features(cfg, asset_class, pq, last_n=last_n_rows, timeframe=bar_size)
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

        # Drift monitor: disable model if KPI deteriorates for N days
        try:
            model_key = f"{symbol}_{timeframe}"
            drift = evaluate_drift(
                ledger_dir=ledger_dir,
                model_key=model_key,
                gate=_gate_from_timeframe(timeframe),
                timeframe=timeframe,
                bucket=bucket,
                cfg=drift_cfg,
                ctx=run_ctx,
                gov_audit=gov_audit,
            )
            if drift.reason == "drift_write_blocked":
                incident_path = Path("octa") / "var" / "evidence" / str(run_id) / "drift_write_block.json"
                incident_path.parent.mkdir(parents=True, exist_ok=True)
                incident_path.write_text(
                    json.dumps(
                        {
                            "ts": now_utc_iso(),
                            "reason": IMMUTABLE_PROD_BLOCK,
                            "mode": mode,
                            "service": "autopilot",
                            "run_id": str(run_id),
                            "operation": "drift_state_write",
                            "target": model_key,
                            "diagnostics": dict(drift.diagnostics or {}),
                        },
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                        default=str,
                    ),
                    encoding="utf-8",
                )
                if mode in {"paper", "live"}:
                    raise RuntimeError(IMMUTABLE_PROD_BLOCK)
                skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "drift_write_blocked"})
                continue
            if drift.disabled:
                skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": drift.reason, "kpi": drift.kpi})
                continue
        except Exception:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "drift_check_failed"})
            continue

        # Risk overlay (fail-closed)
        try:
            regime_state = _regime_state_from_artifact(artifact)
            borrowable = str(os.getenv("OCTA_ASSUME_BORROWABLE", "1")).strip() == "1"
            adjusted = apply_overlay(
                signals=[{"symbol": symbol, "qty": qty, "side": side}],
                portfolio_state={"exposure_used": exposure_used, "gross_short_exposure": 0.0, "drawdown": current_dd},
                market_state={"asset_class": asset_class, "borrowable": borrowable},
                overlay_cfg=overlay_cfg,
                regime_state=regime_state,
            )
            if not adjusted:
                skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "overlay_blocked"})
                continue
            qty = float(adjusted[0].get("qty", qty))
        except Exception:
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "overlay_failed"})
            continue

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
        try:
            _reserved = reg.try_reserve_order(run_id=run_id, order_key=order_key, client_order_id=client_order_id, symbol=symbol, timeframe=timeframe, model_id=sha, side=side, qty=qty)
        except Exception:
            _reserved = True  # immutability guard blocks write in prod context; proceed without registry idempotency
        if not _reserved:
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
        # Slippage forecast: paper orders assume zero slippage (basis for gate comparison).
        ledger.append(AuditEvent.create(
            actor="paper_runner",
            action="slippage.forecast",
            payload={"ts": now_utc_iso(), "symbol": symbol, "slippage": 0.0},
            severity="INFO",
        ))

        # Fail-closed: broker must be available
        if broker is None:
            try:
                reg.set_order_status(order_key, "BLOCKED_NO_BROKER_ADAPTER")
            except Exception:
                pass
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "no_broker_adapter", "order_key": order_key})
            continue

        # Submit order (paper/live selection is handled by broker config + live_enable flag)
        try:
            order = _intent_to_order(intent)
            res = broker.submit_order(order)
            try:
                reg.set_order_status(order_key, str(res.get("status") or "SUBMITTED"))
            except Exception:
                pass  # best-effort; order is already in ledger
            placed.append({"symbol": symbol, "timeframe": timeframe, "order_key": order_key, "order": order, "broker_res": res})
            exposure_used += qty
            ledger.append(AuditEvent.create(actor="paper_runner", action="paper.order_submitted", payload={"ts": now_utc_iso(), "order_key": order_key, "order": order, "broker_res": res}, severity="INFO"))

            # Fill tracking: if broker returned a fill price, record it.
            fill_price_val = res.get("fill_price")
            fill_price_val = float(fill_price_val) if fill_price_val is not None else None
            if fill_price_val:
                ledger.append(AuditEvent.create(
                    actor="paper_runner",
                    action="paper.order_filled",
                    payload={
                        "ts": now_utc_iso(),
                        "run_id": run_id,
                        "symbol": symbol,
                        "side": side,
                        "qty": float(res.get("fill_qty") or qty),
                        "fill_price": fill_price_val,
                        "order_id": res.get("order_id"),
                        "model_id": sha,
                    },
                    severity="INFO",
                ))
                signed_qty = qty if side == "BUY" else -qty
                try:
                    nav_engine.record_trade(symbol, signed_qty, fill_price_val)
                except Exception:
                    pass  # NAVEngine tracking is best-effort; ledger event is the primary record
                # Slippage tracking: compare fill price to last close (signal price).
                # Units: decimal fraction (0.002 = 20bps). Warning threshold: 20bps.
                if _last_close is not None and _last_close > 0.0:
                    try:
                        slippage_frac = abs(fill_price_val - _last_close) / _last_close
                        _slip_sev = "WARNING" if slippage_frac > 0.002 else "INFO"
                        ledger.append(AuditEvent.create(
                            actor="paper_runner",
                            action="slippage.observed",
                            payload={
                                "ts": now_utc_iso(),
                                "symbol": symbol,
                                "fill_price": fill_price_val,
                                "signal_price": _last_close,
                                "slippage": round(slippage_frac, 8),
                                "slippage_bps": round(slippage_frac * 10000, 2),
                            },
                            severity=_slip_sev,
                        ))
                    except Exception:
                        pass  # slippage tracking is best-effort

            # Persist position state after each successful order so exposure_used
            # survives across runs and is never reset to 0.
            open_positions[symbol] = {
                "side": side,
                "exposure": qty,
                "fill_price": fill_price_val,
                "order_key": order_key,
                "ts": now_utc_iso(),
            }
            _save_position_state(
                {"positions": open_positions, "exposure_used": exposure_used, "last_updated": now_utc_iso()},
                Path(ledger_dir),
                mode=mode,
            )
        except Exception as e:
            try:
                reg.set_order_status(order_key, "SUBMIT_FAILED")
            except Exception:
                pass
            skipped.append({"symbol": symbol, "timeframe": timeframe, "reason": "broker_submit_failed", "error": str(e), "order_key": order_key})
            ledger.append(AuditEvent.create(actor="paper_runner", action="paper.order_rejected", payload={"ts": now_utc_iso(), "order_key": order_key, "error": str(e)}, severity="ERROR"))

    # Write NAV snapshot for drift monitor — only when we have fills to report.
    try:
        if nav_engine.positions:
            nav_report = nav_engine.compute_nav()
            ledger.append(AuditEvent.create(
                actor="paper_runner",
                action="performance.nav",
                payload={"ts": now_utc_iso(), "run_id": run_id, "nav": nav_report["nav"]},
                severity="INFO",
            ))
    except Exception:
        pass

    return {"run_id": run_id, "placed": placed, "skipped": skipped, "promoted_count": len(promoted)}
