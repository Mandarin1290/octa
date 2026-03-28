from __future__ import annotations

import json
import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from octa.core.governance.governance_audit import (
    EVENT_EXECUTION_PREFLIGHT,
    EVENT_GOVERNANCE_ENFORCED,
    EVENT_INFERENCE_CYCLE,
    EVENT_PORTFOLIO_PREFLIGHT,
    EVENT_RISK_AGGREGATION,
    GovernanceAudit,
)
from .inference_bridge import InferenceResult, run_inference_cycle
from octa.execution.capital_state import CapitalState, NAV_DISCREPANCY_THRESHOLD
from octa.core.portfolio.preflight import run_preflight

from .broker_router import BrokerRouter, BrokerRouterConfig
from .carry import generate_carry_intents, load_json_file, resolve_carry_rates
from .evidence_selection import build_ml_selection
from .notifier import ExecutionNotifier
from .pre_execution import PreExecutionError, load_pre_execution_settings, run_pre_execution_gate
from .risk_fail_closed import incident_to_dict, safe_decide
from .risk_engine import RiskDecision, RiskEngine, RiskEngineConfig
from .tws_probe import tws_probe
from octa_core.risk_institutional.risk_aggregator import RiskSnapshot, aggregate_risk
from octa.core.governance.kill_switch import KillSwitchConfig, KillSwitchState, evaluate_kill_switch
from octa.core.portfolio.engine import PortfolioEngineConfig
from octa.execution.fill_tracker import FillEvent, FillTracker


# PRODUCTION SHADOW = mode "dry-run" or "shadow".
# This is the production shadow: BrokerRouter runs in sandbox/read-only mode (no real orders),
# NAV uses broker read + fallback, and the full governance audit chain fires.
# This is distinct from the research shadow in octa/core/shadow/shadow_engine.py which is
# a pure vector backtest (no broker, no governance, historical signals only).
PUBLIC_FOUNDATION_EXECUTION_MODES = frozenset({"dry-run", "shadow"})
INTERNAL_ONLY_EXECUTION_MODES = frozenset({"paper", "live"})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")


def _persist_nav_snapshot(*, state_dir: Path, evidence_dir: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = dict(payload)
    snapshot["hash"] = hashlib.sha256(_canonical_json_bytes(snapshot)).hexdigest()
    _write_json(state_dir / "nav_snapshot.json", snapshot)
    _write_json(evidence_dir / "nav_snapshot.json", snapshot)
    return snapshot


def _extract_nav(snapshot: Dict[str, Any]) -> tuple[float | None, str]:
    nav_keys = (
        "net_liquidation",
        "netLiquidation",
        "nav",
        "equity",
        "account_equity",
        "total_equity",
        "totalEquity",
    )
    for key in nav_keys:
        if key in snapshot:
            try:
                return float(snapshot.get(key)), key
            except Exception:
                return None, key
    return None, ""


_NAV_HWM_FILENAME = "nav_hwm.json"
_NAV_DAY_OPEN_FILENAME = "nav_day_open.json"
_LOSS_STREAK_FILENAME = "loss_streak.json"


def _load_nav_hwm(state_dir: Path) -> float:
    path = state_dir / _NAV_HWM_FILENAME
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return float(data.get("hwm_nav", 0.0))
        except Exception:
            pass
    return 0.0


def _save_nav_hwm(state_dir: Path, hwm_nav: float) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / _NAV_HWM_FILENAME).write_text(
        json.dumps({"hwm_nav": hwm_nav}, indent=2),
        encoding="utf-8",
    )


def _load_nav_day_open(state_dir: Path) -> tuple[float, str]:
    """Return (nav_open, date_str) from persisted day-open state, or (0.0, '')."""
    path = state_dir / _NAV_DAY_OPEN_FILENAME
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return float(data.get("nav", 0.0)), str(data.get("date", ""))
        except Exception:
            pass
    return 0.0, ""


def _save_nav_day_open(state_dir: Path, nav: float, date_str: str) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / _NAV_DAY_OPEN_FILENAME).write_text(
        json.dumps({"nav": nav, "date": date_str}, indent=2),
        encoding="utf-8",
    )


def _load_loss_streak(state_dir: Path) -> int:
    path = state_dir / _LOSS_STREAK_FILENAME
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return int(data.get("streak", 0))
        except Exception:
            pass
    return 0


def _save_loss_streak(state_dir: Path, streak: int) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / _LOSS_STREAK_FILENAME).write_text(
        json.dumps({"streak": streak, "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}, indent=2),
        encoding="utf-8",
    )


def _detect_drift_breaches(drift_registry_dir: Path) -> List[Dict[str, Any]]:
    if not drift_registry_dir.exists():
        return []
    breaches: List[Dict[str, Any]] = []
    for path in sorted(drift_registry_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("disabled") is True:
            continue  # entry administratively suppressed; disabled=True means exempt
        # disabled=False OR disabled missing → BREACH (fail-closed)
        breaches.append(
            {
                "model_key": path.stem,
                "path": str(path),
                "reason": str(payload.get("reason", "drift_breach")),
                "streak": payload.get("streak"),
                "updated_at": payload.get("updated_at"),
            }
        )
    return breaches


@dataclass(frozen=True)
class ExecutionConfig:
    mode: str = "dry-run"
    asset_class: Optional[str] = None
    max_symbols: int = 0
    evidence_dir: Path = Path("octa") / "var" / "evidence" / "execution_default"
    base_evidence_dir: Path = Path("octa") / "var" / "evidence"
    training_run_id: Optional[str] = None
    loop: bool = False
    cycle_seconds: int = 60
    max_cycles: int = 1
    enable_live: bool = False
    i_understand_live_risk: bool = False
    enable_carry: bool = False
    carry_config_path: Path = Path("octa") / "var" / "config" / "carry_config.json"
    carry_rates_path: Path = Path("octa") / "var" / "config" / "carry_rates.json"
    enable_carry_live: bool = False
    i_understand_carry_risk: bool = False
    state_dir: Path = Path("octa") / "var" / "state"
    drift_registry_dir: Path = Path("octa") / "var" / "registry" / "models" / "drift"
    broker_cfg_path: Optional[Path] = None
    pre_execution_enabled: Optional[bool] = None
    tws_probe_timeout_sec: int = 10
    ml_inference_enabled: bool = False
    artifact_dir: Path = Path("raw") / "PKL"
    raw_data_dir: Path = Path("raw")
    inference_timeframe: str = "1D"
    # P0-3/P0-5: Secondary artifact source for inference (training promotion output).
    # When set, paper_ready/<SYMBOL>/<TF>/<SYMBOL>_<TF>.pkl is checked as fallback.
    # Default: octa/var/models/paper_ready/ (canonical training output dir).
    # Set to None to disable the secondary lookup.
    paper_ready_dir: Optional[Path] = Path("octa") / "var" / "models" / "paper_ready"


_ASSET_CLASS_META: Dict[str, Dict[str, str]] = {
    "equity":   {"exchange": "SMART",    "currency": "USD"},
    "equities": {"exchange": "SMART",    "currency": "USD"},
    "stock":    {"exchange": "SMART",    "currency": "USD"},
    "etf":      {"exchange": "SMART",    "currency": "USD"},
    "forex":    {"exchange": "IDEALPRO", "currency": "FOREIGN"},
    "fx_carry": {"exchange": "IDEALPRO", "currency": "FOREIGN"},
    "futures":  {"exchange": "GLOBEX",   "currency": "USD"},
    "future":   {"exchange": "GLOBEX",   "currency": "USD"},
    "index":    {"exchange": "CBOE",     "currency": "USD"},
    "options":  {"exchange": "CBOE",     "currency": "USD"},
    "option":   {"exchange": "CBOE",     "currency": "USD"},
    "crypto":   {"exchange": "PAXOS",    "currency": "USD"},
    "bond":     {"exchange": "SMART",    "currency": "USD"},
}


def _build_exposure_dict(
    eligible_rows: List[Dict[str, Any]],
    nav: float,
) -> Dict[str, Any]:
    """Build structured exposure dict for risk_aggregator.

    Raises RuntimeError on unknown asset_class or missing currency.
    Aggregates by: symbol, asset_class, exchange, currency.
    """
    by_symbol: Dict[str, float] = {}
    by_asset_class: Dict[str, float] = {}
    by_exchange: Dict[str, float] = {}
    by_currency: Dict[str, float] = {}
    unit_notional = float(nav) * 0.01  # 1% NAV per unit position

    for row in eligible_rows:
        symbol = str(row.get("symbol", "")).upper()
        asset_class = str(row.get("asset_class", "")).lower().strip()

        if not asset_class or asset_class == "unknown":
            raise RuntimeError(f"UNKNOWN_ASSET_CLASS:{symbol}:{asset_class!r}")

        meta = _ASSET_CLASS_META.get(asset_class)
        if meta is None:
            raise RuntimeError(f"UNKNOWN_ASSET_CLASS:{symbol}:{asset_class!r}")

        exchange = meta["exchange"]
        currency = meta.get("currency", "")
        if not currency:
            raise RuntimeError(f"CURRENCY_MISSING:{symbol}:{asset_class!r}")

        by_symbol[symbol] = by_symbol.get(symbol, 0.0) + unit_notional
        by_asset_class[asset_class] = by_asset_class.get(asset_class, 0.0) + unit_notional
        by_exchange[exchange] = by_exchange.get(exchange, 0.0) + unit_notional
        by_currency[currency] = by_currency.get(currency, 0.0) + unit_notional

    return {
        "by_symbol": by_symbol,
        "by_asset_class": by_asset_class,
        "by_exchange": by_exchange,
        "by_currency": by_currency,
        "total_notional": sum(by_symbol.values()),
        "symbol_count": len(by_symbol),
    }


def _run_aggregate_risk_fail_closed(
    *,
    eligible_rows: List[Dict[str, Any]],
    nav: float,
    evidence_dir: Path,
    gov_audit: GovernanceAudit,
    cycle_idx: int,
    blocks: List[Dict[str, Any]],
    notifier: "ExecutionNotifier",
) -> Optional[RiskSnapshot]:
    """Call aggregate_risk() with fail-closed semantics.

    Returns RiskSnapshot on success.
    Returns None on any failure — caller MUST skip all orders for this cycle.
    """
    try:
        exposures = _build_exposure_dict(eligible_rows, nav)
        snapshot = aggregate_risk(exposures=exposures)

        _write_json(
            evidence_dir / f"exposure_snapshot_cycle_{cycle_idx:03d}.json",
            {
                "cycle": cycle_idx,
                "timestamp_utc": _utc_now_iso(),
                "nav": nav,
                "exposures": exposures,
                "risk_snapshot": {
                    "source": snapshot.source,
                    "var_es": snapshot.var_es,
                    "stress": snapshot.stress,
                },
            },
        )
        gov_audit.emit(
            EVENT_RISK_AGGREGATION,
            {
                "cycle": cycle_idx,
                "symbol_count": exposures["symbol_count"],
                "total_notional": exposures["total_notional"],
                "by_asset_class": exposures["by_asset_class"],
                "by_currency": exposures["by_currency"],
                "source": snapshot.source,
            },
        )
        return snapshot

    except Exception as exc:
        reason = f"RISK_AGGREGATION_FAIL:{type(exc).__name__}:{exc}"
        blocks.append({"cycle": cycle_idx, "strategy": "all", "reason": reason})
        _write_json(
            evidence_dir / f"risk_aggregation_fail_cycle_{cycle_idx:03d}.json",
            {
                "cycle": cycle_idx,
                "timestamp_utc": _utc_now_iso(),
                "reason": reason,
                "error": str(exc),
                "blocked": True,
            },
        )
        gov_audit.emit(
            EVENT_GOVERNANCE_ENFORCED,
            {
                "reason": "risk_aggregation_fail_closed",
                "cycle": cycle_idx,
                "error": str(exc)[:300],
            },
        )
        notifier.emit("risk_block", {"strategy": "all", "reason": reason})
        return None


def _ml_multiplier(level: int) -> float:
    if level <= 0:
        return 1.0
    if level == 1:
        return 1.25
    if level == 2:
        return 1.5
    return 2.0


def _intent_order_id(prefix: str, symbol: str, cycle: int) -> str:
    return f"{prefix}_{symbol}_{cycle}"


def _enforce_foundation_scope(cfg: ExecutionConfig, mode_norm: str) -> None:
    live_flags = {
        "enable_live": bool(cfg.enable_live),
        "i_understand_live_risk": bool(cfg.i_understand_live_risk),
        "enable_carry_live": bool(cfg.enable_carry_live),
        "i_understand_carry_risk": bool(cfg.i_understand_carry_risk),
    }
    if mode_norm != "live" and not any(live_flags.values()):
        return
    payload = {
        "timestamp_utc": _utc_now_iso(),
        "mode": mode_norm,
        "blocked": True,
        "reason": "foundation_scope_blocks_real_execution",
        "release_scope": "offline_training_and_shadow_smoke_only",
        "live_flags": live_flags,
    }
    _write_json(cfg.evidence_dir / "scope_enforcement.json", payload)
    raise RuntimeError("live_execution_blocked_in_v0_0_0_foundation_scope")


def run_execution(cfg: ExecutionConfig) -> Dict[str, Any]:
    cfg.evidence_dir.mkdir(parents=True, exist_ok=True)
    run_log_dir = cfg.evidence_dir
    notifier = ExecutionNotifier(cfg.evidence_dir)
    risk_engine = RiskEngine(RiskEngineConfig())
    mode_norm = str(cfg.mode).strip().lower()
    _enforce_foundation_scope(cfg, mode_norm)
    if mode_norm in INTERNAL_ONLY_EXECUTION_MODES:
        _write_json(
            cfg.evidence_dir / "internal_only_mode_notice.json",
            {
                "mode": mode_norm,
                "internal_only": True,
                "public_foundation_control_plane_reachable": False,
                "approved_public_modes": sorted(PUBLIC_FOUNDATION_EXECUTION_MODES),
            },
        )
    pre_exec_cfg_path = cfg.broker_cfg_path if cfg.broker_cfg_path is not None else None
    if pre_exec_cfg_path is not None:
        try:
            pre_settings = load_pre_execution_settings(Path(pre_exec_cfg_path), mode=cfg.mode)
            if cfg.pre_execution_enabled is not None:
                pre_settings = type(pre_settings)(
                    enabled=bool(cfg.pre_execution_enabled),
                    tws_e2e_script=pre_settings.tws_e2e_script,
                    tws_e2e_env_passthrough=pre_settings.tws_e2e_env_passthrough,
                    tws_e2e_timeout_sec=pre_settings.tws_e2e_timeout_sec,
                    port_check=pre_settings.port_check,
                    handshake=pre_settings.handshake,
                    telegram=pre_settings.telegram,
                    ibkr_client_id=pre_settings.ibkr_client_id,
                )
            pre_exec_res = run_pre_execution_gate(
                settings=pre_settings,
                evidence_dir=cfg.evidence_dir,
                notifier=notifier,
                mode=cfg.mode,
                run_id=cfg.evidence_dir.name,
            )
            _write_json(cfg.evidence_dir / "pre_execution_status.json", pre_exec_res)
        except PreExecutionError as exc:
            _write_json(
                cfg.evidence_dir / "pre_execution_status.json",
                {
                    "enabled": True,
                    "ready": False,
                    "reason": exc.reason,
                    "detail": exc.detail,
                },
            )
            raise RuntimeError(exc.reason) from exc

    gov_audit = GovernanceAudit(run_id=cfg.evidence_dir.name)
    gov_audit.emit(
        EVENT_EXECUTION_PREFLIGHT,
        {
            "mode": cfg.mode,
            "enable_live": cfg.enable_live,
            "max_symbols": cfg.max_symbols,
            "training_run_id": cfg.training_run_id,
            "evidence_dir": str(cfg.evidence_dir),
        },
    )
    carry_cfg = load_json_file(cfg.carry_config_path) if cfg.enable_carry else {}
    supported_instruments = tuple(
        sorted(
            {
                str(x.get("instrument", "")).strip()
                for x in (carry_cfg.get("instruments") if isinstance(carry_cfg.get("instruments"), list) else [])
                if isinstance(x, dict) and str(x.get("instrument", "")).strip()
            }
        )
    )
    broker = BrokerRouter(
        BrokerRouterConfig(
            mode=cfg.mode,
            enable_live=cfg.enable_live,
            i_understand_live_risk=cfg.i_understand_live_risk,
            enable_carry_live=cfg.enable_carry_live,
            i_understand_carry_risk=cfg.i_understand_carry_risk,
            supported_instruments=supported_instruments,
        )
    )

    mode_label = "shadow" if mode_norm in {"dry-run", "shadow"} else mode_norm
    fallback_nav = 100000.0
    nav = fallback_nav
    nav_source = "fallback"
    nav_currency = "UNKNOWN"
    nav_snapshot_raw: Dict[str, Any] = {}
    nav_key = ""
    nav_error = ""

    try:
        nav_snapshot_raw = dict(broker.account_snapshot() or {})
        nav_currency = str(nav_snapshot_raw.get("currency", nav_snapshot_raw.get("base_currency", "UNKNOWN")))
        nav_candidate, nav_key = _extract_nav(nav_snapshot_raw)
    except Exception as exc:
        nav_candidate = None
        nav_error = f"{type(exc).__name__}: {exc}"

    if mode_norm in {"paper", "live"}:
        if nav_candidate is None or nav_candidate <= 0:
            incident = {
                "timestamp_utc": _utc_now_iso(),
                "mode": mode_label,
                "reason": "NAV_RECONCILE_FAILED",
                "nav_candidate": nav_candidate,
                "nav_key": nav_key,
                "error": nav_error,
                "snapshot_keys": sorted(nav_snapshot_raw.keys()),
            }
            _write_json(cfg.evidence_dir / "nav_reconcile_failed.json", incident)
            _persist_nav_snapshot(
                state_dir=cfg.state_dir,
                evidence_dir=cfg.evidence_dir,
                payload={
                    "as_of": _utc_now_iso(),
                    "mode": mode_label,
                    "nav": float(fallback_nav),
                    "currency": nav_currency,
                    "source": "fallback",
                    "broker_details": {
                        "reason": "nav_reconcile_failed",
                        "snapshot_keys": sorted(nav_snapshot_raw.keys()),
                    },
                },
            )
            raise RuntimeError("NAV_RECONCILE_FAILED")
        nav = float(nav_candidate)
        nav_source = "broker"
    else:
        if nav_candidate is not None and nav_candidate > 0:
            nav = float(nav_candidate)
            nav_source = "broker"
        else:
            notifier.emit(
                "nav_reconcile_warning",
                {
                    "mode": mode_label,
                    "reason": "broker_nav_unavailable_fallback",
                    "nav_fallback": float(fallback_nav),
                    "snapshot_keys": sorted(nav_snapshot_raw.keys()),
                },
            )

    _persist_nav_snapshot(
        state_dir=cfg.state_dir,
        evidence_dir=cfg.evidence_dir,
        payload={
            "as_of": _utc_now_iso(),
            "mode": mode_label,
            "nav": float(nav),
            "currency": nav_currency,
            "source": nav_source,
            "broker_details": {
                "nav_key": nav_key,
                "snapshot_keys": sorted(nav_snapshot_raw.keys()),
            },
        },
    )

    # I5: capital guard — cross-run NAV discrepancy check
    capital_state = CapitalState.load_or_init(cfg.state_dir)
    disc = capital_state.discrepancy(nav)
    if disc > NAV_DISCREPANCY_THRESHOLD:
        gov_audit.emit(
            EVENT_GOVERNANCE_ENFORCED,
            {
                "reason": "nav_discrepancy",
                "persisted_nav": capital_state.nav,
                "broker_nav": nav,
                "discrepancy_pct": round(disc * 100, 4),
                "threshold_pct": round(NAV_DISCREPANCY_THRESHOLD * 100, 4),
                "mode": mode_label,
            },
        )
    # Conservative: use the larger of broker nav and persisted nav
    nav = max(nav, capital_state.nav)

    # Module 3: NAV high-water mark drawdown circuit breaker
    _pe_config = PortfolioEngineConfig()
    _hwm_nav = _load_nav_hwm(cfg.state_dir)
    if nav > _hwm_nav:
        _hwm_nav = nav
        _save_nav_hwm(cfg.state_dir, _hwm_nav)
    _portfolio_drawdown = max(0.0, 1.0 - nav / _hwm_nav) if _hwm_nav > 0 else 0.0
    if _portfolio_drawdown > _pe_config.drawdown_limit:
        _dd_incident = {
            "timestamp_utc": _utc_now_iso(),
            "mode": mode_label,
            "reason": "DRAWDOWN_LIMIT_BREACH",
            "portfolio_drawdown": round(_portfolio_drawdown, 6),
            "drawdown_limit": _pe_config.drawdown_limit,
            "nav": float(nav),
            "hwm_nav": float(_hwm_nav),
        }
        _write_json(cfg.evidence_dir / "drawdown_circuit_breaker.json", _dd_incident)
        gov_audit.emit(
            EVENT_GOVERNANCE_ENFORCED,
            {
                "reason": "drawdown_limit_breach",
                "portfolio_drawdown": round(_portfolio_drawdown, 6),
                "drawdown_limit": _pe_config.drawdown_limit,
                "mode": mode_label,
            },
        )
        if mode_norm in {"paper", "live"}:
            notifier.emit_alert("GOVERNANCE_ENFORCED", {"reason": "drawdown_limit_breach", "mode": mode_label})
            raise RuntimeError("DRAWDOWN_LIMIT_BREACH")
        notifier.emit(
            "drawdown_limit_warning",
            {
                "mode": mode_label,
                "portfolio_drawdown_pct": round(_portfolio_drawdown * 100, 2),
                "drawdown_limit_pct": round(_pe_config.drawdown_limit * 100, 2),
            },
        )

    # Module 3: Daily NAV loss limit — wires the kill-switch daily_loss stub
    _today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _nav_open, _nav_open_date = _load_nav_day_open(cfg.state_dir)
    if nav > 0 and (_nav_open <= 0 or _nav_open_date != _today_str):
        # New day (or first run): set today's open
        _nav_open = nav
        _save_nav_day_open(cfg.state_dir, nav, _today_str)
    _daily_loss_frac = max(0.0, (_nav_open - nav) / _nav_open) if _nav_open > 0 else 0.0

    drift_breaches = _detect_drift_breaches(cfg.drift_registry_dir)
    if drift_breaches:
        incident = {
            "timestamp_utc": _utc_now_iso(),
            "mode": mode_label,
            "reason": "DRIFT_BREACH_BLOCK",
            "breaches": drift_breaches,
        }
        _write_json(cfg.evidence_dir / "drift_breach_block.json", incident)
        if mode_norm in {"paper", "live"}:
            raise RuntimeError("DRIFT_BREACH_BLOCK")
        notifier.emit(
            "drift_breach_warning",
            {
                "mode": mode_label,
                "reason": "drift_breach_shadow_continue",
                "breach_count": len(drift_breaches),
            },
        )

    # I7: TWS readiness probe — paper/live modes require a live broker connection
    if mode_norm in {"paper", "live"}:
        if not tws_probe(broker, timeout_seconds=cfg.tws_probe_timeout_sec):
            incident = {
                "timestamp_utc": _utc_now_iso(),
                "mode": mode_label,
                "reason": "TWS_PROBE_FAILED",
            }
            _write_json(cfg.evidence_dir / "tws_probe_failed.json", incident)
            gov_audit.emit(
                EVENT_GOVERNANCE_ENFORCED,
                {"reason": "tws_not_ready", "mode": mode_label},
            )
            # I8: CRITICAL alert — bypasses dedup window
            notifier.emit_alert("GOVERNANCE_ENFORCED", {"reason": "tws_not_ready", "mode": mode_label})
            raise RuntimeError("TWS_PROBE_FAILED")

    cycle_count = max(1, int(cfg.max_cycles) if cfg.loop else 1)
    notifier.emit(
        "execution_start",
        {
            "mode": cfg.mode,
            "run_id": cfg.evidence_dir.name,
            "loop": bool(cfg.loop),
            "max_cycles": cycle_count,
        },
    )

    ml_orders: List[Dict[str, Any]] = []
    carry_orders: List[Dict[str, Any]] = []
    blocks: List[Dict[str, Any]] = []
    state: Dict[str, Any] = {"last_rebalance_ts": None}
    last_scaling: Dict[str, int] = {}

    ml_current_gross = 0.0
    carry_current_gross = 0.0
    carry_current_net = 0.0
    leverage = 1.0
    preflight_positions: Dict[str, float] = {}
    last_preflight_result = None

    def _enforce_portfolio_preflight(*, cycle: int, strategy: str, symbol: str, qty: float) -> bool:
        nonlocal preflight_positions, last_preflight_result
        projected_positions = dict(preflight_positions)
        projected_positions[symbol] = projected_positions.get(symbol, 0.0) + float(qty)
        preflight_result = run_preflight(
            positions=projected_positions,
            nav=nav,
            returns_by_symbol={},
        )
        last_preflight_result = preflight_result
        gov_audit.emit(
            EVENT_PORTFOLIO_PREFLIGHT,
            {
                "ok": preflight_result.ok,
                "reason": preflight_result.reason,
                "checks": preflight_result.checks,
            },
        )
        _write_json(
            cfg.evidence_dir / "portfolio_preflight.json",
            {
                "ok": preflight_result.ok,
                "reason": preflight_result.reason,
                "blocked_symbols": preflight_result.blocked_symbols,
                "checks": preflight_result.checks,
            },
        )
        if not preflight_result.ok:
            incident = {
                "timestamp_utc": _utc_now_iso(),
                "cycle": cycle,
                "strategy": strategy,
                "symbol": symbol,
                "qty": float(qty),
                "reason": preflight_result.reason,
                "checks": preflight_result.checks,
                "blocked_symbols": preflight_result.blocked_symbols,
            }
            _write_json(cfg.evidence_dir / "preflight_block.json", incident)
            blocks.append(
                {
                    "cycle": cycle,
                    "strategy": strategy,
                    "symbol": symbol,
                    "reason": f"portfolio_preflight={preflight_result.reason}",
                }
            )
            notifier.emit(
                "risk_block",
                {
                    "strategy": strategy,
                    "instrument": symbol,
                    "reason": f"portfolio_preflight={preflight_result.reason}",
                    "incident": incident,
                },
            )
            return False
        preflight_positions = projected_positions
        return True

    _ks_config = KillSwitchConfig()
    fill_tracker = FillTracker(cfg.state_dir)

    for cycle_idx in range(1, cycle_count + 1):
        cycle_dir = run_log_dir / f"cycle_{cycle_idx:03d}"
        cycle_dir.mkdir(parents=True, exist_ok=True)

        # Kill switch: check per-cycle state.
        # daily_loss is now live (from day-open NAV tracking); slippage pending Module 4 fills.
        _ks_state = KillSwitchState(
            execution_failures=len(blocks),
            slippage=0.0,
            daily_loss=_daily_loss_frac,
            system_health=1.0,
        )
        _ks_decision = evaluate_kill_switch(_ks_state, _ks_config)
        if _ks_decision.triggered:
            _write_json(
                cfg.evidence_dir / f"kill_switch_triggered_cycle_{cycle_idx:03d}.json",
                {
                    "cycle": cycle_idx,
                    "timestamp_utc": _utc_now_iso(),
                    "reason": _ks_decision.reason,
                    "execution_failures": len(blocks),
                    "daily_loss_pct": round(_daily_loss_frac * 100, 4),
                    "nav_day_open": _nav_open,
                    "nav_current": float(nav),
                },
            )
            gov_audit.emit(
                EVENT_GOVERNANCE_ENFORCED,
                {"reason": f"kill_switch:{_ks_decision.reason}", "cycle": cycle_idx},
            )
            notifier.emit_alert(
                "kill_switch_triggered",
                {"reason": _ks_decision.reason, "cycle": cycle_idx},
            )
            break

        sel = build_ml_selection(
            evidence_out_dir=cfg.evidence_dir,
            base_evidence_dir=cfg.base_evidence_dir,
            run_id=cfg.training_run_id,
        )
        eligible_rows = list(sel["eligible_rows"])
        if cfg.asset_class:
            eligible_rows = [r for r in eligible_rows if str(r.get("asset_class", "")).lower() == str(cfg.asset_class).lower()]
        eligible_rows = sorted(eligible_rows, key=lambda x: str(x.get("symbol", "")))
        if cfg.max_symbols > 0:
            eligible_rows = eligible_rows[: int(cfg.max_symbols)]

        if not eligible_rows:
            blocks.append({"cycle": cycle_idx, "strategy": "ml", "reason": "no_symbols_entry_eligible"})

        # --- ML Inference Pre-Gate (optional, fail-closed) ---
        # When enabled: approved=True, signal<=0 → block before risk engine.
        # When disabled (default) or on any error: fall through unchanged.
        _inference_map: dict = {}
        if cfg.ml_inference_enabled and eligible_rows:
            try:
                _inference_map = run_inference_cycle(
                    eligible_rows=eligible_rows,
                    artifact_dir=cfg.artifact_dir,
                    raw_data_dir=cfg.raw_data_dir,
                    timeframe=cfg.inference_timeframe,
                    evidence_dir=cfg.evidence_dir,
                    cycle_idx=cycle_idx,
                    paper_ready_dir=cfg.paper_ready_dir,
                )
                _n_approved = sum(1 for ir in _inference_map.values() if ir.approved)
                _n_blocked = sum(1 for ir in _inference_map.values() if ir.approved and ir.signal <= 0)
                gov_audit.emit(
                    EVENT_INFERENCE_CYCLE,
                    {
                        "cycle": cycle_idx,
                        "total_symbols": len(_inference_map),
                        "approved_inference": _n_approved,
                        "blocked_by_signal": _n_blocked,
                        "inference_errors": len(_inference_map) - _n_approved,
                    },
                )
            except Exception as _inf_exc:
                # Evidence-write errors must never block execution
                _inference_map = {}

        # --- Risk Aggregation (fail-closed, pre-trade) ---
        _agg_snapshot = _run_aggregate_risk_fail_closed(
            eligible_rows=eligible_rows,
            nav=nav,
            evidence_dir=cfg.evidence_dir,
            gov_audit=gov_audit,
            cycle_idx=cycle_idx,
            blocks=blocks,
            notifier=notifier,
        )
        _risk_agg_blocked = _agg_snapshot is None

        for row in (eligible_rows if not _risk_agg_blocked else []):
            symbol = str(row.get("symbol", "")).upper()
            scaling_level = int(row.get("scaling_level", 0))
            last_level = last_scaling.get(symbol, 0)
            if scaling_level < last_level:
                notifier.emit(
                    "ml_scaling_reduced",
                    {
                        "symbol": symbol,
                        "from_level": last_level,
                        "to_level": scaling_level,
                        "reason": "latest_evidence_scaling_drop",
                    },
                )
            last_scaling[symbol] = scaling_level

            # --- Inference pre-gate (signal filter before risk engine) ---
            # Only gates when inference is enabled AND inference succeeded.
            # approved=False (no artifact, feature error, etc.) → pass through.
            _infer: InferenceResult = _inference_map.get(symbol)  # type: ignore[assignment]
            if _infer is not None and _infer.approved and _infer.signal <= 0:
                _gate_reason = f"inference_pre_gate:signal={_infer.signal}:tf={_infer.timeframe}"
                blocks.append({"cycle": cycle_idx, "strategy": "ml", "symbol": symbol, "reason": _gate_reason})
                notifier.emit(
                    "inference_pre_gate_block",
                    {
                        "symbol": symbol,
                        "signal": _infer.signal,
                        "confidence": _infer.confidence,
                        "timeframe": _infer.timeframe,
                        "reason": _infer.reason,
                    },
                )
                continue

            blocked, ml_decision_obj, incident = safe_decide(
                decide_fn=risk_engine.decide_ml,
                decide_kwargs={
                    "nav": nav,
                    "scaling_level": scaling_level,
                    "current_gross_exposure_pct": ml_current_gross,
                },
                evidence_dir=cfg.evidence_dir,
                strategy="ml",
                symbol=symbol,
                cycle=cycle_idx,
            )
            if blocked:
                reason = "risk=ERROR => BLOCK"
                blocks.append({"cycle": cycle_idx, "strategy": "ml", "symbol": symbol, "reason": reason})
                notifier.emit(
                    "risk_block",
                    {
                        "strategy": "ml",
                        "instrument": symbol,
                        "reason": reason,
                        "incident": incident_to_dict(incident) if incident is not None else None,
                    },
                )
                continue
            assert ml_decision_obj is not None
            ml_decision = ml_decision_obj
            if not ml_decision.allow:
                blocks.append({"cycle": cycle_idx, "strategy": "ml", "symbol": symbol, "reason": ml_decision.reason})
                notifier.emit(
                    "risk_block",
                    {
                        "strategy": "ml",
                        "instrument": symbol,
                        "reason": ml_decision.reason,
                    },
                )
                continue

            side = "BUY"
            qty = round(ml_decision.final_size / max(1.0, nav / 100.0), 6)
            order = {
                "order_id": _intent_order_id("ml", symbol, cycle_idx),
                "instrument": symbol,
                "qty": qty,
                "side": side,
                "order_type": "MKT",
                "asset_class": str(row.get("asset_class", "equities")).lower(),
            }
            if not _enforce_portfolio_preflight(cycle=cycle_idx, strategy="ml", symbol=symbol, qty=qty):
                continue
            result = broker.place_order(strategy="ml", order=order)
            fill_tracker.record(FillEvent(
                order_id=str(order["order_id"]),
                symbol=symbol,
                strategy="ml",
                side=side,
                qty=float(qty),
                status=str(result.get("status", "UNKNOWN")),
                cycle=cycle_idx,
                timestamp_utc=_utc_now_iso(),
                asset_class=str(row.get("asset_class", "equities")).lower(),
                raw_result=result,
            ))
            ml_current_gross += ml_decision.final_size / max(1.0, float(nav))
            ml_orders.append(
                {
                    "cycle": cycle_idx,
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "scaling_level": scaling_level,
                    "multiplier": _ml_multiplier(scaling_level),
                    "risk_decision": ml_decision.__dict__,
                    "broker_result": result,
                }
            )
            notifier.emit(
                "ml_trade_intent",
                {
                    "symbol": symbol,
                    "side": side,
                    "size": qty,
                    "scaling_level": scaling_level,
                    "reason": ml_decision.reason,
                },
            )

        carry_status = {
            "enabled": False,
            "disabled_reason": "carry_disabled",
            "source": None,
            "instruments_considered": 0,
            "intents": 0,
            "timestamp_utc": _utc_now_iso(),
        }
        if cfg.enable_carry and not _risk_agg_blocked:
            snapshot = broker.account_snapshot()
            rates_info = resolve_carry_rates(
                carry_cfg=carry_cfg,
                rates_file_path=cfg.carry_rates_path,
                broker_snapshot=snapshot,
            )
            if not rates_info.get("enabled", False):
                carry_status = {
                    "enabled": False,
                    "disabled_reason": str(rates_info.get("disabled_reason", "carry_rates_unavailable")),
                    "source": rates_info.get("source"),
                    "instruments_considered": 0,
                    "intents": 0,
                    "timestamp_utc": _utc_now_iso(),
                }
            else:
                intents, signal_status = generate_carry_intents(
                    carry_cfg=carry_cfg,
                    rates=dict(rates_info.get("rates", {})),
                    state=state,
                    now_utc=datetime.now(timezone.utc),
                )
                carry_status = {
                    "enabled": True,
                    "disabled_reason": None,
                    "source": rates_info.get("source"),
                    "instruments_considered": signal_status.get("instruments_considered", 0),
                    "intents": len(intents),
                    "timestamp_utc": _utc_now_iso(),
                }
                for idx, intent in enumerate(intents, start=1):
                    live_mode = str(cfg.mode).lower() == "live"
                    blocked, decision_obj, incident = safe_decide(
                        decide_fn=risk_engine.decide_carry,
                        decide_kwargs={
                            "nav": nav,
                            "carry_confidence": float(intent.confidence),
                            "expected_net_carry_after_costs": float(intent.expected_net_carry_after_costs),
                            "funding_cost": float(intent.funding_cost),
                            "carry_drawdown": float(carry_cfg.get("carry_pnl_drawdown", 0.0)),
                            "current_carry_gross_exposure_pct": carry_current_gross,
                            "current_carry_net_exposure_pct": carry_current_net,
                            "current_pair_exposure_pct": float(carry_cfg.get("pair_exposure_pct", 0.0)),
                            "leverage": leverage,
                            "live_mode": live_mode,
                            "pnl_available": bool(carry_cfg.get("pnl_available", not live_mode)),
                        },
                        evidence_dir=cfg.evidence_dir,
                        strategy="carry",
                        symbol=str(intent.instrument),
                        cycle=cycle_idx,
                    )
                    if blocked:
                        reason = "risk=ERROR => BLOCK"
                        blocks.append(
                            {
                                "cycle": cycle_idx,
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": reason,
                            }
                        )
                        notifier.emit(
                            "risk_block",
                            {
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": reason,
                                "incident": incident_to_dict(incident) if incident is not None else None,
                            },
                        )
                        continue
                    assert decision_obj is not None
                    decision: RiskDecision = decision_obj
                    if not decision.allow:
                        blocks.append(
                            {
                                "cycle": cycle_idx,
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": decision.reason,
                            }
                        )
                        notifier.emit(
                            "risk_block",
                            {
                                "strategy": "carry",
                                "instrument": intent.instrument,
                                "reason": decision.reason,
                            },
                        )
                        continue
                    qty = round(decision.final_size / max(1.0, nav / 100.0), 6)
                    side = "BUY" if intent.direction == "LONG_BASE" else "SELL"
                    order = {
                        "order_id": _intent_order_id("carry", intent.instrument, cycle_idx * 1000 + idx),
                        "instrument": intent.instrument,
                        "qty": qty,
                        "side": side,
                        "order_type": "MKT",
                        "asset_class": str(intent.asset_class or "fx_carry").lower(),
                    }
                    if not _enforce_portfolio_preflight(
                        cycle=cycle_idx,
                        strategy="carry",
                        symbol=str(intent.instrument),
                        qty=qty,
                    ):
                        continue
                    result = broker.place_order(strategy="carry", order=order)
                    fill_tracker.record(FillEvent(
                        order_id=str(order["order_id"]),
                        symbol=str(intent.instrument),
                        strategy="carry",
                        side=side,
                        qty=float(qty),
                        status=str(result.get("status", "UNKNOWN")),
                        cycle=cycle_idx,
                        timestamp_utc=_utc_now_iso(),
                        asset_class=str(intent.asset_class or "fx_carry").lower(),
                        raw_result=result,
                    ))
                    carry_orders.append(
                        {
                            "cycle": cycle_idx,
                            "instrument": intent.instrument,
                            "direction": intent.direction,
                            "asset_class": intent.asset_class,
                            "qty": qty,
                            "expected_net_carry_after_costs": intent.expected_net_carry_after_costs,
                            "funding_cost": intent.funding_cost,
                            "risk_decision": decision.__dict__,
                            "broker_result": result,
                        }
                    )
                    notifier.emit(
                        "carry_trade_intent",
                        {
                            "instrument": intent.instrument,
                            "direction": intent.direction,
                            "size": qty,
                            "expected_net_carry": intent.expected_net_carry_after_costs,
                            "funding_cost": intent.funding_cost,
                            "reason": intent.reason,
                        },
                    )
                state["last_rebalance_ts"] = _utc_now_iso()

        _write_json(cycle_dir / "carry_status.json", carry_status)
        _write_json(
            cycle_dir / "cycle_summary.json",
            {
                "cycle": cycle_idx,
                "timestamp_utc": _utc_now_iso(),
                "ml_orders_count": len([x for x in ml_orders if x["cycle"] == cycle_idx]),
                "carry_orders_count": len([x for x in carry_orders if x["cycle"] == cycle_idx]),
                "blocks_count": len([x for x in blocks if x["cycle"] == cycle_idx]),
                "carry_status": carry_status,
            },
        )

        notifier.emit(
            "cycle_summary",
            {
                "cycle": cycle_idx,
                "ml_orders": len([x for x in ml_orders if x["cycle"] == cycle_idx]),
                "carry_orders": len([x for x in carry_orders if x["cycle"] == cycle_idx]),
                "blocks": len([x for x in blocks if x["cycle"] == cycle_idx]),
                "carry_status": carry_status,
            },
        )

        if cfg.loop and cycle_idx < cycle_count:
            time.sleep(max(0, int(cfg.cycle_seconds)))

    # --- Portfolio preflight overlay ---
    preflight_result = last_preflight_result or run_preflight(
        positions=preflight_positions,
        nav=nav,
        returns_by_symbol={},
    )
    gov_audit.emit(
        EVENT_PORTFOLIO_PREFLIGHT,
        {
            "ok": preflight_result.ok,
            "reason": preflight_result.reason,
            "checks": preflight_result.checks,
        },
    )
    _write_json(cfg.evidence_dir / "portfolio_preflight.json", {
        "ok": preflight_result.ok,
        "reason": preflight_result.reason,
        "blocked_symbols": preflight_result.blocked_symbols,
        "checks": preflight_result.checks,
    })

    summary = {
        "run_id": cfg.evidence_dir.name,
        "timestamp_utc": _utc_now_iso(),
        "mode": cfg.mode,
        "ml_orders": len(ml_orders),
        "carry_orders": len(carry_orders),
        "blocks": len(blocks),
        "carry_enabled": bool(cfg.enable_carry),
        "selection_dir": str(cfg.evidence_dir / "selection"),
        "portfolio_preflight_ok": preflight_result.ok,
    }
    _write_json(cfg.evidence_dir / "execution_summary.json", summary)
    _write_json(cfg.evidence_dir / "ml_orders.json", ml_orders)
    _write_json(cfg.evidence_dir / "carry_orders.json", carry_orders)
    _write_json(cfg.evidence_dir / "risk_blocks.json", blocks)
    _write_json(cfg.evidence_dir / "fill_summary.json", fill_tracker.summary_for_date(_today_str))

    notifier.emit(
        "execution_shutdown",
        {
            "run_id": cfg.evidence_dir.name,
            "mode": cfg.mode,
            "ml_orders": len(ml_orders),
            "carry_orders": len(carry_orders),
            "blocks": len(blocks),
        },
    )

    # I5: persist updated capital state for next run
    CapitalState(
        nav=nav,
        timestamp_utc=_utc_now_iso(),
        source=nav_source,
    ).save(cfg.state_dir)

    # Module 3: update consecutive daily-loss streak (day-over-day decline counter)
    _prev_streak = _load_loss_streak(cfg.state_dir)
    _new_streak = (_prev_streak + 1) if _daily_loss_frac > 0 else 0
    _save_loss_streak(cfg.state_dir, _new_streak)
    if _new_streak > 0:
        _write_json(
            cfg.evidence_dir / "loss_streak.json",
            {
                "streak": _new_streak,
                "daily_loss_pct": round(_daily_loss_frac * 100, 4),
                "nav_day_open": _nav_open,
                "nav_current": float(nav),
                "mode": mode_label,
                "timestamp_utc": _utc_now_iso(),
            },
        )

    return summary
