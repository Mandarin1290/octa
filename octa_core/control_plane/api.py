from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from octa_core.bootstrap.start_guard import can_start_live, can_start_paper
from octa_core.control_plane.safety_stop import safe_stop
from octa_core.control_plane.snapshots import load_positions, parse_json_map
from octa_core.kill_switch import kill_switch
from octa_core.risk_institutional.opengamma_client import (
    OpenGammaAuth,
    OpenGammaClient,
    OpenGammaConfig,
)
from octa_core.risk_institutional.risk_aggregator import aggregate_risk
from octa_core.security.audit import AuditLog


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


@dataclass
class RuntimeState:
    mode: str = "stopped"  # stopped|paper|live
    last_run_id: Optional[str] = None
    autopilot_pid: Optional[int] = None


STATE = RuntimeState()


def _audit_path(features_cfg: dict) -> str:
    sec = (features_cfg.get("features") or {}).get("security") if isinstance((features_cfg.get("features") or {}).get("security"), dict) else {}
    audit = (sec.get("audit_log") or {}) if isinstance(sec.get("audit_log"), dict) else {}
    return str(audit.get("path", "artifacts/security/audit.jsonl"))


def _start_autopilot(config_path: str) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = "."
    # Canonical orchestration path: smoke chain drives autopilot steps.
    p = subprocess.Popen(
        [
            os.environ.get("PYTHON", "python"),
            "scripts/octa_smoke_chain.py",
            "--autopilot-config",
            config_path,
            "--limit",
            "0",
        ],
        env=env,
    )
    return int(p.pid)


def _ticker_banner_payload(cfg: dict) -> Dict[str, Any]:
    dashboard = cfg.get("dashboard") if isinstance(cfg.get("dashboard"), dict) else {}
    ticker_cfg = (
        dashboard.get("ticker_banner") if isinstance((dashboard or {}).get("ticker_banner"), dict) else {}
    )
    enabled = bool((ticker_cfg or {}).get("enabled", False))
    if not enabled:
        return {"enabled": False, "text": "N/A", "source": "disabled", "warning": None}

    candidates = [
        Path("artifacts/last_prices.json"),
        Path("artifacts/prices_snapshot.json"),
        Path("octa/var/cache/last_prices.json"),
    ]
    for path in candidates:
        if not path.exists() or not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            rows = []
            for sym, px in sorted(payload.items(), key=lambda x: str(x[0]))[:12]:
                try:
                    rows.append(f"{str(sym).upper()} {float(px):.4f}")
                except Exception:
                    continue
            if rows:
                return {
                    "enabled": True,
                    "text": " | ".join(rows),
                    "source": str(path),
                    "warning": None,
                }
        except Exception as exc:
            return {
                "enabled": True,
                "text": "N/A",
                "source": "error",
                "warning": {
                    "event_type": "dashboard.ticker_banner.local_read_failed",
                    "path": str(path),
                    "error": f"{type(exc).__name__}:{exc}",
                },
            }

    return {
        "enabled": True,
        "text": "N/A",
        "source": "no_local_data",
        "warning": {
            "event_type": "dashboard.ticker_banner.local_data_missing",
            "paths_checked": [str(p) for p in candidates],
        },
    }


def create_app(*, features_path: str = "octa_core/config/octa_features.yaml"):
    from fastapi import FastAPI, HTTPException

    app = FastAPI(title="OCTA Control Plane", version="0.1")

    @app.get("/status")
    def status() -> Dict[str, Any]:
        cfg = _load_yaml(features_path)
        ticker_banner = _ticker_banner_payload(cfg)
        return {
            "mode": STATE.mode,
            "last_run_id": STATE.last_run_id,
            "autopilot_pid": STATE.autopilot_pid,
            "features_path": features_path,
            "allow_live": bool((cfg.get("modes") or {}).get("allow_live", False)),
            "ticker_banner": ticker_banner,
        }

    @app.post("/start")
    def start(mode: str = "paper", autopilot_config: str = "configs/autonomous_paper.yaml") -> Dict[str, Any]:
        cfg = _load_yaml(features_path)
        features = cfg.get("features") or {}
        thresholds = cfg.get("thresholds") or {}
        audit_path = _audit_path(cfg)

        alog = AuditLog(path=audit_path)

        if mode.lower() == "paper":
            d = can_start_paper(features=features, thresholds=thresholds)
            if not d.ok:
                alog.append(event_type="control_plane.start_denied", payload={"mode": "paper", "errors": d.errors, "warnings": d.warnings})
                raise HTTPException(status_code=400, detail={"errors": d.errors, "warnings": d.warnings})
            pid = _start_autopilot(autopilot_config)
            STATE.mode = "paper"
            STATE.autopilot_pid = pid
            alog.append(event_type="control_plane.start", payload={"mode": "paper", "pid": pid, "config": autopilot_config})
            return {"ok": True, "mode": "paper", "pid": pid, "warnings": d.warnings}

        if mode.lower() == "live":
            # Live requires explicit allow + upstream health checks handled by wrapper (start_guard).
            d = can_start_live(features=features, modes=cfg.get("modes") or {}, thresholds=thresholds, ibkr_health_ok=None, opengamma_health_ok=None)
            if not d.ok:
                alog.append(event_type="control_plane.start_denied", payload={"mode": "live", "errors": d.errors, "warnings": d.warnings})
                raise HTTPException(status_code=400, detail={"errors": d.errors, "warnings": d.warnings})
            # For now, live start is blocked by default; use scripts/run_octa.py with explicit live gating.
            raise HTTPException(status_code=400, detail="live_start_not_supported_via_api_use_run_octa")

        raise HTTPException(status_code=400, detail="unknown_mode")

    @app.post("/stop")
    def stop(mode: str = "SAFE") -> Dict[str, Any]:
        cfg = _load_yaml(features_path)
        audit_path = _audit_path(cfg)

        # Best-effort: do not kill processes here (non-destructive). Engage kill-switch and cancel orders.
        res = safe_stop(mode=mode, exec_api=None, audit_path=audit_path)
        STATE.mode = "stopped"
        return {"ok": True, **res}

    @app.post("/train")
    def train(
        scope: str = "global",
        symbol: Optional[str] = None,
        config_path: str = "configs/dev.yaml",
        package: bool = True,
        safe_mode: bool = False,
    ) -> Dict[str, Any]:
        cfg = _load_yaml(features_path)
        audit_path = _audit_path(cfg)

        env = os.environ.copy()
        env["PYTHONPATH"] = "."

        cmd = [sys.executable, "octa_training/run_train.py", "--config", config_path]
        if bool(safe_mode):
            cmd.append("--safe-mode")
        if bool(package):
            cmd.append("--package")

        if scope == "global":
            cmd.append("--all")
        elif scope == "symbol":
            if not symbol:
                raise HTTPException(status_code=400, detail="symbol required when scope=symbol")
            cmd.extend(["--symbol", symbol])
        elif scope == "timeframe":
            # Timeframe-specific training is implemented by existing multi-tf scripts.
            raise HTTPException(status_code=400, detail="timeframe_scope_not_supported_use_autopilot")
        else:
            raise HTTPException(status_code=400, detail="unknown_scope")

        p = subprocess.Popen(cmd, env=env)
        AuditLog(path=audit_path).append(
            event_type="control_plane.train",
            payload={"scope": scope, "symbol": symbol, "config_path": config_path, "package": package, "safe_mode": safe_mode, "pid": int(p.pid)},
        )
        return {"ok": True, "pid": int(p.pid), "cmd": cmd}

    @app.post("/lockdown")
    def lockdown() -> Dict[str, Any]:
        cfg = _load_yaml(features_path)
        audit_path = _audit_path(cfg)
        AuditLog(path=audit_path).append(event_type="control_plane.lockdown", payload={"action": "engage"})
        kill_switch.engage(actor="control_plane", reason="LOCKDOWN", automated=True)
        # Kill switch is handled by octa_core.kill_switch; secrets are never printed.
        os.environ.pop("TELEGRAM_BOT_TOKEN", None)
        os.environ.pop("OCTA_MASTER_KEY", None)
        os.environ.pop("OPENGAMMA_BEARER_TOKEN", None)
        return {"ok": True, "mode": "locked_down"}

    @app.get("/risk_snapshot")
    def risk_snapshot(
        paper_log_path: str = "artifacts/paper_trade_log.ndjson",
        registry_root: str = "artifacts",
        positions_source: str = "auto",
        opengamma_config_path: str = "octa_core/config/opengamma.yaml",
        required: bool = False,
        confidence: float = 0.975,
        horizon_days: int = 1,
        stress_scenario_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        cfg = _load_yaml(features_path)
        features = cfg.get("features") or {}

        exposures, exposures_source = load_positions(source=positions_source, paper_log_path=paper_log_path, registry_root=registry_root)

        og_feat = features.get("opengamma") if isinstance(features.get("opengamma"), dict) else {}
        og_enabled = bool((og_feat or {}).get("enabled", False))
        og_required_for_live = bool((og_feat or {}).get("required_for_live", False))
        og_required = bool(required or (STATE.mode == "live" and og_required_for_live))

        og_client: Optional[OpenGammaClient] = None
        og_cfg = _load_yaml(opengamma_config_path)
        og = og_cfg.get("opengamma") if isinstance(og_cfg.get("opengamma"), dict) else {}
        if og_enabled and bool(og.get("base_url")):
            auth = og.get("auth") if isinstance(og.get("auth"), dict) else {}
            auth_mode = str(auth.get("mode") or "none")
            bearer_env = str(auth.get("bearer_env") or "OPENGAMMA_BEARER_TOKEN")
            bearer = os.getenv(bearer_env) if auth_mode == "bearer_env" else None
            timeouts = og.get("timeouts") if isinstance(og.get("timeouts"), dict) else {}
            retries = og.get("retries") if isinstance(og.get("retries"), dict) else {}
            og_client = OpenGammaClient(
                OpenGammaConfig(
                    base_url=str(og.get("base_url")),
                    connect_timeout_s=float(timeouts.get("connect_s", 3.0)),
                    read_timeout_s=float(timeouts.get("read_s", 20.0)),
                    retries_attempts=int(retries.get("attempts", 3)),
                    retries_wait_s=float(retries.get("wait_s", 1.0)),
                    auth=OpenGammaAuth(mode=auth_mode, bearer_token=bearer),
                )
            )

        try:
            snap = aggregate_risk(
                exposures=exposures,
                opengamma=og_client,
                opengamma_required=og_required,
                confidence=float(confidence),
                horizon_days=int(horizon_days),
                stress_scenario_id=stress_scenario_id,
            )
            return {
                "ok": True,
                "exposures": snap.exposures,
                "var_es": snap.var_es,
                "stress": snap.stress,
                "source": snap.source,
                "meta": {
                    "paper_log_path": paper_log_path,
                    "registry_root": registry_root,
                    "positions_source": exposures_source,
                    "opengamma_enabled": og_enabled,
                    "opengamma_required": og_required,
                },
            }
        except Exception as e:
            if og_required:
                raise HTTPException(status_code=503, detail={"error": "risk_snapshot_failed", "reason": str(e)}) from e
            return {
                "ok": True,
                "exposures": exposures,
                "var_es": None,
                "stress": None,
                "source": "local",
                "meta": {
                    "paper_log_path": paper_log_path,
                    "registry_root": registry_root,
                    "positions_source": exposures_source,
                    "opengamma_enabled": og_enabled,
                    "opengamma_required": og_required,
                    "warning": "opengamma_failed_falling_back_to_local",
                    "error": str(e),
                },
            }

    @app.get("/accounting_snapshot")
    def accounting_snapshot(
        paper_log_path: str = "artifacts/paper_trade_log.ndjson",
        registry_root: str = "artifacts",
        positions_source: str = "auto",
        accounting_config_path: str = "octa_core/config/accounting.yaml",
        ledger_db_path: Optional[str] = None,
        prices_json: Optional[str] = None,
    ) -> Dict[str, Any]:
        from octa_core.accounting.ledger import DoubleEntryLedger
        from octa_core.accounting.valuations import compute_nav_and_pnl

        features_cfg = _load_yaml(features_path)
        features = features_cfg.get("features") or {}
        acct_feat = features.get("accounting") if isinstance(features.get("accounting"), dict) else {}
        acct_enabled = bool((acct_feat or {}).get("enabled", False))

        acct_cfg = _load_yaml(accounting_config_path)
        acct = acct_cfg.get("accounting") if isinstance(acct_cfg.get("accounting"), dict) else {}

        positions, positions_source_used = load_positions(source=positions_source, paper_log_path=paper_log_path, registry_root=registry_root)
        prices_map_raw = parse_json_map(s=prices_json)

        prices: Dict[str, float] = {}
        for k, v in (prices_map_raw or {}).items():
            try:
                prices[str(k)] = float(v)
            except Exception:
                continue

        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        base_ccy = str(acct.get("base_currency") or "EUR")
        nav = compute_nav_and_pnl(ts=ts, base_currency=base_ccy, positions=positions, prices=prices)

        ledger_entries = []
        if acct_enabled:
            dbp = str(ledger_db_path or acct.get("ledger_db_path") or "artifacts/accounting/ledger.sqlite3")
            try:
                ledger_entries = DoubleEntryLedger(db_path=dbp).list_entries(limit=25)
            except Exception:
                ledger_entries = []

        return {
            "ok": True,
            "positions": positions,
            "prices": prices,
            "nav": {"nav": nav.nav, "pnl_realized": nav.pnl_realized, "pnl_unrealized": nav.pnl_unrealized, "base_currency": nav.base_currency},
            "meta": {
                "paper_log_path": paper_log_path,
                "registry_root": registry_root,
                "positions_source": positions_source_used,
                "accounting_enabled": acct_enabled,
                "ledger_entries_count": len(ledger_entries),
                "ledger_entries": ledger_entries,
            },
        }

    return app


__all__ = ["create_app"]
