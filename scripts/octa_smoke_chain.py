from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Sequence

import yaml


SIGNAL_NO_MARKET_DATA_PERMISSIONS = "NO_MARKET_DATA_PERMISSIONS"
SIGNAL_DELAYED_DATA_ONLY = "DELAYED_DATA_ONLY"
SIGNAL_PACING_VIOLATION = "PACING_VIOLATION"
SIGNAL_FARM_DISCONNECTED = "FARM_DISCONNECTED"
SIGNAL_CONNECTION_REFUSED = "CONNECTION_REFUSED"
SIGNAL_TIMEOUT = "TIMEOUT"
SIGNAL_UNKNOWN_ERROR = "UNKNOWN_ERROR"

KNOWN_SIGNALS: tuple[str, ...] = (
    SIGNAL_NO_MARKET_DATA_PERMISSIONS,
    SIGNAL_DELAYED_DATA_ONLY,
    SIGNAL_PACING_VIOLATION,
    SIGNAL_FARM_DISCONNECTED,
    SIGNAL_CONNECTION_REFUSED,
    SIGNAL_TIMEOUT,
    SIGNAL_UNKNOWN_ERROR,
)

FATAL_SIGNALS: frozenset[str] = frozenset(
    {
        SIGNAL_NO_MARKET_DATA_PERMISSIONS,
        SIGNAL_DELAYED_DATA_ONLY,
        SIGNAL_PACING_VIOLATION,
        SIGNAL_FARM_DISCONNECTED,
        SIGNAL_CONNECTION_REFUSED,
        SIGNAL_TIMEOUT,
        SIGNAL_UNKNOWN_ERROR,
    }
)

_SIG_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (SIGNAL_NO_MARKET_DATA_PERMISSIONS, re.compile(r"no market data permissions?", re.IGNORECASE)),
    (SIGNAL_DELAYED_DATA_ONLY, re.compile(r"delayed data", re.IGNORECASE)),
    (SIGNAL_PACING_VIOLATION, re.compile(r"pacing violation", re.IGNORECASE)),
    (SIGNAL_FARM_DISCONNECTED, re.compile(r"market data farm disconnected", re.IGNORECASE)),
    (
        SIGNAL_CONNECTION_REFUSED,
        re.compile(r"(connection refused|failed to connect|could not connect)", re.IGNORECASE),
    ),
    (SIGNAL_TIMEOUT, re.compile(r"(timeout|timed out)", re.IGNORECASE)),
)


@dataclass(frozen=True)
class StepResult:
    name: str
    command: list[str]
    rc: int
    stdout: str
    stderr: str
    signals: list[str]
    ok: bool
    status: str = "PASS"
    reason: str = ""
    rerun_applied: bool = False


@dataclass(frozen=True)
class StepSpec:
    name: str
    argv: list[str]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_text_if_exists(path_raw: str) -> str:
    p = Path(path_raw)
    if not p.exists() or not p.is_file():
        return ""
    return p.read_text(encoding="utf-8")


def _deterministic_run_suffix(*, steps: Sequence[StepSpec], meta_args: dict[str, Any] | None) -> str:
    args = dict(meta_args or {})
    cfg_paths = {
        "config_yaml": str(args.get("config_yaml") or ""),
        "autopilot_config": str(args.get("autopilot_config") or ""),
        "ibkr_config": str(args.get("ibkr_config") or ""),
        "marketdata_config": str(args.get("marketdata_config") or ""),
    }
    cfg_payload = {
        key: {
            "path": value,
            "content": _read_text_if_exists(value) if value else "",
        }
        for key, value in sorted(cfg_paths.items())
    }
    payload = {
        "flags": {
            "offline_safe": bool(args.get("offline_safe", False)),
            "enable_delayed_data": bool(args.get("enable_delayed_data", False)),
            "with_nexus": bool(args.get("with_nexus", False)),
            "limit": int(args.get("limit", 0) or 0),
        },
        "steps": [{"name": s.name, "argv": list(s.argv)} for s in steps],
        "config_payload": cfg_payload,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


def _run_id(*, offline_safe: bool, steps: Sequence[StepSpec], meta_args: dict[str, Any] | None) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if offline_safe:
        return f"{ts}_{_deterministic_run_suffix(steps=steps, meta_args=meta_args)}"
    return ts


def _git_sha() -> str:
    try:
        cp = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
        out = (cp.stdout or "").strip()
        return out if out else "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def parse_failure_signals(stdout: str, stderr: str, rc: int) -> list[str]:
    blob = f"{stdout}\n{stderr}"
    out: list[str] = []
    for signal, pat in _SIG_PATTERNS:
        if pat.search(blob):
            out.append(signal)
    if rc != 0 and not out:
        out.append(SIGNAL_UNKNOWN_ERROR)
    return out


def is_step_ok(*, rc: int, signals: Sequence[str]) -> bool:
    if rc != 0:
        return False
    if any(s in FATAL_SIGNALS for s in signals):
        return False
    return True


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _step_result_payload(result: StepResult) -> Dict[str, Any]:
    return {
        "name": result.name,
        "command": result.command,
        "rc": result.rc,
        "ok": result.ok,
        "status": result.status,
        "reason": result.reason,
        "signals": list(result.signals),
        "rerun_applied": result.rerun_applied,
    }


def _write_step_artifacts(out_dir: Path, step_idx: int, result: StepResult) -> None:
    prefix = f"step{step_idx}_{result.name}"
    _write_text(out_dir / f"{prefix}.stdout.txt", result.stdout)
    _write_text(out_dir / f"{prefix}.stderr.txt", result.stderr)
    _write_text(out_dir / f"{prefix}.rc.txt", str(result.rc) + "\n")


def _patch_delayed_data_flag(config_path: Path) -> tuple[bool, str]:
    raw = config_path.read_text(encoding="utf-8")
    lines = raw.splitlines()
    patched = False
    out_lines: list[str] = []
    for line in lines:
        m = re.match(r"^(\s*)(allow_delayed_data|delayed_data)\s*:\s*(.+?)\s*$", line)
        if m:
            indent, key, _value = m.groups()
            out_lines.append(f"{indent}{key}: true")
            patched = True
        else:
            out_lines.append(line)
    if not patched:
        return False, raw
    new_raw = "\n".join(out_lines)
    if raw.endswith("\n"):
        new_raw += "\n"
    config_path.write_text(new_raw, encoding="utf-8")
    return True, raw


def _suggested_actions(signals: Sequence[str]) -> list[str]:
    actions: list[str] = []
    sigs = set(signals)
    if SIGNAL_NO_MARKET_DATA_PERMISSIONS in sigs:
        actions.append("Verify IBKR market data subscriptions for requested symbols/exchanges.")
    if SIGNAL_DELAYED_DATA_ONLY in sigs:
        actions.append("Use --enable-delayed-data when config has delayed flag, or enable live market data permissions.")
    if SIGNAL_PACING_VIOLATION in sigs:
        actions.append("Reduce request rate/burst and retry after pacing window cooldown.")
    if SIGNAL_FARM_DISCONNECTED in sigs:
        actions.append("Wait for IBKR market data farm reconnect; verify TWS/Gateway connectivity state.")
    if SIGNAL_CONNECTION_REFUSED in sigs:
        actions.append("Check TWS/Gateway is running and API socket host/port are correct.")
    if SIGNAL_TIMEOUT in sigs:
        actions.append("Increase step timeout or verify local loopback/API responsiveness.")
    if SIGNAL_UNKNOWN_ERROR in sigs:
        actions.append("Inspect step stderr/stdout logs in evidence pack for root cause.")
    if not actions:
        actions.append("No action required.")
    return actions


def _build_summary(*, run_id: str, started_at_utc: str, finished_at_utc: str, step_results: Sequence[StepResult]) -> Dict[str, Any]:
    all_signals: list[str] = []
    per_step: list[Dict[str, Any]] = []
    for r in step_results:
        all_signals.extend(list(r.signals))
        per_step.append(_step_result_payload(r))
    unique_signals = sorted(set(all_signals))
    ok = all(bool(r.ok) for r in step_results)
    return {
        "run_id": run_id,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "ok": ok,
        "steps": per_step,
        "root_causes": unique_signals,
        "suggested_actions": _suggested_actions(unique_signals),
    }


def _run_step(name: str, command: list[str]) -> StepResult:
    cp = subprocess.run(command, capture_output=True, text=True, check=False)
    stdout = cp.stdout or ""
    stderr = cp.stderr or ""
    signals = parse_failure_signals(stdout=stdout, stderr=stderr, rc=int(cp.returncode))
    ok = is_step_ok(rc=int(cp.returncode), signals=signals)
    return StepResult(
        name=name,
        command=command,
        rc=int(cp.returncode),
        stdout=stdout,
        stderr=stderr,
        signals=signals,
        ok=ok,
        status="PASS" if ok else "FAIL",
    )


def _is_offline_probe_step(spec: StepSpec) -> bool:
    tokens = [spec.name.lower()] + [str(x).lower() for x in spec.argv]
    marker = " ".join(tokens)
    return any(
        tok in marker
        for tok in ("ibkr", "tws", "ib_insync", "connectasync", "run_nexus_paper.py")
    )


def _venv_marker() -> Dict[str, Any]:
    return {
        "in_venv": bool(os.environ.get("VIRTUAL_ENV")),
        "virtual_env": os.environ.get("VIRTUAL_ENV"),
    }


def _ibkr_connect_async_probe_code() -> str:
    return textwrap.dedent(
        """
        import asyncio
        import json
        import sys
        from pathlib import Path

        import yaml

        def _to_int(v, default):
            try:
                return int(v)
            except Exception:
                return int(default)

        def _load_cfg(path_str):
            p = Path(path_str)
            if not p.exists():
                raise SystemExit(f"missing_ibkr_config:{p}")
            raw = yaml.safe_load(p.read_text(encoding='utf-8')) or {}
            ib = raw.get('ibkr') if isinstance(raw.get('ibkr'), dict) else raw
            host = str(ib.get('host', '127.0.0.1'))
            port = _to_int(ib.get('port', 7497), 7497)
            client_id = _to_int(ib.get('client_id', ib.get('clientId', 901)), 901)
            timeout = float(ib.get('timeout', 5.0))
            return host, port, client_id, timeout

        async def _run(host, port, client_id, timeout):
            from ib_insync import IB
            ib = IB()
            try:
                ok = await ib.connectAsync(host, port, clientId=client_id, timeout=timeout)
                if not ok or not ib.isConnected():
                    raise SystemExit('ib_connect_failed')
                payload = {
                    'host': host,
                    'port': port,
                    'client_id': client_id,
                    'serverVersion': ib.client.serverVersion(),
                    'managedAccounts': ib.managedAccounts(),
                }
                print(json.dumps(payload, sort_keys=True))
                return 0
            finally:
                if ib.isConnected():
                    ib.disconnect()

        def main():
            cfg_path = sys.argv[1] if len(sys.argv) > 1 else 'configs/execution_ibkr.yaml'
            host, port, client_id, timeout = _load_cfg(cfg_path)
            return asyncio.run(_run(host, port, client_id, timeout))

        raise SystemExit(main())
        """
    ).strip()


def _default_steps(*, autopilot_config: str | None, limit: int, with_nexus: bool, ibkr_config: str) -> list[StepSpec]:
    if not autopilot_config:
        raise SystemExit("--autopilot-config is required when using the default chain")

    steps: list[StepSpec] = [
        StepSpec(
            name="ibkr_api_ready",
            argv=[sys.executable, "-c", _ibkr_connect_async_probe_code(), ibkr_config],
        ),
        StepSpec(
            name="autopilot_universe_train",
            argv=[
                sys.executable,
                "scripts/octa_autopilot.py",
                "--config",
                autopilot_config,
                "--limit",
                str(int(limit)),
            ],
        ),
    ]
    if with_nexus:
        steps.append(
            StepSpec(
                name="nexus_paper_smoke",
                argv=[sys.executable, "scripts/run_nexus_paper.py", "--duration-sec", "6"],
            )
        )
    return steps


def _parse_step_arg(raw: str) -> StepSpec:
    if "::" not in raw:
        raise SystemExit(f"invalid --step format (expected <name>::<command string>): {raw}")
    name, command = raw.split("::", 1)
    step_name = name.strip()
    if not step_name:
        raise SystemExit(f"invalid --step name: {raw}")
    argv = shlex.split(command)
    if not argv:
        raise SystemExit(f"invalid --step command: {raw}")
    return StepSpec(name=step_name, argv=argv)


def _load_steps_from_yaml(path: str) -> list[StepSpec]:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise SystemExit(f"--config-yaml not found: {cfg_path}")
    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    rows = data.get("steps") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not rows:
        raise SystemExit("--config-yaml must contain non-empty 'steps' list")

    out: list[StepSpec] = []
    for row in rows:
        if not isinstance(row, dict):
            raise SystemExit("each step in --config-yaml must be a mapping")
        name = str(row.get("name") or "").strip()
        if not name:
            raise SystemExit("yaml step missing 'name'")
        argv_raw = row.get("argv")
        if isinstance(argv_raw, list):
            argv = [str(x) for x in argv_raw]
        elif isinstance(row.get("command"), str):
            argv = shlex.split(str(row["command"]))
        else:
            raise SystemExit(f"yaml step '{name}' must provide argv list or command string")
        if not argv:
            raise SystemExit(f"yaml step '{name}' has empty argv")
        out.append(StepSpec(name=name, argv=argv))
    return out


def _resolve_steps(
    *,
    steps_raw: Sequence[str],
    config_yaml: str | None,
    autopilot_config: str | None,
    limit: int,
    with_nexus: bool,
    ibkr_config: str,
) -> list[StepSpec]:
    if steps_raw:
        return [_parse_step_arg(s) for s in steps_raw]
    if config_yaml:
        return _load_steps_from_yaml(config_yaml)
    return _default_steps(
        autopilot_config=autopilot_config,
        limit=limit,
        with_nexus=with_nexus,
        ibkr_config=ibkr_config,
    )


def run_smoke_chain(
    *,
    steps: Sequence[StepSpec],
    enable_delayed_data: bool,
    offline_safe: bool = False,
    delayed_data_config: str,
    out_root: Path = Path("artifacts/smoke_chain"),
    meta_args: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    run_id = f"smoke_chain_{_run_id(offline_safe=offline_safe, steps=steps, meta_args=meta_args)}"
    out_dir = out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    started = _utc_iso()

    commands = [{"name": s.name, "argv": list(s.argv)} for s in steps]

    meta = {
        "run_id": run_id,
        "started_at_utc": started,
        "git_sha": _git_sha(),
        "python_version": sys.version,
        "python_executable": sys.executable,
        "platform": sys.platform,
        "venv": _venv_marker(),
        "cwd": str(Path.cwd()),
        "args": dict(meta_args or {}),
        "commands": commands,
    }
    _write_json(out_dir / "meta.json", meta)
    _write_json(out_dir / "commands.json", {"steps": commands})

    results: list[StepResult] = []
    chain_ok = True

    for idx, spec in enumerate(steps, start=1):
        if not chain_ok:
            result = StepResult(
                name=spec.name,
                command=list(spec.argv),
                rc=99,
                stdout="",
                stderr="not_run_due_to_previous_failure",
                signals=[SIGNAL_UNKNOWN_ERROR],
                ok=False,
                status="FAIL",
                reason="UPSTREAM_FAILURE",
            )
            results.append(result)
            _write_step_artifacts(out_dir, idx, result)
            continue

        if offline_safe and _is_offline_probe_step(spec):
            result = StepResult(
                name=spec.name,
                command=list(spec.argv),
                rc=0,
                stdout="offline_safe_skip\n",
                stderr="OFFLINE_SAFE\n",
                signals=[],
                ok=True,
                status="SKIP",
                reason="OFFLINE_SAFE",
            )
            results.append(result)
            _write_step_artifacts(out_dir, idx, result)
            continue

        result = _run_step(spec.name, list(spec.argv))
        rerun_applied = False
        if idx == 1 and (not result.ok) and enable_delayed_data and (SIGNAL_DELAYED_DATA_ONLY in result.signals):
            cfg_path = Path(delayed_data_config)
            if cfg_path.exists():
                before = cfg_path.read_text(encoding="utf-8")
                patched, _ = _patch_delayed_data_flag(cfg_path)
                if patched:
                    rerun_applied = True
                    _write_text(out_dir / "marketdata_ibkr.before.yaml", before)
                    _write_text(out_dir / "marketdata_ibkr.after.yaml", cfg_path.read_text(encoding="utf-8"))
                    result = _run_step(spec.name, list(spec.argv))

        result = StepResult(
            name=result.name,
            command=result.command,
            rc=result.rc,
            stdout=result.stdout,
            stderr=result.stderr,
            signals=result.signals,
            ok=result.ok,
            status=result.status,
            reason=result.reason,
            rerun_applied=rerun_applied,
        )
        results.append(result)
        _write_step_artifacts(out_dir, idx, result)
        chain_ok = bool(result.ok)

    finished = _utc_iso()
    summary = _build_summary(
        run_id=run_id,
        started_at_utc=started,
        finished_at_utc=finished,
        step_results=results,
    )
    _write_json(out_dir / "summary.json", summary)
    return {"out_dir": str(out_dir), "summary": summary}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run configured smoke-chain steps with evidence artifacts.")
    ap.add_argument("--step", action="append", default=[], help="Repeatable: <name>::<command string>")
    ap.add_argument("--config-yaml", default=None, help="YAML file containing steps: [{name, argv|command}]")
    ap.add_argument("--autopilot-config", default=None, help="Required for default chain step autopilot_universe_train")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--with-nexus", action="store_true", default=False)
    ap.add_argument("--ibkr-config", default="configs/execution_ibkr.yaml")
    ap.add_argument("--marketdata-config", default="configs/marketdata_ibkr.yaml")
    ap.add_argument("--enable-delayed-data", action="store_true", default=False)
    ap.add_argument("--offline-safe", action="store_true", default=False)
    ap.add_argument("--out-root", default="artifacts/smoke_chain")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    steps = _resolve_steps(
        steps_raw=list(args.step),
        config_yaml=str(args.config_yaml) if args.config_yaml else None,
        autopilot_config=str(args.autopilot_config) if args.autopilot_config else None,
        limit=int(args.limit),
        with_nexus=bool(args.with_nexus),
        ibkr_config=str(args.ibkr_config),
    )

    result = run_smoke_chain(
        steps=steps,
        enable_delayed_data=bool(args.enable_delayed_data),
        offline_safe=bool(args.offline_safe),
        delayed_data_config=str(args.marketdata_config),
        out_root=Path(str(args.out_root)),
        meta_args={
            "step": list(args.step),
            "config_yaml": args.config_yaml,
            "autopilot_config": args.autopilot_config,
            "limit": int(args.limit),
            "with_nexus": bool(args.with_nexus),
            "ibkr_config": str(args.ibkr_config),
            "marketdata_config": str(args.marketdata_config),
            "enable_delayed_data": bool(args.enable_delayed_data),
            "offline_safe": bool(args.offline_safe),
        },
    )
    payload = {
        "run_id": result["summary"]["run_id"],
        "ok": bool(result["summary"]["ok"]),
        "root_causes": result["summary"]["root_causes"],
        "evidence_dir": result["out_dir"],
    }
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
