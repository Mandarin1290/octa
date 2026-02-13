from __future__ import annotations

import os
import subprocess
import sys
from typing import List

import typer

from octa.core.orchestration.runner import run_cascade

app = typer.Typer(add_completion=False)


def _ensure_deps() -> None:
    missing: List[str] = []
    for mod in ("duckdb", "dagster", "typer"):
        try:
            __import__(mod)
        except Exception:
            missing.append(mod)

    if not missing:
        return

    if os.getenv("OCTA_AUTO_INSTALL", "").lower() in {"1", "true", "yes"}:
        req_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "requirements-ops.txt")
        req_path = os.path.abspath(req_path)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_path])
        return

    raise RuntimeError(
        f"Missing dependencies: {', '.join(missing)}. Install with requirements-ops.txt or set OCTA_AUTO_INSTALL=true"
    )


@app.command()
def main(
    config: str = typer.Option(None, "--config", help="Path to training config yaml"),
    resume: bool = typer.Option(False, "--resume", help="Resume from checkpoints"),
    run_id: str = typer.Option(None, "--run-id", help="Override run id"),
    universe_limit: int = typer.Option(0, "--universe-limit", help="Limit number of symbols"),
    symbols: str = typer.Option("", "--symbols", help="Comma-separated symbol allowlist"),
    altdata_config: str = typer.Option(None, "--altdata-config", help="Altdata config yaml"),
    altdata_run_id: str = typer.Option(None, "--altdata-run-id", help="Altdata FeatureRegistry run id"),
    var_root: str = typer.Option(None, "--var-root", help="Override octa/var root"),
):
    _ensure_deps()
    allowlist = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    run_cascade(
        config_path=config,
        resume=resume,
        run_id=run_id,
        universe_limit=universe_limit,
        symbols=allowlist if allowlist else None,
        altdata_config_path=altdata_config,
        altdata_run_id=altdata_run_id,
        var_root=var_root,
    )


if __name__ == "__main__":
    app()
