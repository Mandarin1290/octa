from __future__ import annotations

import typer

from octa.foundation.control_plane import run_foundation_training

app = typer.Typer(add_completion=False)


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
    if altdata_config or altdata_run_id or var_root:
        raise typer.BadParameter(
            "non_canonical_training_options: use the canonical Foundation control plane without altdata or var-root overrides",
            param_hint="--altdata-config/--altdata-run-id/--var-root",
        )
    allowlist = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    run_foundation_training(
        run_id=run_id,
        config_path=config,
        resume=resume,
        max_symbols=universe_limit,
        symbols=",".join(allowlist) if allowlist else None,
    )


if __name__ == "__main__":
    app()
