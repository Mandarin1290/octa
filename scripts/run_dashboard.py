from __future__ import annotations

import argparse
from pathlib import Path

import yaml


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


def main() -> None:
    ap = argparse.ArgumentParser(description="Run OCTA control plane (FastAPI)")
    ap.add_argument("--features", default="octa_core/config/octa_features.yaml")
    args = ap.parse_args()

    cfg = _load_yaml(args.features)
    feats = cfg.get("features") or {}
    cp = feats.get("control_plane") if isinstance(feats.get("control_plane"), dict) else {}
    if not bool(cp.get("enabled", False)):
        raise SystemExit("control_plane disabled: set features.control_plane.enabled=true")

    host = str(cp.get("host", "127.0.0.1"))
    port = int(cp.get("port", 8080))

    from octa_core.control_plane.api import create_app

    app = create_app(features_path=args.features)

    import uvicorn

    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
