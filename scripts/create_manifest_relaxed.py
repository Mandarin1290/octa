import json
import hashlib
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from octa_training.core.config import load_config
from core.training_safety_lock import _json_default, _sha256_of_str

def main():
    cfg_path = "configs/cascade_hf.yaml"
    cfg = load_config(cfg_path)
    
    raw = cfg.model_dump() if hasattr(cfg, "model_dump") else (cfg.dict() if hasattr(cfg, "dict") else {})
    cfg_json = json.dumps(raw, sort_keys=True, default=_json_default)
    cfg_fp = _sha256_of_str(cfg_json)
    
    print(f"Config: {cfg_path}")
    print(f"New Fingerprint: {cfg_fp}")
    
    run_id = "20260121T120000Z"
    manifest = {
        "run_id": run_id,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "reports_dir": "/home/n-b/Octa/reports",
        "dataset": "stocks",
        "manifest_dir": f"/home/n-b/Octa/reports/gates/{run_id}",
        "artifacts_dir": f"/home/n-b/Octa/reports/cascade/{run_id}/global_gate_1d",
        "config_fingerprint": cfg_fp
    }
    
    manifest_path = Path(f"reports/gates/{run_id}/gate_manifest.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote manifest to {manifest_path}")

if __name__ == "__main__":
    main()
