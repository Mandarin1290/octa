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
    print(f"Fingerprint: {cfg_fp}")
    
    # Update manifest
    manifest_path = Path("reports/gates/20260120T181614Z/gate_manifest.json")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        manifest["config_fingerprint"] = cfg_fp
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"Updated {manifest_path}")

if __name__ == "__main__":
    main()
