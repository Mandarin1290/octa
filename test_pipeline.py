#!/usr/bin/env python3
import traceback

from octa_training.core.config import load_config
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry

cfg = load_config()
print("Config signal:", hasattr(cfg, 'signal'), getattr(cfg, 'signal', None))
print("Tuning enabled:", cfg.tuning.enabled)
print("Models order:", cfg.tuning.models_order)
state = StateRegistry(cfg.paths.state_dir)

try:
    result = train_evaluate_package("AAPL_1M", cfg, state, "test_run", safe_mode=False, smoke_test=False)
    print("Result:", result)
except Exception as e:
    print("Error:", e)
    traceback.print_exc()
