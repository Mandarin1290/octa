import multiprocessing


def pytest_configure(config):
    # Ensure a safe start method in multi-threaded test environments
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        # already set; ignore
        pass


import pathlib
import sys

# Ensure repository root is on sys.path for imports during tests
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
