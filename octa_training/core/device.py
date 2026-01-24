from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass
from typing import Optional

try:
    import psutil
except Exception:
    psutil = None


@dataclass
class DeviceProfile:
    has_gpu: bool
    gpu_name: Optional[str]
    gpu_mem_mb: Optional[int]
    cpu_cores: int
    cpu_threads: int
    ram_mb_total: int
    ram_mb_avail: int


def _nvidia_smi_info() -> Optional[dict]:
    try:
        out = subprocess.check_output(["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"], stderr=subprocess.DEVNULL, text=True)
        # parse first line
        line = out.strip().splitlines()[0]
        name, mem = [p.strip() for p in line.split(",")]
        return {"name": name, "mem_mb": int(mem)}
    except Exception:
        return None


def detect_device() -> DeviceProfile:
    # CPU
    if psutil:
        cpu_physical = psutil.cpu_count(logical=False) or 1
        cpu_logical = psutil.cpu_count(logical=True) or cpu_physical
        vm = psutil.virtual_memory()
        ram_total = int(vm.total / (1024 * 1024))
        ram_avail = int(vm.available / (1024 * 1024))
    else:
        cpu_logical = os.cpu_count() or 1
        cpu_physical = cpu_logical
        ram_total = 0
        ram_avail = 0

    # GPU detection: prefer nvidia-smi, fallback to torch if available
    gpu_name = None
    gpu_mem = None
    has_gpu = False

    ninfo = _nvidia_smi_info()
    if ninfo:
        has_gpu = True
        gpu_name = ninfo.get("name")
        gpu_mem = ninfo.get("mem_mb")
    else:
        try:
            import torch

            if torch.cuda.is_available():
                has_gpu = True
                gpu_name = torch.cuda.get_device_name(0)
                # try to get mem via torch
                try:
                    props = torch.cuda.get_device_properties(0)
                    gpu_mem = int(props.total_memory / (1024 * 1024))
                except Exception:
                    gpu_mem = None
        except Exception:
            pass

    return DeviceProfile(has_gpu=has_gpu, gpu_name=gpu_name, gpu_mem_mb=gpu_mem, cpu_cores=cpu_physical, cpu_threads=cpu_logical, ram_mb_total=ram_total, ram_mb_avail=ram_avail)


def apply_threading_policy(profile: DeviceProfile, cfg) -> int:
    """Apply a conservative threading policy for numerical backends.

    Important: default behavior is preserved unless cfg.compute.enabled is True
    or OCTA_CPU_OPTIMIZED=1 is set.
    """

    compute = getattr(cfg, "compute", None)
    cpu_optimized = bool(getattr(compute, "enabled", False)) or str(os.getenv("OCTA_CPU_OPTIMIZED", "")).strip() == "1"

    max_workers = int(getattr(cfg, "max_workers", None) or 4)

    if not cpu_optimized:
        # Legacy behavior: allow up to max_workers threads, leave ~1 logical thread free.
        threads = max(1, min(profile.cpu_threads - 1 if profile.cpu_threads > 1 else 1, max_workers))
        os.environ.setdefault("OMP_NUM_THREADS", str(threads))
        os.environ.setdefault("MKL_NUM_THREADS", str(threads))
        os.environ.setdefault("OPENBLAS_NUM_THREADS", str(threads))
        return threads

    # CPU-optimized policy: reserve one physical core and avoid oversubscription.
    reserve_cores = int(getattr(compute, "reserve_cores", 1) or 0)
    reserve_cores = max(0, reserve_cores)
    cpu_units = int(profile.cpu_cores if bool(getattr(compute, "prefer_physical_cores", True)) else profile.cpu_threads)
    cpu_units = max(1, cpu_units)
    max(1, cpu_units - reserve_cores)

    # For daemon-style parallelism, we generally want BLAS threads per worker = 1.
    # For single-job workflows, allow an override.
    single_job_threads = getattr(compute, "single_job_blas_threads", None)
    if isinstance(single_job_threads, int) and single_job_threads and single_job_threads > 0 and max_workers <= 1:
        threads = int(single_job_threads)
    else:
        threads = int(getattr(compute, "blas_threads_per_worker", 1) or 1)

    threads = max(1, threads)
    # Also export an explicit OCTA hint that downstream code can read.
    os.environ.setdefault("OCTA_TRAIN_THREADS", str(threads))

    # Common math backends
    os.environ.setdefault("OMP_NUM_THREADS", str(threads))
    os.environ.setdefault("MKL_NUM_THREADS", str(threads))
    os.environ.setdefault("OPENBLAS_NUM_THREADS", str(threads))
    os.environ.setdefault("NUMEXPR_NUM_THREADS", str(threads))
    os.environ.setdefault("BLIS_NUM_THREADS", str(threads))
    # macOS/Accelerate (harmless on Linux)
    os.environ.setdefault("VECLIB_MAXIMUM_THREADS", str(threads))
    # Keep OpenMP from opportunistically expanding thread counts.
    os.environ.setdefault("OMP_DYNAMIC", "FALSE")

    # Note: we do NOT change process-level affinity or nice by default.
    return threads


def memory_guard(profile: DeviceProfile, cfg) -> int:
    min_ram = getattr(cfg, "min_ram_mb", 1024)
    workers = getattr(cfg, "max_workers", 1)
    if profile.ram_mb_avail and profile.ram_mb_avail < min_ram:
        # reduce workers
        safe = max(1, int(profile.ram_mb_avail / 1024))
        return min(safe, workers)
    return workers


def choose_training_device(prefer_gpu: bool, profile: DeviceProfile) -> str:
    if prefer_gpu and profile.has_gpu:
        return "gpu"
    return "cpu"


def safe_set_xgb_params(prefer_gpu: bool, profile: DeviceProfile) -> dict:
    params = {}
    if prefer_gpu and profile.has_gpu:
        params.update({"tree_method": "gpu_hist", "predictor": "gpu_predictor"})
    else:
        params.update({"tree_method": "hist"})
    return params


def safe_set_lgbm_params(prefer_gpu: bool, profile: DeviceProfile) -> dict:
    params = {}
    if prefer_gpu and profile.has_gpu:
        params.update({"device": "gpu"})
    else:
        params.update({"device": "cpu"})
    return params


def safe_set_cat_params(prefer_gpu: bool, profile: DeviceProfile) -> dict:
    params = {}
    if prefer_gpu and profile.has_gpu:
        params.update({"task_type": "GPU"})
    return params


def profile_to_json(profile: DeviceProfile) -> str:
    data = asdict(profile)
    try:
        import orjson  # type: ignore

        return orjson.dumps(data).decode("utf-8")
    except Exception:
        return json.dumps(data, ensure_ascii=False)
