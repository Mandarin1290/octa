import os

from octa_training.core.device import DeviceProfile, apply_threading_policy


class _Cfg:
    def __init__(self):
        class _Compute:
            enabled = True
            reserve_cores = 1
            prefer_physical_cores = True
            auto_cap_max_workers = True
            blas_threads_per_worker = 1
            single_job_blas_threads = None

        self.compute = _Compute()
        self.max_workers = 4


def test_cpu_optimized_sets_conservative_thread_envs(monkeypatch):
    # Clear any existing envs so setdefault behavior is deterministic.
    for k in [
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "BLIS_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "OMP_DYNAMIC",
        "OCTA_TRAIN_THREADS",
    ]:
        monkeypatch.delenv(k, raising=False)

    prof = DeviceProfile(
        has_gpu=False,
        gpu_name=None,
        gpu_mem_mb=None,
        cpu_cores=4,
        cpu_threads=8,
        ram_mb_total=0,
        ram_mb_avail=0,
    )
    cfg = _Cfg()
    threads = apply_threading_policy(prof, cfg)

    assert threads == 1
    assert os.environ.get("OMP_NUM_THREADS") == "1"
    assert os.environ.get("MKL_NUM_THREADS") == "1"
    assert os.environ.get("OPENBLAS_NUM_THREADS") == "1"
    assert os.environ.get("NUMEXPR_NUM_THREADS") == "1"
    assert os.environ.get("OCTA_TRAIN_THREADS") == "1"
    assert os.environ.get("OMP_DYNAMIC") == "FALSE"
