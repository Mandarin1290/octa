from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:
    BackgroundScheduler = None

from core.training_safety_lock import TrainingSafetyLockError, assert_training_armed
from octa_training.core.config import load_config
from octa_training.core.device import (
    apply_threading_policy,
    detect_device,
    memory_guard,
)
from octa_training.core.io_parquet import discover_parquets
from octa_training.core.locks import symbol_lock
from octa_training.core.mem_profile import maybe_start as mem_maybe_start
from octa_training.core.mem_profile import snapshot as mem_snapshot
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry

logger = logging.getLogger("octa_training.daemon")


class Daemon:
    def __init__(self, cfg_path: str | None = None):
        self.cfg = load_config(cfg_path)
        self.state = StateRegistry(self.cfg.paths.state_dir)
        self.scheduler = BackgroundScheduler() if BackgroundScheduler is not None else None
        self.max_workers = int(getattr(self.cfg, 'max_workers', 2))
        self.device = detect_device()

        # Optional compute policy (disabled by default)
        try:
            compute = getattr(self.cfg, 'compute', None)
            if compute is not None and bool(getattr(compute, 'enabled', False)):
                reserve_cores = int(getattr(compute, 'reserve_cores', 1) or 0)
                reserve_cores = max(0, reserve_cores)
                cpu_units = int(self.device.cpu_cores if bool(getattr(compute, 'prefer_physical_cores', True)) else self.device.cpu_threads)
                cpu_units = max(1, cpu_units)
                avail = max(1, cpu_units - reserve_cores)
                if bool(getattr(compute, 'auto_cap_max_workers', True)):
                    self.max_workers = max(1, min(self.max_workers, avail))

                # Apply conservative BLAS/OpenMP threading caps
                apply_threading_policy(self.device, self.cfg)
        except Exception:
            # Never fail daemon startup due to optional policy wiring.
            pass

    def start(self):
        logger.info("Starting daemon")
        # recover stale running jobs
        try:
            requeued = self.state.abort_stale_running(timeout_seconds=3600)
            if requeued:
                logger.info("Requeued stale jobs", extra={"requeued": requeued})
        except Exception:
            logger.exception("Failed to abort stale running jobs")

        # schedule periodic discovery (if apscheduler available)
        if self.scheduler is not None:
            self.scheduler.add_job(self.discover_and_enqueue, 'interval', seconds=60, id='discover')
            self.scheduler.start()

        try:
            self.run_loop()
        finally:
            if self.scheduler is not None:
                self.scheduler.shutdown()

    def discover_and_enqueue(self):
        discovered = discover_parquets(Path(self.cfg.paths.raw_dir), state=self.state)
        for p in discovered:
            s = self.state.get_symbol_state(p.symbol) or {}
            existing_hash = s.get('last_seen_parquet_hash')
            if existing_hash != p.sha256:
                logger.info("Detected changed parquet; enqueueing", extra={"symbol": p.symbol})
                self.state.enqueue(p.symbol, reason='file_change')
                # persist seen
                self.state.update_symbol_state(p.symbol, last_seen_parquet_hash=p.sha256, last_parquet_mtime=p.mtime, last_parquet_size=p.size)

    def run_loop(self):
        # Apply memory-based worker guard (conservative)
        try:
            safe_workers = memory_guard(self.device, self.cfg)
            if isinstance(safe_workers, int) and safe_workers >= 1:
                self.max_workers = min(self.max_workers, safe_workers)
        except Exception:
            pass

        with ThreadPoolExecutor(max_workers=self.max_workers) as exe:
            futures = {}
            while True:
                # refresh device profile
                try:
                    prof = detect_device()
                    self.device = prof
                except Exception:
                    prof = self.device

                # Optional: re-apply threading policy on topology changes
                try:
                    compute = getattr(self.cfg, 'compute', None)
                    if compute is not None and bool(getattr(compute, 'enabled', False)):
                        apply_threading_policy(prof, self.cfg)
                except Exception:
                    pass

                try:
                    q = self.state.dequeue_ready()
                except Exception:
                    q = None

                if q:
                    qid = q['id']
                    symbol = q['symbol']
                    run_id = f"daemon-{int(time.time())}-{symbol}"
                    self.state.mark_running(qid, symbol, run_id)
                    futures[exe.submit(self._run_job, qid, symbol, run_id)] = (qid, symbol)

                # collect completed
                done = [f for f in list(futures.keys()) if f.done()]
                for f in done:
                    qid, symbol = futures.pop(f)
                    try:
                        res = f.result()
                        status = 'SUCCESS' if getattr(res, 'passed', False) else 'FAIL'
                        try:
                            self.state.mark_done(qid, res.run_id, status)
                        except Exception:
                            logger.exception("Failed to mark_done")
                        repdir = Path(self.cfg.paths.reports_dir) / 'daemon' / datetime.utcnow().strftime('%Y%m%d')
                        repdir.mkdir(parents=True, exist_ok=True)
                        outp = repdir / f"{symbol}_{res.run_id}.json"
                        import json
                        outp.write_text(json.dumps({"symbol": symbol, "run_id": res.run_id, "passed": getattr(res, 'passed', False), "metrics": (res.metrics.dict() if getattr(res, 'metrics', None) else None), "gate": (res.gate_result.dict() if getattr(res, 'gate_result', None) else None), "pack": getattr(res, 'pack_result', None)}, default=str))
                        logger.info("Job finished", extra={"symbol": symbol, "run_id": res.run_id, "status": status})
                    except Exception as e:
                        logger.exception("Job future raised", extra={"symbol": symbol, "error": str(e)})

                time.sleep(1)

    def _run_job(self, qid: int, symbol: str, run_id: str):
        with symbol_lock(self.cfg.paths.state_dir, symbol):
            if mem_maybe_start():
                mem_snapshot(label=f"daemon:before_job:{symbol}", logger=logger)
            try:
                assert_training_armed(self.cfg, symbol, "1D")
            except TrainingSafetyLockError as e:
                logger.warning("Daemon blocked training by safety lock", extra={"symbol": symbol, "reason": str(e)})
                # mark as failed/blocked in state
                self.state.update_symbol_state(symbol, last_gate_result="LOCK_BLOCKED")
                class _Res:
                    passed = False
                    run_id = run_id
                    metrics = None
                    gate_result = None
                    pack_result = None

                return _Res()
            res = train_evaluate_package(symbol, self.cfg, self.state, run_id, safe_mode=False, smoke_test=getattr(self.cfg, 'smoke_test_after_package', False), logger=logger)
            if mem_maybe_start():
                mem_snapshot(label=f"daemon:after_job:{symbol}", logger=logger)
            return res


def main():
    d = Daemon()
    d.start()


if __name__ == '__main__':
    main()
