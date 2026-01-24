from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path


class LockError(Exception):
    pass


@contextmanager
def symbol_lock(state_dir: Path, symbol: str):
    """Context manager for per-symbol file lock.

    Uses POSIX file locking (fcntl). Lock file is placed under state_dir/locks/<symbol>.lock
    """
    lock_dir = Path(state_dir) / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    safe_name = symbol.replace('/', '_')
    lock_path = lock_dir / f"{safe_name}.lock"
    fd = None
    try:
        fd = open(lock_path, 'w')
        # acquire exclusive lock, blocking
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX)
        yield
    except Exception as e:
        raise LockError(str(e)) from e
    finally:
        if fd:
            try:
                fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                fd.close()
            except Exception:
                pass
