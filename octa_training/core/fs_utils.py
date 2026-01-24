import os
import shutil
import time
from datetime import datetime, timedelta


def get_free_gb(path="/"):
    total, used, free = shutil.disk_usage(path)
    return free / (1024 ** 3)


def _iter_files_older_than(root_dir, days=30, pattern_exts=None):
    cutoff = datetime.utcnow() - timedelta(days=days)
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if pattern_exts and not any(fn.endswith(ext) for ext in pattern_exts):
                continue
            fp = os.path.join(dirpath, fn)
            try:
                mtime = datetime.utcfromtimestamp(os.path.getmtime(fp))
            except OSError:
                continue
            if mtime < cutoff:
                yield fp


def prune_old_reports(reports_dir, days=30, dry_run=False):
    if not os.path.isdir(reports_dir):
        return 0
    removed = 0
    for fp in _iter_files_older_than(reports_dir, days=days, pattern_exts=[".json", ".csv", ".parquet"]):
        try:
            if dry_run:
                removed += 1
                continue
            os.remove(fp)
            removed += 1
        except Exception:
            continue
    return removed


def prune_research_artifacts(workspace_root, days=7, dry_run=False):
    # Prune likely large temporary artifacts from research folders (pick common folders)
    removed = 0
    candidates = [
        os.path.join(workspace_root, "reports"),
        os.path.join(workspace_root, "raw"),
        os.path.join(workspace_root, "altdata"),
        os.path.join(workspace_root, "OCTA_FIX_PACK"),
    ]
    for cand in candidates:
        if not os.path.isdir(cand):
            continue
        for fp in _iter_files_older_than(cand, days=days, pattern_exts=[".parquet", ".pkl", ".joblib", ".json"]):
            try:
                if dry_run:
                    removed += 1
                    continue
                os.remove(fp)
                removed += 1
            except Exception:
                continue
    return removed


def ensure_disk_space(required_gb=5.0, workspace_root=".", try_prune=True):
    free = get_free_gb(workspace_root)
    if free >= required_gb:
        return True
    if not try_prune:
        return False
    # Attempt light pruning
    prune_research_artifacts(workspace_root, days=3, dry_run=False)
    prune_old_reports(os.path.join(workspace_root, "reports"), days=7, dry_run=False)
    # small sleep to allow fs to settle
    time.sleep(0.5)
    free = get_free_gb(workspace_root)
    return free >= required_gb
