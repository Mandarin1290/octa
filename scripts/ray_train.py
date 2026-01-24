from __future__ import annotations

import argparse
import subprocess
import sys

try:
    import ray
except Exception:
    ray = None


def local_train(parquet: str, target: str, version: str, n_jobs: int = 1):
    cmd = [sys.executable, '-m', 'scripts.train_and_save', '--parquet', parquet, '--target', target, '--version', version]
    print('Running local train:', ' '.join(cmd))
    subprocess.check_call(cmd)


def ray_train(parquet: str, target: str, version: str, num_workers: int = 2):
    import ray
    from ray import tune

    def train_fn(config):
        import subprocess
        cmd = [sys.executable, '-m', 'scripts.train_and_save', '--parquet', parquet, '--target', target, '--version', f"{version}-ray-{config['worker']}" ]
        subprocess.check_call(cmd)

    ray.init()
    analysis = tune.run(train_fn, resources_per_trial={"cpu": 1}, config={"worker": list(range(num_workers))}, num_samples=1)
    print('Ray results:', analysis.trials)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--parquet', required=True)
    p.add_argument('--target', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--workers', type=int, default=2)
    args = p.parse_args()

    if ray is None:
        print('Ray not installed, falling back to local training')
        local_train(args.parquet, args.target, args.version)
    else:
        ray_train(args.parquet, args.target, args.version, args.workers)
