#!/usr/bin/env python3
"""Batch runner to process assets in `raw/`.

Current behaviour:
- Detects files in `raw/` ending with `_OPT.parquet` (case-insensitive) and runs:
  1. scripts/prepare_options_labels.py --symbol <SYMBOL> --horizon_days 1
  2. scripts/refine_option_labels.py --symbol <SYMBOL>_labeled --cost_pct 0.0005 --quantile 0.75
  3. scripts/train_options_time_series.py --symbol <SYMBOL>_labeled_refined --folds 5
  4. scripts/train_options_lstm_torch.py --symbol <SYMBOL>_labeled_refined --seq_len 16 --epochs 2 --batch_size 512

Usage: python scripts/run_all_assets.py
"""
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

ROOT = Path('.')
RAW = ROOT / 'raw'
SCRIPTS = ROOT / 'scripts'

PY = sys.executable
ENV_PY_PREFIX = ''
if 'VIRTUAL_ENV' in os.environ:
    # preserve PYTHONPATH usage consistent with previous runs
    pass


def run(cmd, check=False):
    logging.info('RUN: %s', ' '.join(cmd))
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    logging.info(res.stdout)
    if check and res.returncode != 0:
        raise RuntimeError(f'Command failed: {cmd} rc={res.returncode}')
    return res.returncode


def find_option_parquets():
    files = []
    for p in RAW.glob('*.parquet'):
        name = p.stem
        if name.lower().endswith('_opt'):
            files.append(p)
    return sorted(files)


def process_option(p: Path):
    sym = p.stem
    logging.info('Processing option symbol %s', sym)
    # 1 prepare
    run([PY, str(SCRIPTS / 'prepare_options_labels.py'), '--symbol', sym, '--horizon_days', '1'])
    # 2 refine
    run([PY, str(SCRIPTS / 'refine_option_labels.py'), '--symbol', f"{sym}_labeled", '--cost_pct', '0.0005', '--quantile', '0.75'])
    # 3 wf train
    run([PY, str(SCRIPTS / 'train_options_time_series.py'), '--symbol', f"{sym}_labeled_refined", '--folds', '5'])
    # 4 lstm train
    run([PY, str(SCRIPTS / 'train_options_lstm_torch.py'), '--symbol', f"{sym}_labeled_refined", '--seq_len', '16', '--epochs', '2', '--batch_size', '512'])


def main():
    opt_files = find_option_parquets()
    if not opt_files:
        logging.info('No option parquets found in raw/')
        return
    for p in opt_files:
        try:
            process_option(p)
        except Exception as e:
            logging.exception('Failed processing %s: %s', p, e)


if __name__ == '__main__':
    import os
    main()
