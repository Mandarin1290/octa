#!/usr/bin/env python3
"""Refine option labels: apply transaction costs and thresholding.

Writes `raw/{symbol}_labeled_refined.parquet`.
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def refine(path: Path, out: Path, cost_pct: float = 0.0, quantile: float = None, min_abs: float = None):
    logging.info('Loading %s', path)
    df = pd.read_parquet(path)
    if 'delta_hedged_return' not in df.columns:
        raise RuntimeError('delta_hedged_return missing; run prepare_options_labels first')

    # cost_pct interpreted as fraction of notional (e.g. 0.001 = 0.1%)
    if cost_pct > 0:
        df['delta_hedged_return_net'] = df['delta_hedged_return'] - cost_pct
    else:
        df['delta_hedged_return_net'] = df['delta_hedged_return']

    # threshold by quantile (safe for NaN)
    if quantile is not None:
        if df['delta_hedged_return_net'].notna().any():
            thr = df['delta_hedged_return_net'].abs().quantile(quantile)
            if np.isnan(thr):
                logging.warning('Quantile produced NaN; falling back to sign-based labels')
                df['label_refined_pos'] = (df['delta_hedged_return_net'] > 0).astype(int)
            else:
                logging.info('Quantile %s threshold = %s', quantile, thr)
                df['label_refined_pos'] = (df['delta_hedged_return_net'] > thr).astype(int)
        else:
            logging.warning('All delta_hedged_return_net are NaN; using sign fallback')
            df['label_refined_pos'] = (df['delta_hedged_return_net'] > 0).astype(int)
    elif min_abs is not None:
        df['label_refined_pos'] = (df['delta_hedged_return_net'] > min_abs).astype(int)
    else:
        # default: sign of net return
        df['label_refined_pos'] = (df['delta_hedged_return_net'] > 0).astype(int)

    logging.info('Writing refined parquet to %s rows=%d', out, len(df))
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', required=True)
    p.add_argument('--cost_pct', type=float, default=0.0)
    p.add_argument('--quantile', type=float, default=None)
    p.add_argument('--min_abs', type=float, default=None)
    p.add_argument('--out', default=None)
    args = p.parse_args()
    src = Path('raw') / f"{args.symbol}.parquet"
    out = Path('raw') / f"{args.symbol}_refined.parquet" if args.out is None else Path(args.out)
    refine(src, out, cost_pct=args.cost_pct, quantile=args.quantile, min_abs=args.min_abs)


if __name__ == '__main__':
    main()
