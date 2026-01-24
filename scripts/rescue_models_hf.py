import json
import shutil
import sys
from pathlib import Path
import yaml

# Hardcoded thresholds matching the new hf_defaults.yaml
# (Reading yaml involves dependencies, hardcoding is safer/faster for this rescue script)

# Realistische Filter
THRESHOLDS = {
    'turnover_per_day': 4.0,      # maximal 4x pro Tag
    'net_to_gross': 0.30,         # mindestens 0.3
    'sharpe_min': 0.7,            # mindestens 0.7
    'sharpe_max': 6.0,            # maximal 6.0 (alles darüber ist verdächtig)
    'avg_net_trade_return': 0.00004, # mindestens 0.004%
    'min_trades': 50              # mindestens 50 Trades
}

def check_model(meta_path):
    try:
        with open(meta_path, 'r') as f:
            data = json.load(f)
        
        metrics = data.get('metrics', {})
        

        # Harte Filter
        if metrics.get('turnover_per_day', 999) > THRESHOLDS['turnover_per_day']:
            return False, f"Turnover {metrics.get('turnover_per_day'):.2f} > {THRESHOLDS['turnover_per_day']}"
        if metrics.get('net_to_gross', -1) < THRESHOLDS['net_to_gross']:
            return False, f"Net/Gross {metrics.get('net_to_gross'):.2f} < {THRESHOLDS['net_to_gross']}"
        sharpe = metrics.get('sharpe', -1)
        if sharpe < THRESHOLDS['sharpe_min']:
            return False, f"Sharpe {sharpe:.2f} < {THRESHOLDS['sharpe_min']}"
        if sharpe > THRESHOLDS['sharpe_max']:
            return False, f"Sharpe {sharpe:.2f} > {THRESHOLDS['sharpe_max']}"
        if metrics.get('avg_net_trade_return', -1) < THRESHOLDS['avg_net_trade_return']:
            return False, f"AvgReturn {metrics.get('avg_net_trade_return'):.2e} < {THRESHOLDS['avg_net_trade_return']}"
        if metrics.get('n_trades', 0) < THRESHOLDS['min_trades']:
            return False, f"n_trades {metrics.get('n_trades', 0)} < {THRESHOLDS['min_trades']}"
        return True, "Passed"

    except Exception as e:
        return False, str(e)

def main():
    root = Path("/home/n-b/Octa/raw/PKL/_cascade_hf")
    fail_dir = root / "_debug_fail"
    
    if not fail_dir.exists():
        print(f"No debug dir found at {fail_dir}")
        return

    rescued = []
    
    print(f"Scanning {fail_dir} for rescue candidates...")
    files = list(fail_dir.glob("*.meta.json"))
    print(f"Found {len(files)} meta files.")
    
    for meta_file in files:
        passed, msg = check_model(meta_file)
        pkl_file = meta_file.with_suffix('') # Strip .json? No, .meta.json -> .meta -> wrong.
        # Format is STEM.meta.json and STEM.pkl
        # meta_file.name is "SYM.meta.json"
        
        sym = meta_file.name.replace(".meta.json", "")
        pkl_file = fail_dir / f"{sym}.pkl"
        
        if passed:
            if pkl_file.exists():
                # Move to root
                dest_pkl = root / pkl_file.name
                dest_meta = root / meta_file.name
                
                print(f"RESCUING {sym} (Sharpe={json.load(open(meta_file))['metrics']['sharpe']:.2f})")
                shutil.copy(pkl_file, dest_pkl)
                shutil.copy(meta_file, dest_meta)
                rescued.append(sym)
            else:
                print(f"Warning: {sym} passed but no PKL found.")
        else:
            # print(f"Skipping {sym}: {msg}")
            pass

    print(f"\nRescued {len(rescued)} models.")
    
    # Write pass list
    with open(root / "rescued_pass_list.txt", "w") as f:
        for r in rescued:
            f.write(f"{r}\n")

if __name__ == "__main__":
    main()
