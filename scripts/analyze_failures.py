import pandas as pd
import sys

# Load the diagnostics
try:
    df = pd.read_csv('/home/n-b/Octa/reports/cascade/20260121T120000Z/1H/diagnostics/fast_reason_report/rows.csv', names=['symbol', 'status', 'path', 'reason'])

    # Filter out FX data failures
    non_fx_df = df[~df['reason'].str.contains('fx_g1', na=False)]

    print(f"Total Non-FX models: {len(non_fx_df)}")
    print("Sample Non-FX reasons:")
    print(non_fx_df['reason'].head(10).values)
except Exception as e:
    print(e)
