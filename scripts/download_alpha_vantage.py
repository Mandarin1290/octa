#!/usr/bin/env python3
"""
Download Intraday AAPL data from Alpha Vantage API.
Requires API key from https://www.alphavantage.co/support/#api-key
Usage: python scripts/download_alpha_vantage.py --api-key YOUR_KEY --symbol AAPL --interval 1min --output raw/AAPL_AV_1M.parquet
"""

import argparse
from pathlib import Path

import pandas as pd
import requests


def download_alpha_vantage_intraday(api_key, symbol, interval='1min', output='raw/AAPL_AV_1M.parquet'):
    """
    Download up to 20 years of intraday data from Alpha Vantage.
    Note: Free tier limited to 5 calls/min, 500 calls/day.
    """
    base_url = "https://www.alphavantage.co/query"
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize': 'compact',  # compact = last 100 data points (free)
        'datatype': 'json'
    }

    print(f"Downloading {symbol} {interval} data from Alpha Vantage...")
    response = requests.get(base_url, params=params)
    data = response.json()

    if 'Time Series (Daily)' not in data:
        print("Error:", data.get('Error Message', 'Unknown error'))
        print("Full response:", data)
        return None

    # Parse JSON to DataFrame
    time_series = data['Time Series (Daily)']
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.index = pd.to_datetime(df.index)
    df = df.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }).astype(float)
    df = df.sort_index()

    # Save to parquet
    Path(output).parent.mkdir(exist_ok=True)
    df.to_parquet(output)
    print(f"Saved {len(df)} rows to {output}")
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Alpha Vantage Intraday Data")
    parser.add_argument('--api-key', required=True, help='Your Alpha Vantage API key')
    parser.add_argument('--symbol', default='AAPL', help='Stock symbol')
    parser.add_argument('--interval', default='1min', choices=['1min', '5min', '15min', '30min', '60min'], help='Interval')
    parser.add_argument('--output', default='raw/AAPL_AV_1M.parquet', help='Output file')
    args = parser.parse_args()

    download_alpha_vantage_intraday(args.api_key, args.symbol, args.interval, args.output)
