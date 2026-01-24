import pandas as pd
import yfinance as yf

# Download 1-minute data for AAPL for the last 7 days (max for 1m)
data = yf.download('AAPL', period='7d', interval='1m')

# Rename columns to match expected format
data.columns = data.columns.droplevel(1)  # Remove Ticker level
data = data.rename(columns={
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Adj Close': 'adj_close',
    'Volume': 'volume'
})

# Ensure datetime index
data.index = pd.to_datetime(data.index)
data = data.reset_index()  # Convert index to column
data = data.rename(columns={'Datetime': 'timestamp'})

# Save as parquet
data.to_parquet('/home/n-b/Octa/raw/AAPL_1m.parquet')

print("Downloaded and saved AAPL 1m data.")
