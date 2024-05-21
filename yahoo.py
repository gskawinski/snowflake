import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# List of assets
assets = ["^GSPC", "^GDAXI", "DX-Y.NYB", "GC=F"]  # Tickers for S&P 500, DAX, US Dollar Index, Gold

# Function to perform initial batch loading
def initial_batch_load():
    for asset in assets:
        data = yf.download(asset, start="2000-01-01", end="2023-08-01", interval="1d")
        if not data.empty:
            csv_filename = f"{asset}.csv"
            data.to_csv(csv_filename)
            print(f"Saved {asset} initial data to {csv_filename}")

# Function to perform delta load
def delta_load(asset):
    csv_filename = f"{asset}.csv"
    
    if os.path.exists(csv_filename):
        existing_data = pd.read_csv(csv_filename, index_col=0, parse_dates=True)
        last_date = existing_data.index[-1]

        # Calculate start and end dates for delta load (last date + 1 day to today)
        start_date = last_date + timedelta(days=1)
        end_date = datetime.now().date()

        if start_date <= end_date:
            new_data = yf.download(asset, start=start_date, end=end_date, interval="1d")
            if not new_data.empty:
                new_data = new_data[new_data.index > last_date]
                updated_data = pd.concat([existing_data, new_data])
                updated_data.to_csv(csv_filename)
                print(f"Updated {asset} data with delta load.")

# Perform initial batch loading
initial_batch_load()

# Perform delta load for each asset
for asset in assets:
    delta_load(asset)
