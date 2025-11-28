# This py script run daily using windows task schedular to get previous day data for NSE scripts
import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "nse_500"
target_table = "nse_500_hist_data"

# Connect to SQL Server
try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    print("✅ Connected to SQL Server successfully.")
except Exception as e:
    print("❌ Connection failed:", e)
    exit()

# Create the target table if not exists
create_table_query = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{target_table}')
BEGIN
    CREATE TABLE {target_table} (
        trading_date DATE,
        open_price VARCHAR(50),
        high_price VARCHAR(50),
        low_price VARCHAR(50),
        close_price VARCHAR(50),
        volume VARCHAR(50),
        dividend VARCHAR(50),
        stocksplit VARCHAR(50),
        ticker VARCHAR(50),
        company VARCHAR(255)
    );
END
"""
cursor.execute(create_table_query)
conn.commit()

# Fetch NSE-500 tickers from source table
cursor.execute(f"SELECT ticker, company_name FROM {source_table}")
nse500_tickers = cursor.fetchall()

if not nse500_tickers:
    print("❌ No tickers found in source table.")
    exit()

# Loop through each ticker
for ticker, company_name in nse500_tickers:
    # Get the latest date already available for the ticker
    cursor.execute(f"SELECT MAX(trading_date) FROM {target_table} WHERE ticker = ?", ticker)
    max_date = cursor.fetchone()[0]

    # If no data exists, fetch for past year
    if max_date:
        start_date = max_date + timedelta(days=1)
    else:
        start_date = datetime.today() - timedelta(days=365)

    end_date = datetime.today() + timedelta(days=1)

    print(f"📈 Fetching data for {ticker} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    stock = yf.Ticker(ticker)
    data = stock.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d")

    if data.empty:
        print(f"⚠ No data found for {ticker}. Skipping...")
        continue

    data = data.reset_index().rename(columns={"Date": "trading_date"})
    data["Ticker"] = ticker
    data["Company"] = company_name

    for _, row in data.iterrows():
        insert_query = f"""
        INSERT INTO {target_table} (
            trading_date, open_price, high_price, low_price, close_price,
            volume, dividend, stocksplit, ticker, company
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query,
                       row['trading_date'], row['Open'], row['High'], row['Low'], row['Close'],
                       row['Volume'], row['Dividends'], row['Stock Splits'], row['Ticker'], row['Company'])

    conn.commit()
    print(f"✅ Inserted data for {ticker}")

# Cleanup
cursor.close()
conn.close()
print("✅ NSE-500 historical data sync complete!")
