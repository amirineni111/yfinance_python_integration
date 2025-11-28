# this PY script to get top NSE 500 script names and no need to run unless we want to re-establish all scripts in master table
import yfinance as yf
import pandas as pd
import pyodbc
import requests
from io import StringIO

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"  # Change as per your setup
database = "stockdata_db"
table_name = "nse_500"

# Establish connection using Windows Authentication
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
    print("❌ Failed to connect to SQL Server:", e)
    exit()

# Create table if not exists
create_table_query = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
BEGIN
    CREATE TABLE {table_name} (
        ticker VARCHAR(50) PRIMARY KEY,
        company_name VARCHAR(255)
    );
END
"""
cursor.execute(create_table_query)
conn.commit()

# ✅ Fetch NSE-500 Stock Symbols using requests with headers
nse500_url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

try:
    response = requests.get(nse500_url, headers=headers)
    response.raise_for_status()  # Raise error if request fails

    # Read CSV from NSE response
    csv_data = StringIO(response.text)
    nse500_df = pd.read_csv(csv_data)
    nse500_tickers = nse500_df["Symbol"].tolist()
    
    print(f"✅ Successfully fetched {len(nse500_tickers)} NSE-500 tickers.")
except Exception as e:
    print("❌ Failed to fetch NSE-500 stock list:", e)
    exit()

# ✅ Insert NSE-500 stocks into SQL Server
for ticker in nse500_tickers:
    nse_ticker = f"{ticker}.NS"  # NSE stocks on Yahoo Finance have '.NS' suffix
    print(f"Fetching data for {nse_ticker}...")

    stock = yf.Ticker(nse_ticker)

    # Get the company name
    company_name = stock.info.get("longName", "N/A")

    # Insert data into SQL Server
    try:
        insert_query = f"""
        INSERT INTO {table_name} (ticker, company_name)
        VALUES (?, ?)
        """
        cursor.execute(insert_query, nse_ticker, company_name)
        conn.commit()
        print(f"✅ Inserted: {nse_ticker} - {company_name}")
    except Exception as e:
        print(f"⚠ Error inserting {nse_ticker}: {e}")

# Close the connection
cursor.close()
conn.close()
print("✅ Data fetching complete! NSE-500 stock list inserted into SQL Server.")
