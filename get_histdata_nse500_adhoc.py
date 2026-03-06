# this py script meant to import last 1000 days of data for NSE scripts 
import yfinance as yf
import pandas as pd
import pyodbc

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"  # Change as per your setup
database = "stockdata_db"
source_table = "nse_500"  # Table with tickers
target_table = "nse_500_hist_data"  # Table to store stock history

# ✅ Establish connection to SQL Server
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

# ✅ Create historical data table if it doesn't exist
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

# ✅ Fetch NSE-500 tickers from SQL Server
cursor.execute(f"SELECT ticker, company_name FROM {source_table} where process_flag='y'")
nse500_tickers = cursor.fetchall()

if not nse500_tickers:
    print("❌ No tickers found in the database. Please check your NSE-500 table.")
    exit()

# ✅ Loop through each ticker and fetch last 1000 days of data
for ticker, company_name in nse500_tickers:
    print(f"Fetching data for {ticker} (last 1000 days)...")
    
    stock = yf.Ticker(ticker)

    # Get stock history (last 1000 days, interval: 1 day)
    data = stock.history(period="1000d", interval="1d")
    
    if data.empty:
        print(f"⚠ No data found for {ticker}. Skipping...")
        continue

    # Reset index and rename Date column
    data = data.reset_index().rename(columns={"Date": "trading_date"})

    # Add Ticker and Company Name columns
    data["Ticker"] = ticker
    data["Company"] = company_name

    # ✅ Insert data into SQL Server (with duplicate check)
    inserted = 0
    skipped = 0
    for _, row in data.iterrows():
        trade_date = row['trading_date']
        # Strip timezone if present (yfinance returns tz-aware dates)
        if hasattr(trade_date, 'tz') and trade_date.tz is not None:
            trade_date = trade_date.tz_localize(None)

        # Check if record already exists for this ticker + date
        cursor.execute(
            f"SELECT COUNT(*) FROM {target_table} WHERE ticker = ? AND trading_date = ?",
            ticker, trade_date
        )
        if cursor.fetchone()[0] > 0:
            skipped += 1
            continue

        insert_query = f"""
        INSERT INTO {target_table} (trading_date, open_price, high_price, low_price, close_price, volume, dividend, stocksplit, ticker, company)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # Convert values to string and handle NaN/None values
        open_val = str(row['Open']) if pd.notna(row['Open']) else None
        high_val = str(row['High']) if pd.notna(row['High']) else None
        low_val = str(row['Low']) if pd.notna(row['Low']) else None
        close_val = str(row['Close']) if pd.notna(row['Close']) else None
        volume_val = str(row['Volume']) if pd.notna(row['Volume']) else None
        div_val = str(row['Dividends']) if pd.notna(row['Dividends']) else None
        split_val = str(row['Stock Splits']) if pd.notna(row['Stock Splits']) else None
        
        cursor.execute(insert_query, trade_date, open_val, high_val, low_val, close_val,
                       volume_val, div_val, split_val, row['Ticker'], row['Company'])
        inserted += 1

    conn.commit()
    if skipped > 0:
        print(f"⚠ {ticker}: {inserted} inserted, {skipped} skipped (already exist)")
    else:
        print(f"✅ Data for {ticker} inserted successfully ({inserted} rows).")

# ✅ Close the connection
cursor.close()
conn.close()
print("✅ Data fetching complete! NSE-500 historical data inserted into SQL Server.")
