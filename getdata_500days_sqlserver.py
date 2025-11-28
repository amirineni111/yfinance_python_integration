import yfinance as yf
import pandas as pd
import pyodbc

server = "localhost\\MSSQLSERVER01"  # Use double backslashes
database = "stockdata_db"
table_name = "stock_hist_data"

# Establish connection using Windows Authentication
try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
except Exception as e:
    print("❌ Failed to connect to SQL Server:", e)
    exit()

# Create table if not exists
create_table_query = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
BEGIN
    CREATE TABLE {table_name} (
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

# Example stock, modify as needed
tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
           "TSM", "COST", "NFLX", "ADBE", "AMD", "PEP", "CSCO", "QCOM",
           "TXN", "INTC", "AMAT", "PDD", "LEMONTREE.NS", "VBL.NS", "HDFCLIFE.NS", "NYKAA.NS"]


# Loop through each ticker and fetch stock data
for ticker in tickers:
    print(f"Fetching data for {ticker} (last 500 days)...")
    stock = yf.Ticker(ticker)
    
    # Get stock history (last 500 days, interval: 1 day)
    data = stock.history(period="500d", interval="1d")
    
    if data.empty:
        print(f"⚠ No data found for {ticker}. Skipping...")
        continue

    # Reset index, rename columns to match SQL
    data = data.reset_index().rename(columns={"Date": "trading_date"})
    
    # Print column names for debugging
    print("Columns in DataFrame:", data.columns)

    # Fetch the actual company name
    company_name = stock.info.get("longName", "N/A")

    # Add Ticker and Company Name columns
    data["Ticker"] = ticker
    data["Company"] = company_name

    # Insert data into SQL Server
    for _, row in data.iterrows():
        insert_query = f"""
        INSERT INTO {table_name} (trading_date, open_price, high_price, low_price, close_price, volume, dividend, stocksplit, ticker, company)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, row['trading_date'], row['Open'], row['High'], row['Low'], row['Close'],
                       row['Volume'], row['Dividends'], row['Stock Splits'], row['Ticker'], row['Company'])

    conn.commit()
    print(f"✅ Data for {ticker} inserted successfully.")

# Close the connection
cursor.close()
conn.close()
print("✅ Data fetching complete! All stock data inserted into SQL Server.")
