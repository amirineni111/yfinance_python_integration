# this PY script used to get list of top 100 NASDAQ script names and no need to run unless we want to update the top 100 list of nasdaq
import yfinance as yf
import pandas as pd
import pyodbc

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"  # Change as per your setup
database = "stockdata_db"
table_name = "nasdaq_top100"

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

# ✅ Fetch Top 100 NASDAQ-100 Tickers from yfinance
nasdaq100_tickers = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "TSM", "COST", 
    "NFLX", "ADBE", "AMD", "PEP", "CSCO", "QCOM", "TXN", "INTC", "AMAT", "PDD",
    "LRCX", "BKNG", "ISRG", "VRTX", "INTU", "ADP", "ASML", "GILD", "REGN", "PYPL",
    "SBUX", "MDLZ", "PANW", "TMUS", "CHTR", "MU", "CSX", "MRNA", "SNPS", "KLAC",
    "MELI", "ORLY", "DXCM", "ADSK", "IDXX", "CDNS", "KDP", "MNST", "CTAS", "FTNT",
    "MAR", "EXC", "ROP", "PCAR", "PAYX", "WDAY", "ANSS", "XEL", "AZN", "FAST",
    "BIIB", "AEP", "MSI", "ODFL", "CPRT", "GEHC", "TTWO", "CEG", "VRSK", "SIRI",
    "FTV", "CTSH", "CHKP", "CDW", "NXPI", "MCHP", "DLTR", "EBAY", "CSGP", "WBD",
    "TSCO", "PDD", "ROST", "NTES", "WBA", "KHC", "VTRS", "PTON", "FOX", "FOXA",
    "JD", "LULU", "BIDU", "EXPE", "ILMN", "DOCU", "SPLK", "ZS", "OKTA", "DDOG"
]  # Modify if needed

# ✅ Insert NASDAQ-100 stocks into SQL Server
for ticker in nasdaq100_tickers:
    print(f"Fetching data for {ticker}...")
    stock = yf.Ticker(ticker)

    # Get the company name
    company_name = stock.info.get("longName", "N/A")

    # Insert data into SQL Server
    try:
        insert_query = f"""
        INSERT INTO {table_name} (ticker, company_name)
        VALUES (?, ?)
        """
        cursor.execute(insert_query, ticker, company_name)
        conn.commit()
        print(f"✅ Inserted: {ticker} - {company_name}")
    except Exception as e:
        print(f"⚠ Error inserting {ticker}: {e}")

# Close the connection
cursor.close()
conn.close()
print("✅ Data fetching complete! NASDAQ-100 stock list inserted into SQL Server.")
