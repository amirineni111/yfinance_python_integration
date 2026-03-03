# This py script run daily using windows task schedular to get previous day data for NSE scripts
import yfinance as yf
import pandas as pd
import pyodbc
import logging
import os
import sys
from datetime import datetime, timedelta

# --- Logging setup ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "nse_stock_fetch.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "nse_500"
target_table = "nse_500_hist_data"

logger.info("=" * 50)
logger.info("NSE Stock Fetch started at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
logger.info("Python: %s", sys.executable)
logger.info("=" * 50)

# Connect to SQL Server
try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    logger.info("Connected to SQL Server successfully.")
except Exception as e:
    logger.error("Connection failed: %s", e, exc_info=True)
    sys.exit(1)

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
    logger.error("No tickers found in source table.")
    sys.exit(1)

logger.info("Found %d tickers to process.", len(nse500_tickers))

success_count = 0
skip_count = 0
error_count = 0

# Loop through each ticker
for ticker, company_name in nse500_tickers:
    try:
        # Get the latest date already available for the ticker
        cursor.execute(f"SELECT MAX(trading_date) FROM {target_table} WHERE ticker = ?", ticker)
        max_date = cursor.fetchone()[0]

        # If no data exists, fetch for past year
        if max_date:
            start_date = max_date + timedelta(days=1)
        else:
            start_date = datetime.today() - timedelta(days=365)

        end_date = datetime.today() + timedelta(days=1)

        logger.info("Fetching data for %s from %s to %s", ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        stock = yf.Ticker(ticker)
        data = stock.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d")

        if data.empty:
            logger.warning("No data found for %s. Skipping...", ticker)
            skip_count += 1
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
        logger.info("Inserted data for %s (%d rows)", ticker, len(data))
        success_count += 1

    except Exception as e:
        logger.error("Failed to process %s: %s", ticker, e, exc_info=True)
        error_count += 1
        continue

# Cleanup
cursor.close()
conn.close()
logger.info("=" * 50)
logger.info("NSE-500 fetch complete. Success: %d | Skipped: %d | Errors: %d", success_count, skip_count, error_count)
logger.info("=" * 50)
