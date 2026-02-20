"""
Market Context Daily ETL Script
================================
Fetches daily market regime, volatility, index returns, sector ETF data,
and treasury yields from yfinance → market_context_daily table in SQL Server.

This data is consumed by all 3 ML pipelines (NASDAQ, NSE, Forex) as additional
features for improved prediction accuracy.

Usage:
    python get_market_context_daily.py              # Incremental (fetch missing dates)
    python get_market_context_daily.py --backfill   # Full 2-year historical load

Schedule: Run daily BEFORE ML pipelines (added as step 0 in run_all_data_fetch.bat)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import pyodbc
import argparse
from datetime import datetime, timedelta

# ============================================================
# Configuration
# ============================================================

server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
target_table = "market_context_daily"

# Tickers to download (grouped by category)
GLOBAL_TICKERS = {
    # Volatility indices
    '^VIX': 'vix',
    '^INDIAVIX': 'india_vix',
    # Major indices
    '^GSPC': 'sp500',
    '^IXIC': 'nasdaq_comp',
    '^NSEI': 'nifty50',
    # Currency / Rates
    'DX-Y.NYB': 'dxy',
    '^TNX': 'us_10y_yield',
}

US_SECTOR_ETFS = {
    'XLK': 'xlk',   # Technology
    'XLF': 'xlf',   # Financials
    'XLE': 'xle',   # Energy
    'XLV': 'xlv',   # Healthcare
    'XLI': 'xli',   # Industrials
    'XLC': 'xlc',   # Communication Services
    'XLY': 'xly',   # Consumer Discretionary
    'XLP': 'xlp',   # Consumer Staples
    'XLB': 'xlb',   # Basic Materials
    'XLRE': 'xlre',  # Real Estate
    'XLU': 'xlu',   # Utilities
}

INDIA_SECTOR_INDICES = {
    '^CNXIT': 'nifty_it',
    '^NSEBANK': 'nifty_bank',
    '^CNXPHARMA': 'nifty_pharma',
    '^CNXAUTO': 'nifty_auto',
    '^CNXFMCG': 'nifty_fmcg',
}

# All tickers combined
ALL_TICKERS = {**GLOBAL_TICKERS, **US_SECTOR_ETFS, **INDIA_SECTOR_INDICES}

# Backfill period
BACKFILL_DAYS = 730  # ~2 years

# ============================================================
# Table DDL
# ============================================================

CREATE_TABLE_SQL = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{target_table}')
BEGIN
    CREATE TABLE {target_table} (
        trading_date DATE PRIMARY KEY,
        -- Volatility indices
        vix_close FLOAT NULL,
        vix_change_pct FLOAT NULL,
        india_vix_close FLOAT NULL,
        india_vix_change_pct FLOAT NULL,
        -- Major indices
        sp500_close FLOAT NULL,
        sp500_return_1d FLOAT NULL,
        nasdaq_comp_close FLOAT NULL,
        nasdaq_comp_return_1d FLOAT NULL,
        nifty50_close FLOAT NULL,
        nifty50_return_1d FLOAT NULL,
        -- Currency / Rates
        dxy_close FLOAT NULL,
        dxy_return_1d FLOAT NULL,
        us_10y_yield_close FLOAT NULL,
        us_10y_yield_change FLOAT NULL,
        -- US Sector ETF returns (1-day)
        xlk_return_1d FLOAT NULL,
        xlf_return_1d FLOAT NULL,
        xle_return_1d FLOAT NULL,
        xlv_return_1d FLOAT NULL,
        xli_return_1d FLOAT NULL,
        xlc_return_1d FLOAT NULL,
        xly_return_1d FLOAT NULL,
        xlp_return_1d FLOAT NULL,
        xlb_return_1d FLOAT NULL,
        xlre_return_1d FLOAT NULL,
        xlu_return_1d FLOAT NULL,
        -- India Sector Index returns (1-day)
        nifty_it_return_1d FLOAT NULL,
        nifty_bank_return_1d FLOAT NULL,
        nifty_pharma_return_1d FLOAT NULL,
        nifty_auto_return_1d FLOAT NULL,
        nifty_fmcg_return_1d FLOAT NULL,
        -- Metadata
        data_fetched_at DATETIME DEFAULT GETDATE()
    );
    
    CREATE INDEX IX_{target_table}_date ON {target_table} (trading_date);
END
"""

# ============================================================
# Functions
# ============================================================

def connect_db():
    """Connect to SQL Server with Windows auth."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        print("✅ Connected to SQL Server.")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to SQL Server: {e}")
        exit(1)


def ensure_table(conn):
    """Create market_context_daily table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()
    print(f"✅ Table '{target_table}' ready.")


def get_last_date(conn):
    """Get the most recent trading_date in the table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(trading_date) FROM {target_table}")
    result = cursor.fetchone()[0]
    return result


def download_data(start_date, end_date):
    """Batch-download all tickers from yfinance."""
    all_yf_tickers = list(ALL_TICKERS.keys())
    ticker_str = ' '.join(all_yf_tickers)

    print(f"\n📊 Downloading {len(all_yf_tickers)} tickers from {start_date} to {end_date}...")
    print(f"   Tickers: {ticker_str}")

    try:
        raw = yf.download(
            ticker_str,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            interval='1d',
            group_by='ticker',
            auto_adjust=True,
            threads=True,
        )
    except Exception as e:
        print(f"❌ yfinance download failed: {e}")
        return pd.DataFrame()

    if raw.empty:
        print("⚠️  No data returned from yfinance.")
        return pd.DataFrame()

    # Build a clean DataFrame with one row per trading date
    result = pd.DataFrame(index=raw.index)
    result.index.name = 'trading_date'

    for yf_ticker, col_prefix in ALL_TICKERS.items():
        try:
            # Handle multi-ticker download column structure
            if isinstance(raw.columns, pd.MultiIndex):
                if yf_ticker in raw.columns.get_level_values(0):
                    ticker_data = raw[yf_ticker]
                else:
                    print(f"  ⚠️  {yf_ticker} ({col_prefix}): not in download results, skipping.")
                    continue
            else:
                # Single ticker case (shouldn't happen with batch, but handle it)
                ticker_data = raw

            close_col = 'Close'
            if close_col not in ticker_data.columns:
                print(f"  ⚠️  {yf_ticker} ({col_prefix}): no 'Close' column, skipping.")
                continue

            close = ticker_data[close_col].astype(float)

            # For volatility indices and yield: store close + daily change
            if col_prefix in ('vix', 'india_vix'):
                result[f'{col_prefix}_close'] = close
                result[f'{col_prefix}_change_pct'] = close.pct_change() * 100

            elif col_prefix == 'us_10y_yield':
                result[f'{col_prefix}_close'] = close
                result[f'{col_prefix}_change'] = close.diff()

            elif col_prefix in ('sp500', 'nasdaq_comp', 'nifty50', 'dxy'):
                result[f'{col_prefix}_close'] = close
                result[f'{col_prefix}_return_1d'] = close.pct_change() * 100

            else:
                # Sector ETFs and India sector indices — just 1-day return
                result[f'{col_prefix}_return_1d'] = close.pct_change() * 100

        except Exception as e:
            print(f"  ⚠️  {yf_ticker} ({col_prefix}): Error processing — {e}")

    # Drop rows where ALL data columns are NaN
    data_cols = [c for c in result.columns if c != 'trading_date']
    result = result.dropna(how='all', subset=data_cols if data_cols else None)

    print(f"✅ Downloaded {len(result)} trading days of market context data.")
    return result


def insert_data(conn, df):
    """Insert rows into market_context_daily, skipping existing dates."""
    if df.empty:
        print("⚠️  No data to insert.")
        return

    cursor = conn.cursor()

    # Get existing dates to avoid duplicates
    cursor.execute(f"SELECT trading_date FROM {target_table}")
    existing_dates = set(row[0] for row in cursor.fetchall())

    # Column order must match INSERT statement
    db_columns = [
        'trading_date',
        # Volatility
        'vix_close', 'vix_change_pct',
        'india_vix_close', 'india_vix_change_pct',
        # Indices
        'sp500_close', 'sp500_return_1d',
        'nasdaq_comp_close', 'nasdaq_comp_return_1d',
        'nifty50_close', 'nifty50_return_1d',
        # Currency / Rates
        'dxy_close', 'dxy_return_1d',
        'us_10y_yield_close', 'us_10y_yield_change',
        # US Sector ETFs
        'xlk_return_1d', 'xlf_return_1d', 'xle_return_1d',
        'xlv_return_1d', 'xli_return_1d', 'xlc_return_1d',
        'xly_return_1d', 'xlp_return_1d', 'xlb_return_1d',
        'xlre_return_1d', 'xlu_return_1d',
        # India Sector Indices
        'nifty_it_return_1d', 'nifty_bank_return_1d',
        'nifty_pharma_return_1d', 'nifty_auto_return_1d',
        'nifty_fmcg_return_1d',
    ]

    placeholders = ', '.join(['?' for _ in db_columns])
    col_names = ', '.join(db_columns)
    insert_sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})"

    inserted = 0
    skipped = 0

    for idx, row in df.iterrows():
        trading_date = idx.date() if hasattr(idx, 'date') else idx

        if trading_date in existing_dates:
            skipped += 1
            continue

        values = [trading_date]
        for col in db_columns[1:]:  # Skip trading_date
            val = row.get(col, None)
            if val is not None and pd.notna(val):
                values.append(float(val))
            else:
                values.append(None)

        try:
            cursor.execute(insert_sql, values)
            inserted += 1
        except Exception as e:
            print(f"  ⚠️  Error inserting {trading_date}: {e}")

    conn.commit()
    print(f"✅ Inserted {inserted} rows, skipped {skipped} existing dates.")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Fetch daily market context data')
    parser.add_argument('--backfill', action='store_true',
                        help=f'Backfill {BACKFILL_DAYS} days of historical data')
    args = parser.parse_args()

    print("=" * 60)
    print("Market Context Daily ETL")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = connect_db()
    ensure_table(conn)

    if args.backfill:
        start_date = datetime.today() - timedelta(days=BACKFILL_DAYS)
        print(f"\n🔄 Backfill mode: loading {BACKFILL_DAYS} days from {start_date.date()}")
    else:
        last_date = get_last_date(conn)
        if last_date:
            start_date = last_date + timedelta(days=1)
            print(f"\n📅 Incremental mode: last date in DB = {last_date}, fetching from {start_date.date()}")
        else:
            start_date = datetime.today() - timedelta(days=BACKFILL_DAYS)
            print(f"\n📅 First run: no data found, backfilling {BACKFILL_DAYS} days")

    end_date = datetime.today() + timedelta(days=1)

    if start_date.date() >= end_date.date():
        print("✅ Data is already up to date. Nothing to fetch.")
        conn.close()
        return

    df = download_data(start_date, end_date)
    insert_data(conn, df)

    # Summary
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*), MIN(trading_date), MAX(trading_date) FROM {target_table}")
    count, min_date, max_date = cursor.fetchone()
    print(f"\n📊 Table summary: {count} rows, {min_date} to {max_date}")

    cursor.close()
    conn.close()
    print("✅ Market context ETL complete!")


if __name__ == '__main__':
    main()
