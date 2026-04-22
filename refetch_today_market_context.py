"""
Re-fetch and update today's market context data (for fixing Indian market data lag)
This script updates an existing row instead of inserting a new one.
"""
import yfinance as yf
import pandas as pd
import pyodbc
import logging
from datetime import datetime, timedelta

server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
target_table = "market_context_daily"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# All tickers
GLOBAL_TICKERS = {
    '^VIX': 'vix', '^INDIAVIX': 'india_vix', '^GSPC': 'sp500',
    '^IXIC': 'nasdaq_comp', '^NSEI': 'nifty50', 'DX-Y.NYB': 'dxy', '^TNX': 'us_10y_yield',
}
US_SECTOR_ETFS = {
    'XLK': 'xlk', 'XLF': 'xlf', 'XLE': 'xle', 'XLV': 'xlv', 'XLI': 'xli',
    'XLC': 'xlc', 'XLY': 'xly', 'XLP': 'xlp', 'XLB': 'xlb', 'XLRE': 'xlre', 'XLU': 'xlu',
}
INDIA_SECTOR_INDICES = {
    '^CNXIT': 'nifty_it', '^NSEBANK': 'nifty_bank', '^CNXPHARMA': 'nifty_pharma',
    '^CNXAUTO': 'nifty_auto', '^CNXFMCG': 'nifty_fmcg',
}
ALL_TICKERS = {**GLOBAL_TICKERS, **US_SECTOR_ETFS, **INDIA_SECTOR_INDICES}

def download_data(target_date):
    """Download data for target_date, fetching previous days for pct_change calculation."""
    start_date = target_date - timedelta(days=5)  # Get extra days for pct_change
    end_date = target_date + timedelta(days=1)
    
    ticker_str = ' '.join(ALL_TICKERS.keys())
    logger.info(f"Downloading for {target_date.date()} (fetching from {start_date.date()})")
    
    raw = yf.download(ticker_str, start=start_date, end=end_date, interval='1d',
                      group_by='ticker', auto_adjust=True, threads=True)
    
    if raw.empty:
        logger.error("No data returned")
        return None
    
    result = pd.DataFrame(index=raw.index)
    result.index.name = 'trading_date'
    
    for yf_ticker, col_prefix in ALL_TICKERS.items():
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if yf_ticker not in raw.columns.get_level_values(0):
                    continue
                ticker_data = raw[yf_ticker]
            else:
                ticker_data = raw
            
            if 'Close' not in ticker_data.columns:
                continue
            
            close = ticker_data['Close'].astype(float)
            
            if col_prefix in ('vix', 'india_vix'):
                result[f'{col_prefix}_close'] = close
                result[f'{col_prefix}_change_pct'] = close.pct_change(fill_method=None) * 100
            elif col_prefix == 'us_10y_yield':
                result[f'{col_prefix}_close'] = close
                result[f'{col_prefix}_change'] = close.diff()
            elif col_prefix in ('sp500', 'nasdaq_comp', 'nifty50', 'dxy'):
                result[f'{col_prefix}_close'] = close
                result[f'{col_prefix}_return_1d'] = close.pct_change(fill_method=None) * 100
            else:
                result[f'{col_prefix}_return_1d'] = close.pct_change(fill_method=None) * 100
        except Exception as e:
            logger.warning(f"{yf_ticker}: {e}")
    
    # Filter to just the target date
    if target_date in result.index:
        return result.loc[[target_date]]
    else:
        logger.warning(f"No data for {target_date.date()}")
        return None

def update_row(conn, target_date, df):
    """Update the existing row for target_date."""
    if df is None or df.empty:
        logger.error("No data to update")
        return
    
    cursor = conn.cursor()
    row = df.iloc[0]
    
    # Build UPDATE statement for non-NULL values
    updates = []
    values = []
    
    columns = [
        'vix_close', 'vix_change_pct', 'india_vix_close', 'india_vix_change_pct',
        'sp500_close', 'sp500_return_1d', 'nasdaq_comp_close', 'nasdaq_comp_return_1d',
        'nifty50_close', 'nifty50_return_1d', 'dxy_close', 'dxy_return_1d',
        'us_10y_yield_close', 'us_10y_yield_change',
        'xlk_return_1d', 'xlf_return_1d', 'xle_return_1d', 'xlv_return_1d', 'xli_return_1d',
        'xlc_return_1d', 'xly_return_1d', 'xlp_return_1d', 'xlb_return_1d', 'xlre_return_1d', 'xlu_return_1d',
        'nifty_it_return_1d', 'nifty_bank_return_1d', 'nifty_pharma_return_1d',
        'nifty_auto_return_1d', 'nifty_fmcg_return_1d',
    ]
    
    for col in columns:
        val = row.get(col, None)
        if val is not None and pd.notna(val):
            updates.append(f"{col} = ?")
            values.append(float(val))
    
    if not updates:
        logger.warning("No non-NULL values to update")
        return
    
    update_sql = f"UPDATE {target_table} SET {', '.join(updates)} WHERE trading_date = ?"
    values.append(target_date)
    
    logger.info(f"Updating {len(updates)} columns for {target_date.date()}")
    cursor.execute(update_sql, values)
    conn.commit()
    logger.info(f"✅ Updated {cursor.rowcount} row(s)")

def main():
    target_date = datetime(2026, 4, 21)  # Change this date as needed
    
    logger.info("=" * 60)
    logger.info(f"Re-fetching market context data for {target_date.date()}")
    logger.info("=" * 60)
    
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};"
        f"DATABASE={database};Trusted_Connection=yes;"
    )
    
    df = download_data(target_date)
    
    if df is not None:
        update_row(conn, target_date, df)
        
        # Show what we got
        logger.info("\nData retrieved:")
        for col in df.columns:
            val = df.iloc[0][col]
            if pd.notna(val):
                logger.info(f"  {col}: {val:.4f}")
    
    conn.close()

if __name__ == '__main__':
    main()
