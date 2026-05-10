# This py script is for adhoc historical data import for Forex pairs
# Reads forex symbols from dbo.forex_master where process_flag='Y' and fetches specified number of days
# Uses Polygon.io API for accurate forex data
import os
import pandas as pd
import pyodbc
import requests
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv

load_dotenv()

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "forex_master"
target_table = "forex_hist_data"

# Polygon.io API Configuration
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    print("❌ POLYGON_API_KEY not found in .env file. Exiting.")
    exit()

# Configuration - Change these as needed
DAYS_TO_FETCH = 365  # Number of days of historical data to fetch
BATCH_SIZE = 50      # Commit after every N records
API_WAIT_TIME = 1    # Polygon paid tier — no aggressive rate limiting needed

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

# Fetch forex symbols with process_flag = 'Y' from master table
try:
    cursor.execute(f"""
        SELECT symbol, currency_from, currency_to, yfinance_symbol 
        FROM {source_table} 
        WHERE process_flag = 'Y'
        ORDER BY symbol
    """)
    forex_symbols = cursor.fetchall()
    
    if not forex_symbols:
        print("❌ No forex symbols found with process_flag='Y' in master table.")
        print("💡 Please update forex_master table: UPDATE forex_master SET process_flag='Y' WHERE symbol='AUDUSD'")
        exit()
    
    print(f"📊 Found {len(forex_symbols)} forex pairs to process (process_flag='Y').")
    print(f"📅 Fetching last {DAYS_TO_FETCH} days of historical data using Polygon.io API...")
except Exception as e:
    print("❌ Failed to fetch forex symbols:", e)
    exit()

# Function to fetch forex data from Polygon.io
def fetch_forex_data_polygon(from_currency, to_currency, api_key, days_back=365):
    """
    Fetch forex data from Polygon.io API for a historical date range

    Parameters:
    -----------
    from_currency : str
        Base currency (e.g., 'AUD', 'EUR')
    to_currency : str
        Quote currency (e.g., 'USD')
    api_key : str
        Your Polygon.io API key
    days_back : int
        Number of days of historical data to fetch

    Returns:
    --------
    pd.DataFrame with columns: trading_date, open_price, high_price, low_price, close_price, volume
    """
    symbol = f"C:{from_currency}{to_currency}"
    from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    to_date = datetime.now().strftime('%Y-%m-%d')

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}"
    params = {
        'apiKey': api_key,
        'adjusted': 'true',
        'limit': 50000
    }

    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  ⚠️  Rate limited (429). Waiting {wait}s before retry {attempt + 1}/3...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'ERROR':
                print(f"  ❌ Polygon API Error: {data.get('error')}")
                return pd.DataFrame()

            if data.get('resultsCount', 0) == 0:
                print(f"  ❌ No data returned from Polygon for {from_currency}/{to_currency}")
                return pd.DataFrame()

            records = []
            for bar in data['results']:
                records.append({
                    'trading_date': datetime.utcfromtimestamp(bar['t'] / 1000).date(),
                    'open_price': float(bar['o']),
                    'high_price': float(bar['h']),
                    'low_price': float(bar['l']),
                    'close_price': float(bar['c']),
                    'volume': int(bar.get('v', 0))
                })

            df = pd.DataFrame(records)
            df = df.sort_values('trading_date').reset_index(drop=True)
            return df

        except requests.exceptions.Timeout:
            print(f"  ❌ Request timeout (attempt {attempt + 1}/3)")
        except Exception as e:
            print(f"  ❌ Error fetching data: {e}")
            return pd.DataFrame()

    print(f"  ❌ All retries exhausted for {from_currency}/{to_currency}")
    return pd.DataFrame()

# Process each forex pair
total_records = 0
success_count = 0
error_count = 0
update_count = 0
insert_count = 0

for idx, (symbol, currency_from, currency_to, yfinance_symbol) in enumerate(forex_symbols, 1):
    try:
        print(f"\n{'='*70}")
        print(f"[{idx}/{len(forex_symbols)}] 🔄 Processing {symbol} ({currency_from}/{currency_to})...")
        print(f"{'='*70}")
        
        # Fetch data from Polygon.io
        hist = fetch_forex_data_polygon(currency_from, currency_to, POLYGON_API_KEY, DAYS_TO_FETCH)
        
        if hist.empty:
            print(f"  ⚠️  No data found for {symbol}. Skipping...")
            error_count += 1
            continue
        
        print(f"  📈 Retrieved {len(hist)} trading days of data")
        print(f"  📅 Date range: {hist['trading_date'].min()} to {hist['trading_date'].max()}")
        
        # Process each day's data
        record_count = 0
        for _, row in hist.iterrows():
            try:
                trading_date = row['trading_date']
                open_price = row['open_price']
                high_price = row['high_price']
                low_price = row['low_price']
                close_price = row['close_price']
                volume = row['volume']
                
                # Calculate previous close and daily change
                previous_close = None
                daily_change = None
                daily_change_pct = None
                if record_count > 0:
                    # Get previous row for previous_close calculation
                    prev_idx = record_count - 1
                    if prev_idx >= 0:
                        previous_close = hist.iloc[prev_idx]['close_price'] if prev_idx < len(hist) else None
                        if close_price and previous_close:
                            daily_change = close_price - previous_close
                            daily_change_pct = (daily_change / previous_close) * 100
                
                # Check if record already exists
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {target_table} 
                    WHERE symbol = ? AND trading_date = ?
                """, (symbol, trading_date))
                
                exists = cursor.fetchone()[0]
                
                if exists > 0:
                    # Update existing record
                    update_query = f"""
                    UPDATE {target_table}
                    SET 
                        currency_from = ?,
                        currency_to = ?,
                        open_price = ?,
                        high_price = ?,
                        low_price = ?,
                        close_price = ?,
                        volume = ?,
                        daily_change = ?,
                        daily_change_pct = ?,
                        previous_close = ?,
                        exchange = 'CCY',
                        market_state = 'REGULAR',
                        created_date = GETDATE()
                    WHERE symbol = ? AND trading_date = ?
                    """
                    cursor.execute(update_query, (
                        currency_from, currency_to,
                        open_price, high_price, low_price, close_price, volume,
                        daily_change, daily_change_pct, previous_close,
                        symbol, trading_date
                    ))
                    update_count += 1
                else:
                    # Insert new record
                    insert_query = f"""
                    INSERT INTO {target_table} (
                        symbol, currency_from, currency_to, trading_date,
                        open_price, high_price, low_price, close_price, volume,
                        daily_change, daily_change_pct, previous_close,
                        exchange, market_state, created_date
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CCY', 'REGULAR', GETDATE())
                    """
                    cursor.execute(insert_query, (
                        symbol, currency_from, currency_to, trading_date,
                        open_price, high_price, low_price, close_price, volume,
                        daily_change, daily_change_pct, previous_close
                    ))
                    insert_count += 1
                
                record_count += 1
                
                # Commit in batches for better performance
                if record_count % BATCH_SIZE == 0:
                    conn.commit()
                    print(f"  💾 Committed {record_count}/{len(hist)} records...")
                
            except Exception as row_error:
                print(f"  ❌ Error processing record for {trading_date}: {str(row_error)}")
                continue
        
        # Final commit for this symbol
        conn.commit()
        total_records += record_count
        success_count += 1
        
        print(f"  ✅ Completed {symbol}: {record_count} records processed")
        print(f"    📊 Inserts: {insert_count} | Updates: {update_count}")
        
        # Reset flag after successful processing
        try:
            cursor.execute(f"""
                UPDATE {source_table}
                SET process_flag = 'N'
                WHERE symbol = ?
            """, (symbol,))
            conn.commit()
            print(f"    🚩 Reset process_flag to 'N' for {symbol}")
        except Exception as flag_error:
            print(f"    ⚠️  Could not reset flag: {str(flag_error)}")
        
        # Rate limiting - minimal wait for Polygon.io paid tier
        if idx < len(forex_symbols):
            print(f"  ⏳ Waiting {API_WAIT_TIME} second (rate limit)...")
            time.sleep(API_WAIT_TIME)
        
    except Exception as e:
        print(f"  ❌ Error processing {symbol}: {str(e)}")
        error_count += 1
        conn.rollback()
        continue

# Close connection
cursor.close()
conn.close()

# Final Summary
print("\n" + "="*70)
print("📊 FOREX HISTORICAL DATA IMPORT SUMMARY (Polygon.io)")
print("="*70)
print(f"✅ Symbols successfully processed: {success_count}/{len(forex_symbols)}")
print(f"❌ Symbols with errors: {error_count}")
print(f"📝 Total records processed: {total_records}")
print(f"  ➕ New records inserted: {insert_count}")
print(f"  🔄 Existing records updated: {update_count}")
print(f"📅 Historical period: Last {DAYS_TO_FETCH} days")
print(f"🔑 API Source: Polygon.io")
print("="*70)
print("\n💡 TIPS:")
print("   • Set process_flag='Y' in forex_master for symbols you want to process")
print("     Example: UPDATE forex_master SET process_flag='Y' WHERE symbol IN ('EURUSD', 'GBPUSD')")
print("   • Polygon.io fetches the full date range in a single API call per pair")
print("   • Data is reliable with paid Polygon.io membership\n")
