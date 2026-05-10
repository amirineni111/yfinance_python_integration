# This py script runs daily using Windows Task Scheduler to get previous day data for Forex pairs
# Reads forex symbols from dbo.forex_master table and inserts into dbo.forex_hist_data
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

# Polygon.io API Configuration (replaces Alpha Vantage)
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    print("❌ POLYGON_API_KEY not found in .env file. Exiting.")
    exit()
API_WAIT_TIME = 12   # 5 calls/min limit on Polygon — 12s gap keeps safely under

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

# Fetch active forex symbols from master table
try:
    cursor.execute(f"""
        SELECT symbol, currency_from, currency_to, yfinance_symbol 
        FROM {source_table} 
        WHERE is_active = 'Y'
        ORDER BY symbol
    """)
    forex_symbols = cursor.fetchall()
    
    if not forex_symbols:
        print("❌ No active forex symbols found in master table.")
        exit()
    
    print(f"📊 Found {len(forex_symbols)} active forex pairs to process.")
    print("🔑 Using Polygon.io API for data retrieval...")
except Exception as e:
    print("❌ Failed to fetch forex symbols:", e)
    exit()

# Function to fetch forex data from Polygon.io
def fetch_forex_latest(from_currency, to_currency, api_key, target_date):
    """
    Fetch forex data from Polygon.io for a specific date

    Parameters:
    -----------
    from_currency : str
        Base currency (e.g., 'AUD', 'EUR')
    to_currency : str
        Quote currency (e.g., 'USD')
    api_key : str
        Polygon.io API key
    target_date : date
        Target trading date

    Returns:
    --------
    dict or None: Trading data for the target date
    """
    symbol = f"C:{from_currency}{to_currency}"
    date_str = target_date.strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{date_str}/{date_str}"
    params = {
        'apiKey': api_key,
        'adjusted': 'true'
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
                return None

            if data.get('resultsCount', 0) == 0:
                print(f"  ⚠️  No data returned from Polygon for {date_str}")
                return None

            result = data['results'][0]
            return {
                'trading_date': target_date,
                'open_price': float(result['o']),
                'high_price': float(result['h']),
                'low_price': float(result['l']),
                'close_price': float(result['c']),
                'volume': int(result.get('v', 0))
            }

        except requests.exceptions.Timeout:
            print(f"  ❌ Request timeout (attempt {attempt + 1}/3)")
        except Exception as e:
            print(f"  ❌ Error fetching data: {e}")
            return None

    print(f"  ❌ All retries exhausted for {from_currency}/{to_currency}")
    return None

# Calculate target trading day: try today first, fallback to previous trading day
today = datetime.now()

# Primary target is today (forex daily bar closes at 5 PM EST)
target_day = today

# If weekend, adjust to Friday
if today.weekday() == 5:  # Saturday
    target_day = today - timedelta(days=1)  # Friday
elif today.weekday() == 6:  # Sunday
    target_day = today - timedelta(days=2)  # Friday

# Fallback: previous trading day (in case API hasn't updated yet)
if today.weekday() == 0:  # Monday
    fallback_day = today - timedelta(days=3)  # Friday
elif today.weekday() == 6:  # Sunday
    fallback_day = today - timedelta(days=2)  # Friday
elif today.weekday() == 5:  # Saturday
    fallback_day = today - timedelta(days=1)  # Friday
else:
    fallback_day = today - timedelta(days=1)

target_day_str = target_day.strftime('%Y-%m-%d')
fallback_day_str = fallback_day.strftime('%Y-%m-%d')
print(f"📅 Primary target date: {target_day_str}")
print(f"📅 Fallback date: {fallback_day_str}")

# Process each forex pair
success_count = 0
error_count = 0

for idx, (symbol, currency_from, currency_to, yfinance_symbol) in enumerate(forex_symbols, 1):
    try:
        print(f"\n[{idx}/{len(forex_symbols)}] 🔄 Processing {symbol} ({currency_from}/{currency_to})...")
        
        # Fetch data from Polygon.io — try today first, fallback to previous day
        forex_data = fetch_forex_latest(currency_from, currency_to, POLYGON_API_KEY, target_day.date())

        if not forex_data and target_day.date() != fallback_day.date():
            print(f"  ℹ️  Today's data not available yet, trying fallback date {fallback_day_str}...")
            forex_data = fetch_forex_latest(currency_from, currency_to, POLYGON_API_KEY, fallback_day.date())
        
        if not forex_data:
            print(f"  ⚠️  No data found for {symbol}. Skipping...")
            error_count += 1
            continue
        
        # Extract data from Polygon.io response
        trading_date = forex_data['trading_date']
        open_price = forex_data['open_price']
        high_price = forex_data['high_price']
        low_price = forex_data['low_price']
        close_price = forex_data['close_price']
        volume = forex_data['volume']
        
        # Calculate daily change (simplified - no previous_close from API)
        previous_close = None
        daily_change = None
        daily_change_pct = None
        
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
            print(f"  ✅ Updated {symbol} for {trading_date}")
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
            print(f"  ✅ Inserted {symbol} for {trading_date}")
        
        conn.commit()
        success_count += 1
        
        # Rate limiting - minimal wait for Polygon.io paid tier
        if idx < len(forex_symbols):
            print(f"  ⏳ Waiting {API_WAIT_TIME} second (rate limit)...")
            time.sleep(API_WAIT_TIME)
            
    except Exception as e:
        print(f"❌ Error processing {symbol}: {str(e)}")
        error_count += 1
        conn.rollback()
        continue

# Close connection
cursor.close()
conn.close()

# Summary
print("\n" + "="*60)
print("📊 FOREX DATA UPDATE SUMMARY (Polygon.io API)")
print("="*60)
print(f"✅ Successfully processed: {success_count} records")
print(f"❌ Errors: {error_count} records")
print(f"📅 Target date: {target_day_str} | Fallback: {fallback_day_str}")
print("="*60)
