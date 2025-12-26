# This py script runs daily using Windows Task Scheduler to get previous day data for Forex pairs
# Reads forex symbols from dbo.forex_master table and inserts into dbo.forex_hist_data
# Uses Alpha Vantage API for accurate forex data
import pandas as pd
import pyodbc
import requests
from datetime import datetime, timedelta
import time

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "forex_master"
target_table = "forex_hist_data"

# Alpha Vantage API Configuration
ALPHA_VANTAGE_KEY = "AG63AW94QZN86YBX"  # Your API key
API_WAIT_TIME = 15  # Seconds to wait between API calls (free tier: 5 calls/minute)

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
    print("🔑 Using Alpha Vantage API for data retrieval...")
except Exception as e:
    print("❌ Failed to fetch forex symbols:", e)
    exit()

# Function to fetch forex data from Alpha Vantage
def fetch_forex_latest(from_currency, to_currency, api_key, target_date):
    """
    Fetch latest forex data from Alpha Vantage API for a specific date
    
    Parameters:
    -----------
    from_currency : str
        Base currency (e.g., 'AUD', 'EUR')
    to_currency : str
        Quote currency (e.g., 'USD')
    api_key : str
        Alpha Vantage API key
    target_date : date
        Target trading date
    
    Returns:
    --------
    dict or None: Trading data for the target date
    """
    
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'FX_DAILY',
        'from_symbol': from_currency,
        'to_symbol': to_currency,
        'apikey': api_key,
        'outputsize': 'compact',  # Get last 100 days
        'datatype': 'json'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        # Check for errors
        if 'Error Message' in data:
            print(f"  ❌ API Error: {data['Error Message']}")
            return None
        
        if 'Note' in data:
            print(f"  ⚠️  API Limit: {data['Note']}")
            return None
        
        if 'Time Series FX (Daily)' not in data:
            print(f"  ❌ No data returned from Alpha Vantage")
            return None
        
        # Get time series data
        time_series = data['Time Series FX (Daily)']
        
        # Look for the target date
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        if target_date_str in time_series:
            values = time_series[target_date_str]
            return {
                'trading_date': target_date,
                'open_price': float(values['1. open']),
                'high_price': float(values['2. high']),
                'low_price': float(values['3. low']),
                'close_price': float(values['4. close']),
                'volume': 0
            }
        else:
            # If exact date not found, try to find the most recent date before target
            available_dates = sorted(time_series.keys(), reverse=True)
            for date_str in available_dates:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                if date_obj <= target_date:
                    values = time_series[date_str]
                    print(f"  ℹ️  Using closest available date: {date_str}")
                    return {
                        'trading_date': date_obj,
                        'open_price': float(values['1. open']),
                        'high_price': float(values['2. high']),
                        'low_price': float(values['3. low']),
                        'close_price': float(values['4. close']),
                        'volume': 0
                    }
            
            print(f"  ⚠️  No data found for {target_date_str} or earlier dates")
            return None
            
    except requests.exceptions.Timeout:
        print(f"  ❌ Request timeout")
        return None
    except Exception as e:
        print(f"  ❌ Error fetching data: {e}")
        return None

# Calculate previous trading day (skip weekends)
today = datetime.now()
if today.weekday() == 0:  # Monday
    prev_day = today - timedelta(days=3)  # Friday
elif today.weekday() == 6:  # Sunday
    prev_day = today - timedelta(days=2)  # Friday
else:
    prev_day = today - timedelta(days=1)

prev_day_str = prev_day.strftime('%Y-%m-%d')
print(f"📅 Fetching data for: {prev_day_str}")

# Process each forex pair
success_count = 0
error_count = 0

for idx, (symbol, currency_from, currency_to, yfinance_symbol) in enumerate(forex_symbols, 1):
    try:
        print(f"\n[{idx}/{len(forex_symbols)}] 🔄 Processing {symbol} ({currency_from}/{currency_to})...")
        
        # Fetch data from Alpha Vantage
        forex_data = fetch_forex_latest(currency_from, currency_to, ALPHA_VANTAGE_KEY, prev_day.date())
        
        if not forex_data:
            print(f"  ⚠️  No data found for {symbol}. Skipping...")
            error_count += 1
            continue
        
        # Extract data from Alpha Vantage response
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
        
        # Rate limiting - Alpha Vantage free tier allows 5 calls/minute
        if idx < len(forex_symbols):
            print(f"  ⏳ Waiting {API_WAIT_TIME} seconds (API rate limit)...")
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
print("📊 FOREX DATA UPDATE SUMMARY (Alpha Vantage API)")
print("="*60)
print(f"✅ Successfully processed: {success_count} records")
print(f"❌ Errors: {error_count} records")
print(f"📅 Date: {prev_day_str}")
print(f"⚠️  API Limits: 5 calls/minute, 100 calls/day (free tier)")
print("="*60)
