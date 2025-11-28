# This py script runs daily using Windows Task Scheduler to get previous day data for Forex pairs
# Reads forex symbols from dbo.forex_master table and inserts into dbo.forex_hist_data
import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "forex_master"
target_table = "forex_hist_data"

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
        WHERE is_active = 1
        ORDER BY symbol
    """)
    forex_symbols = cursor.fetchall()
    
    if not forex_symbols:
        print("❌ No active forex symbols found in master table.")
        exit()
    
    print(f"📊 Found {len(forex_symbols)} active forex pairs to process.")
except Exception as e:
    print("❌ Failed to fetch forex symbols:", e)
    exit()

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

for symbol, currency_from, currency_to, yfinance_symbol in forex_symbols:
    try:
        print(f"\n🔄 Processing {symbol} ({yfinance_symbol})...")
        
        # Fetch data from yfinance
        ticker = yf.Ticker(yfinance_symbol)
        
        # Get 2 days of data to ensure we have previous day
        hist = ticker.history(period="2d", interval="1d")
        
        if hist.empty:
            print(f"⚠️  No data found for {symbol}. Skipping...")
            error_count += 1
            continue
        
        # Get info for additional data
        info = ticker.info
        
        # Reset index to get Date as column
        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.date
        
        # Filter for previous day only
        prev_data = hist[hist['Date'] == prev_day.date()]
        
        if prev_data.empty:
            print(f"⚠️  No data for {prev_day_str} for {symbol}. Using latest available...")
            prev_data = hist.tail(1)
        
        # Extract data
        for _, row in prev_data.iterrows():
            trading_date = row['Date']
            open_price = float(row['Open']) if pd.notna(row['Open']) else None
            high_price = float(row['High']) if pd.notna(row['High']) else None
            low_price = float(row['Low']) if pd.notna(row['Low']) else None
            close_price = float(row['Close']) if pd.notna(row['Close']) else None
            volume = int(row['Volume']) if pd.notna(row['Volume']) else 0
            
            # Get additional info from ticker.info
            previous_close = info.get('previousClose', None)
            bid_price = info.get('bid', None)
            ask_price = info.get('ask', None)
            fifty_two_week_high = info.get('fiftyTwoWeekHigh', None)
            fifty_two_week_low = info.get('fiftyTwoWeekLow', None)
            fifty_day_avg = info.get('fiftyDayAverage', None)
            two_hundred_day_avg = info.get('twoHundredDayAverage', None)
            market_state = info.get('marketState', 'REGULAR')
            exchange = info.get('exchange', 'CCY')
            
            # Calculate daily change
            daily_change = None
            daily_change_pct = None
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
                    open_price = ?,
                    high_price = ?,
                    low_price = ?,
                    close_price = ?,
                    volume = ?,
                    daily_change = ?,
                    daily_change_pct = ?,
                    previous_close = ?,
                    bid_price = ?,
                    ask_price = ?,
                    fifty_two_week_high = ?,
                    fifty_two_week_low = ?,
                    fifty_day_avg = ?,
                    two_hundred_day_avg = ?,
                    market_state = ?,
                    exchange = ?,
                    last_updated = GETDATE()
                WHERE symbol = ? AND trading_date = ?
                """
                cursor.execute(update_query, (
                    open_price, high_price, low_price, close_price, volume,
                    daily_change, daily_change_pct, previous_close,
                    bid_price, ask_price, fifty_two_week_high, fifty_two_week_low,
                    fifty_day_avg, two_hundred_day_avg, market_state, exchange,
                    symbol, trading_date
                ))
                print(f"✅ Updated {symbol} for {trading_date}")
            else:
                # Insert new record
                insert_query = f"""
                INSERT INTO {target_table} (
                    symbol, currency_from, currency_to, trading_date,
                    open_price, high_price, low_price, close_price, volume,
                    daily_change, daily_change_pct, previous_close,
                    bid_price, ask_price, fifty_two_week_high, fifty_two_week_low,
                    fifty_day_avg, two_hundred_day_avg, market_state, exchange,
                    data_source, last_updated, created_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'yfinance', GETDATE(), GETDATE())
                """
                cursor.execute(insert_query, (
                    symbol, currency_from, currency_to, trading_date,
                    open_price, high_price, low_price, close_price, volume,
                    daily_change, daily_change_pct, previous_close,
                    bid_price, ask_price, fifty_two_week_high, fifty_two_week_low,
                    fifty_day_avg, two_hundred_day_avg, market_state, exchange
                ))
                print(f"✅ Inserted {symbol} for {trading_date}")
            
            conn.commit()
            success_count += 1
            
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
print("📊 FOREX DATA UPDATE SUMMARY")
print("="*60)
print(f"✅ Successfully processed: {success_count} records")
print(f"❌ Errors: {error_count} records")
print(f"📅 Date: {prev_day_str}")
print("="*60)
