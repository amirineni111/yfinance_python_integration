# This py script fetches forex data for a custom date range
# Configure the date range parameters below and run the script
import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

# ========================================
# CONFIGURATION PARAMETERS - MODIFY THESE
# ========================================

# Date Range Configuration
START_DATE = "2025-12-05"  # Format: "YYYY-MM-DD" 
END_DATE = "2025-12-07"    # Format: "YYYY-MM-DD"

# Forex Symbols to Process (leave empty to process all active symbols)
# Example: ["EURUSD", "GBPUSD", "AUDUSD"] or [] for all
FOREX_SYMBOLS_FILTER = []

# Processing Options
BATCH_SIZE = 50           # Commit after every N records
OVERWRITE_EXISTING = True # True: Update existing records, False: Skip existing

# ========================================
# SQL SERVER CONNECTION
# ========================================
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

# Validate date range
try:
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    if start_dt >= end_dt:
        raise ValueError("START_DATE must be before END_DATE")
        
    days_diff = (end_dt - start_dt).days
    print(f"📅 Date Range: {START_DATE} to {END_DATE} ({days_diff} days)")
    
except Exception as e:
    print(f"❌ Invalid date configuration: {e}")
    exit()

# Fetch forex symbols from master table
try:
    if FOREX_SYMBOLS_FILTER:
        # Use specified symbols
        symbol_list = "', '".join(FOREX_SYMBOLS_FILTER)
        query = f"""
            SELECT symbol, currency_from, currency_to, yfinance_symbol 
            FROM {source_table} 
            WHERE symbol IN ('{symbol_list}') AND is_active = 'Y'
            ORDER BY symbol
        """
    else:
        # Use all active symbols
        query = f"""
            SELECT symbol, currency_from, currency_to, yfinance_symbol 
            FROM {source_table} 
            WHERE is_active = 'Y'
            ORDER BY symbol
        """
    
    cursor.execute(query)
    forex_symbols = cursor.fetchall()
    
    if not forex_symbols:
        print("❌ No forex symbols found matching criteria.")
        exit()
    
    print(f"📊 Found {len(forex_symbols)} forex pairs to process.")
    
    # Display symbols to process
    print("💱 Symbols to process:", [row[0] for row in forex_symbols])
    
except Exception as e:
    print("❌ Failed to fetch forex symbols:", e)
    exit()

# Process each forex pair
total_records = 0
success_count = 0
error_count = 0
update_count = 0
insert_count = 0

print("\n" + "="*80)
print(f"🚀 STARTING FOREX DATA IMPORT")
print(f"📅 Date Range: {START_DATE} to {END_DATE}")
print(f"🔄 Overwrite Existing: {'Yes' if OVERWRITE_EXISTING else 'No'}")
print("="*80)

for idx, (symbol, currency_from, currency_to, yfinance_symbol) in enumerate(forex_symbols, 1):
    try:
        print(f"\n[{idx}/{len(forex_symbols)}] 🔄 Processing {symbol} ({yfinance_symbol})...")
        
        # Fetch data from yfinance for the date range
        ticker = yf.Ticker(yfinance_symbol)
        
        # Calculate period to fetch (add buffer days)
        buffer_days = 10
        fetch_start = start_dt - timedelta(days=buffer_days)
        fetch_end = end_dt + timedelta(days=buffer_days)
        
        # Get historical data
        hist = ticker.history(start=fetch_start, end=fetch_end, interval="1d")
        
        if hist.empty:
            print(f"  ⚠️  No data found for {symbol}. Skipping...")
            error_count += 1
            continue
        
        # Reset index and filter for exact date range
        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.date
        
        # Filter for specified date range
        hist = hist[(hist['Date'] >= start_dt.date()) & (hist['Date'] <= end_dt.date())]
        
        if hist.empty:
            print(f"  ⚠️  No data in specified date range for {symbol}. Skipping...")
            error_count += 1
            continue
        
        print(f"  📈 Retrieved {len(hist)} trading days of data")
        
        # Get additional info from ticker
        info = ticker.info
        exchange = info.get('exchange', 'CCY')
        market_state = info.get('marketState', 'REGULAR')
        
        # Process each day's data
        batch_count = 0
        symbol_inserts = 0
        symbol_updates = 0
        
        for _, row in hist.iterrows():
            try:
                trading_date = row['Date']
                open_price = float(row['Open']) if pd.notna(row['Open']) else None
                high_price = float(row['High']) if pd.notna(row['High']) else None
                low_price = float(row['Low']) if pd.notna(row['Low']) else None
                close_price = float(row['Close']) if pd.notna(row['Close']) else None
                volume = int(row['Volume']) if pd.notna(row['Volume']) else 0
                
                # Calculate daily change (use previous close from data)
                daily_change = None
                daily_change_pct = None
                previous_close = info.get('previousClose', None)
                if close_price and previous_close:
                    daily_change = close_price - previous_close
                    daily_change_pct = (daily_change / previous_close) * 100
                
                # Get additional market data
                bid_price = info.get('bid', None)
                ask_price = info.get('ask', None)
                fifty_two_week_high = info.get('fiftyTwoWeekHigh', None)
                fifty_two_week_low = info.get('fiftyTwoWeekLow', None)
                fifty_day_avg = info.get('fiftyDayAverage', None)
                two_hundred_day_avg = info.get('twoHundredDayAverage', None)
                
                # Check if record already exists
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {target_table} 
                    WHERE symbol = ? AND trading_date = ?
                """, (symbol, trading_date))
                
                exists = cursor.fetchone()[0]
                
                if exists > 0 and not OVERWRITE_EXISTING:
                    # Skip existing record
                    continue
                elif exists > 0:
                    # Update existing record
                    update_query = f"""
                    UPDATE {target_table}
                    SET 
                        currency_from = ?, currency_to = ?,
                        open_price = ?, high_price = ?, low_price = ?, close_price = ?,
                        volume = ?, daily_change = ?, daily_change_pct = ?,
                        previous_close = ?, bid_price = ?, ask_price = ?,
                        fifty_two_week_high = ?, fifty_two_week_low = ?,
                        fifty_day_avg = ?, two_hundred_day_avg = ?,
                        exchange = ?, market_state = ?, created_date = GETDATE()
                    WHERE symbol = ? AND trading_date = ?
                    """
                    cursor.execute(update_query, (
                        currency_from, currency_to,
                        open_price, high_price, low_price, close_price, volume,
                        daily_change, daily_change_pct, previous_close,
                        bid_price, ask_price, fifty_two_week_high, fifty_two_week_low,
                        fifty_day_avg, two_hundred_day_avg, exchange, market_state,
                        symbol, trading_date
                    ))
                    symbol_updates += 1
                    update_count += 1
                else:
                    # Insert new record
                    insert_query = f"""
                    INSERT INTO {target_table} (
                        symbol, currency_from, currency_to, trading_date, open_price, high_price, low_price, close_price,
                        volume, daily_change, daily_change_pct, previous_close,
                        bid_price, ask_price, fifty_two_week_high, fifty_two_week_low,
                        fifty_day_avg, two_hundred_day_avg, exchange, market_state, created_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                    """
                    cursor.execute(insert_query, (
                        symbol, currency_from, currency_to, trading_date, open_price, high_price, low_price, close_price,
                        volume, daily_change, daily_change_pct, previous_close,
                        bid_price, ask_price, fifty_two_week_high, fifty_two_week_low,
                        fifty_day_avg, two_hundred_day_avg, exchange, market_state
                    ))
                    symbol_inserts += 1
                    insert_count += 1
                
                batch_count += 1
                total_records += 1
                
                # Commit in batches
                if batch_count % BATCH_SIZE == 0:
                    conn.commit()
                    print(f"    💾 Committed {batch_count}/{len(hist)} records...")
                    
            except Exception as row_error:
                print(f"    ❌ Error processing {trading_date}: {str(row_error)}")
                continue
        
        # Final commit for this symbol
        conn.commit()
        
        print(f"  ✅ Completed {symbol}: {len(hist)} records processed")
        print(f"    📊 Inserts: {symbol_inserts} | Updates: {symbol_updates}")
        
        success_count += 1
        
    except Exception as e:
        print(f"  ❌ Error processing {symbol}: {str(e)}")
        error_count += 1
        conn.rollback()
        continue

# Close connection
cursor.close()
conn.close()

# Final Summary
print("\n" + "="*80)
print("📊 FOREX CUSTOM DATE RANGE IMPORT SUMMARY")
print("="*80)
print(f"✅ Symbols successfully processed: {success_count}/{len(forex_symbols)}")
print(f"❌ Symbols with errors: {error_count}")
print(f"📝 Total records processed: {total_records}")
print(f"  ➕ New records inserted: {insert_count}")
print(f"  🔄 Existing records updated: {update_count}")
print(f"📅 Date range: {START_DATE} to {END_DATE}")
print(f"🔄 Overwrite mode: {'Enabled' if OVERWRITE_EXISTING else 'Disabled'}")
print("="*80)

if error_count > 0:
    print(f"\n⚠️  {error_count} symbols had errors. Check the output above for details.")

print("\n💡 TIP: To modify date range or symbols, edit the configuration section at the top of this script.")