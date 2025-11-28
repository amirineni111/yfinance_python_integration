# This py script is for adhoc historical data import for Forex pairs
# Reads forex symbols from dbo.forex_master where process_flag='Y' and fetches specified number of days
import yfinance as yf
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "forex_master"
target_table = "forex_hist_data"

# Configuration - Change these as needed
DAYS_TO_FETCH = 365  # Number of days of historical data to fetch
BATCH_SIZE = 50      # Commit after every N records

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
    print(f"📅 Fetching last {DAYS_TO_FETCH} days of historical data...")
except Exception as e:
    print("❌ Failed to fetch forex symbols:", e)
    exit()

# Process each forex pair
total_records = 0
success_count = 0
error_count = 0
update_count = 0
insert_count = 0

for idx, (symbol, currency_from, currency_to, yfinance_symbol) in enumerate(forex_symbols, 1):
    try:
        print(f"\n{'='*70}")
        print(f"[{idx}/{len(forex_symbols)}] 🔄 Processing {symbol} ({yfinance_symbol})...")
        print(f"{'='*70}")
        
        # Fetch data from yfinance
        ticker = yf.Ticker(yfinance_symbol)
        
        # Get historical data
        hist = ticker.history(period=f"{DAYS_TO_FETCH}d", interval="1d")
        
        if hist.empty:
            print(f"⚠️  No data found for {symbol}. Skipping...")
            error_count += 1
            continue
        
        # Get ticker info for additional data
        info = ticker.info
        
        # Reset index to get Date as column
        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date']).dt.date
        
        print(f"📈 Retrieved {len(hist)} trading days of data")
        
        # Get static info that applies to all records
        fifty_two_week_high = info.get('fiftyTwoWeekHigh', None)
        fifty_two_week_low = info.get('fiftyTwoWeekLow', None)
        fifty_day_avg = info.get('fiftyDayAverage', None)
        two_hundred_day_avg = info.get('twoHundredDayAverage', None)
        market_state = info.get('marketState', 'REGULAR')
        exchange = info.get('exchange', 'CCY')
        bid_price = info.get('bid', None)
        ask_price = info.get('ask', None)
        
        # Process each day's data
        record_count = 0
        for row_idx, row in hist.iterrows():
            try:
                trading_date = row['Date']
                open_price = float(row['Open']) if pd.notna(row['Open']) else None
                high_price = float(row['High']) if pd.notna(row['High']) else None
                low_price = float(row['Low']) if pd.notna(row['Low']) else None
                close_price = float(row['Close']) if pd.notna(row['Close']) else None
                volume = int(row['Volume']) if pd.notna(row['Volume']) else 0
                
                # Calculate previous close (from previous row if available)
                previous_close = None
                if row_idx > 0:
                    prev_row = hist.iloc[row_idx - 1]
                    previous_close = float(prev_row['Close']) if pd.notna(prev_row['Close']) else None
                
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
                    update_count += 1
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
        
        print(f"✅ Completed {symbol}: {record_count} records processed")
        print(f"  📊 Inserts: {insert_count} | Updates: {update_count}")
        
        # Reset flag after successful processing
        try:
            cursor.execute(f"""
                UPDATE {source_table}
                SET process_flag = 'N',
                    last_processed = GETDATE()
                WHERE symbol = ?
            """, (symbol,))
            conn.commit()
            print(f"  🚩 Reset process_flag to 'N' for {symbol}")
        except Exception as flag_error:
            print(f"  ⚠️  Could not reset flag: {str(flag_error)}")
        
    except Exception as e:
        print(f"❌ Error processing {symbol}: {str(e)}")
        error_count += 1
        conn.rollback()
        continue

# Close connection
cursor.close()
conn.close()

# Final Summary
print("\n" + "="*70)
print("📊 FOREX HISTORICAL DATA IMPORT SUMMARY")
print("="*70)
print(f"✅ Symbols successfully processed: {success_count}/{len(forex_symbols)}")
print(f"❌ Symbols with errors: {error_count}")
print(f"📝 Total records processed: {total_records}")
print(f"  ➕ New records inserted: {insert_count}")
print(f"  🔄 Existing records updated: {update_count}")
print(f"📅 Historical period: Last {DAYS_TO_FETCH} days")
print("="*70)
print("\n💡 TIP: Set process_flag='Y' in forex_master for symbols you want to process")
print("   Example: UPDATE forex_master SET process_flag='Y' WHERE symbol IN ('EURUSD', 'GBPUSD')")
