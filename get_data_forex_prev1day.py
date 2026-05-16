# This py script runs daily using Windows Task Scheduler to get previous day data for Forex pairs
# Reads forex symbols from dbo.forex_master table and inserts into dbo.forex_hist_data
# Uses OANDA v20 REST API for accurate forex data
import os
import logging
import argparse
import pandas as pd
import pyodbc
import requests
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

import sys

# Fix Windows console encoding (charmap) — must be done before any logging
# Without this, Polygon.io error messages containing non-ASCII chars (e.g. ¥ for JPY)
# crash the StreamHandler and surface as a misleading 'charmap' error instead of the real one
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Load .env from the script's own directory (not CWD — important for Task Scheduler)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

# ── File Logging ──────────────────────────────────────────────────────────────
log_dir = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'get_data_forex_prev1day.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("=" * 60)
logger.info("get_data_forex_prev1day.py started")
logger.info(f"Script dir: {SCRIPT_DIR} | CWD: {os.getcwd()}")

# CLI args
parser = argparse.ArgumentParser(description="Load Forex daily bars into forex_hist_data")
parser.add_argument(
    "--target-date",
    type=str,
    default=None,
    help="Specific date to load in YYYY-MM-DD format. If omitted, ET 5 PM cutoff logic is used."
)
args = parser.parse_args()

# SQL Server Connection Details
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
source_table = "forex_master"
target_table = "forex_hist_data"

# OANDA v20 REST API Configuration
OANDA_API_TOKEN = os.getenv("OANDA_API_TOKEN")
if not OANDA_API_TOKEN:
    logger.error("OANDA_API_TOKEN not found in .env file. Exiting.")
    exit(1)
OANDA_ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice")  # "practice" or "live"
OANDA_BASE_URL = (
    "https://api-fxpractice.oanda.com" if OANDA_ENVIRONMENT == "practice"
    else "https://api-fxtrade.oanda.com"
)
API_WAIT_TIME = 1   # OANDA allows 120 req/s; small delay between pairs is sufficient
logger.info(f"OANDA environment: {OANDA_ENVIRONMENT} | Base URL: {OANDA_BASE_URL}")

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
    logger.error(f"Connection failed: {e}")
    exit(1)

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
        logger.error("No active forex symbols found in master table. Exiting.")
        exit(1)
    
    logger.info(f"Found {len(forex_symbols)} active forex pairs to process.")
    logger.info("Using OANDA v20 REST API for data retrieval.")
except Exception as e:
    logger.error(f"Failed to fetch forex symbols: {e}")
    exit(1)

# Function to fetch forex data from OANDA v20 REST API
def fetch_forex_latest(from_currency, to_currency, api_token, target_date, oanda_base_url):
    """
    Fetch forex daily mid-price candle from OANDA v20 REST API for a specific date.
    Daily bars are aligned to 17:00 New York time (standard forex NY close convention).

    Parameters:
    -----------
    from_currency : str
        Base currency (e.g., 'AUD', 'EUR')
    to_currency : str
        Quote currency (e.g., 'USD')
    api_token : str
        OANDA v20 personal access token
    target_date : date
        Target trading date
    oanda_base_url : str
        OANDA API base URL (practice or live environment)

    Returns:
    --------
    dict or None: Trading data for the target date
    """
    # OANDA instrument format: EUR_USD, GBP_USD, etc.
    instrument = f"{from_currency}_{to_currency}"
    # Fetch a 3-day window to reliably capture the target date's daily bar.
    # OANDA daily bars are anchored to 17:00 New York time, so the bar labelled
    # with time T21:00:00Z (5pm EDT) represents the next calendar day's trading.
    # A generous window ensures we always capture the right complete candle.
    from_dt = f"{(target_date - timedelta(days=2)).strftime('%Y-%m-%d')}T00:00:00Z"
    to_dt = f"{(target_date + timedelta(days=1)).strftime('%Y-%m-%d')}T00:00:00Z"

    url = f"{oanda_base_url}/v3/instruments/{instrument}/candles"
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Accept-Datetime-Format': 'RFC3339',
    }
    params = {
        'price': 'M',                          # Mid prices (bid/ask average)
        'granularity': 'D',                    # Daily bars
        'from': from_dt,
        'to': to_dt,
        'dailyAlignment': '17',                # 5pm NY close convention
        'alignmentTimezone': 'America/New_York',
    }

    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning(f"Rate limited (429) for {from_currency}/{to_currency}. Waiting {wait}s before retry {attempt + 1}/3...")
                time.sleep(wait)
                continue

            if response.status_code == 401:
                logger.error(f"OANDA 401 Unauthorized for {from_currency}/{to_currency} — check OANDA_API_TOKEN in .env")
                return None

            if response.status_code == 404:
                logger.warning(f"OANDA 404 — instrument {instrument} not found or not supported on this account")
                return None

            response.raise_for_status()
            data = response.json()

            # Filter to complete candles only (incomplete = still forming in current session)
            candles = [c for c in data.get('candles', []) if c.get('complete', False)]

            if not candles:
                logger.warning(f"No complete candle data from OANDA for {from_currency}/{to_currency} on {target_date}")
                return None

            # Take the last complete candle in the window (closest to target date)
            candle = candles[-1]
            mid = candle['mid']

            return {
                'trading_date': target_date,
                'open_price': float(mid['o']),
                'high_price': float(mid['h']),
                'low_price': float(mid['l']),
                'close_price': float(mid['c']),
                'volume': int(candle.get('volume', 0))  # Tick volume (price tick count)
            }

        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout for {from_currency}/{to_currency} (attempt {attempt + 1}/3)")
        except Exception as e:
            logger.error(f"Error fetching data for {from_currency}/{to_currency}: {e}")
            return None

    logger.error(f"All retries exhausted for {from_currency}/{to_currency}")
    return None

# Calculate target trading day based on ET 5 PM forex daily close
def get_previous_trading_day(reference_date, steps_back=1):
    """
    Return the previous weekday date stepping back `steps_back` trading days.
    Trading day here means Mon-Fri (weekends excluded).
    """
    d = reference_date
    moved = 0
    while moved < steps_back:
        d = d - timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            moved += 1
    return d


def get_target_and_fallback_days(now_et):
    """
    Determine target/fallback trading days using ET close cutoff (5:00 PM ET).

    Rules:
    - Weekdays at/after 5:00 PM ET: target = today
    - Weekdays before 5:00 PM ET: target = previous trading day
    - Weekends: target = previous trading day (Friday)
    - fallback = trading day before target
    """
    close_cutoff = now_et.replace(hour=17, minute=0, second=0, microsecond=0)
    today_et_date = now_et.date()

    if now_et.weekday() >= 5:  # Saturday/Sunday
        target = get_previous_trading_day(today_et_date, steps_back=1)
    elif now_et >= close_cutoff:
        target = today_et_date
    else:
        target = get_previous_trading_day(today_et_date, steps_back=1)

    fallback = get_previous_trading_day(target, steps_back=1)
    return target, fallback


def parse_target_date(value):
    """Parse YYYY-MM-DD string to date, returning None when not supplied."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid --target-date '{value}'. Expected format: YYYY-MM-DD")
        exit(1)


now_et = datetime.now(ZoneInfo("America/New_York"))
cli_target_date = parse_target_date(args.target_date)

if cli_target_date:
    target_day = cli_target_date
    fallback_day = cli_target_date  # exact-date load by default when override is provided
    logger.info(f"Using CLI override date: {cli_target_date.strftime('%Y-%m-%d')}")
else:
    target_day, fallback_day = get_target_and_fallback_days(now_et)

target_day_str = target_day.strftime('%Y-%m-%d')
fallback_day_str = fallback_day.strftime('%Y-%m-%d')
logger.info(f"Current ET time: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
logger.info(f"Primary target date: {target_day_str} | Fallback date: {fallback_day_str}")

# Process each forex pair
success_count = 0
error_count = 0

for idx, (symbol, currency_from, currency_to, yfinance_symbol) in enumerate(forex_symbols, 1):
    try:
        logger.info(f"[{idx}/{len(forex_symbols)}] Processing {symbol} ({currency_from}/{currency_to})...")
        
        # Fetch data from OANDA — try primary date first, then fallback date
        forex_data = fetch_forex_latest(currency_from, currency_to, OANDA_API_TOKEN, target_day, OANDA_BASE_URL)

        if not forex_data and target_day != fallback_day:
            logger.info(f"Primary date not available, trying fallback date {fallback_day_str}...")
            forex_data = fetch_forex_latest(currency_from, currency_to, OANDA_API_TOKEN, fallback_day, OANDA_BASE_URL)
        
        if not forex_data:
            logger.warning(f"No data found for {symbol}. Skipping.")
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
            logger.info(f"Updated {symbol} for {trading_date}")
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
            logger.info(f"Inserted {symbol} for {trading_date}")
        
        conn.commit()
        success_count += 1
        
        # Rate limiting for Polygon free tier safety
        if idx < len(forex_symbols):
            time.sleep(API_WAIT_TIME)
            
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
        error_count += 1
        conn.rollback()
        continue

# Close connection
cursor.close()
conn.close()

# Summary
logger.info("=" * 60)
logger.info("FOREX DATA UPDATE SUMMARY (OANDA v20 REST API)")
logger.info(f"Successfully processed: {success_count} records | Errors: {error_count} records")
logger.info(f"Target date: {target_day_str} | Fallback: {fallback_day_str}")
logger.info("=" * 60)
