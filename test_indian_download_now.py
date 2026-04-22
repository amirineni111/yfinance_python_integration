"""Test downloading Indian market data right now to see if April 21 data is available"""
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

# Indian market tickers
indian_tickers = {
    '^INDIAVIX': 'india_vix',
    '^NSEI': 'nifty50',
    '^CNXIT': 'nifty_it',
    '^NSEBANK': 'nifty_bank',
    '^CNXPHARMA': 'nifty_pharma',
    '^CNXAUTO': 'nifty_auto',
    '^CNXFMCG': 'nifty_fmcg',
}

start_date = datetime(2026, 4, 18)
end_date = datetime(2026, 4, 22)

print(f"Downloading Indian market data from {start_date.date()} to {end_date.date()}")
print("=" * 80)

ticker_str = ' '.join(indian_tickers.keys())

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
    
    if raw.empty:
        print("❌ No data returned from yfinance")
    else:
        print(f"✅ Downloaded data with shape: {raw.shape}")
        print(f"Date range in response: {raw.index.min().date()} to {raw.index.max().date()}")
        print(f"\nDates available: {[d.date() for d in raw.index]}")
        
        print("\n" + "=" * 80)
        print("Checking each ticker:")
        print("=" * 80)
        
        for yf_ticker, col_prefix in indian_tickers.items():
            print(f"\n{yf_ticker} ({col_prefix}):")
            
            if isinstance(raw.columns, pd.MultiIndex):
                if yf_ticker in raw.columns.get_level_values(0):
                    ticker_data = raw[yf_ticker]
                    print(f"  ✅ Found in data")
                    print(f"  Dates: {list(ticker_data.index.map(lambda x: x.date()))}")
                    if 'Close' in ticker_data.columns:
                        print(f"  Close prices:")
                        for idx, val in ticker_data['Close'].items():
                            print(f"    {idx.date()}: {val}")
                    else:
                        print(f"  ❌ No 'Close' column")
                else:
                    print(f"  ❌ Not found in download results")
            else:
                print(f"  Single ticker mode (shouldn't happen)")
                
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
