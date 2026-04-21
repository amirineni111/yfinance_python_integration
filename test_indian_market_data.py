"""Quick test to check if Indian market data is available from yfinance today"""
import yfinance as yf
from datetime import datetime, timedelta

# Test downloading Indian tickers
indian_tickers = ['^INDIAVIX', '^NSEI', '^CNXIT', '^NSEBANK']

print("Testing Indian market data availability for recent days:")
print("=" * 80)

end_date = datetime.today() + timedelta(days=1)
start_date = datetime.today() - timedelta(days=5)

for ticker in indian_tickers:
    print(f"\nTicker: {ticker}")
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if not data.empty:
            print(f"  Last available date: {data.index[-1].date()}")
            print(f"  Last close: {data['Close'].iloc[-1]}")
            print(f"  Total days: {len(data)}")
        else:
            print(f"  ❌ No data returned")
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "=" * 80)
print("Note: If last available date is not today (2026-04-21),")
print("it might be a market holiday in India or data lag from yfinance.")
