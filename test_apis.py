"""Test API endpoints for fetching stock lists."""
import requests
import time
import json

# Test NASDAQ screener API
print("=== Testing NASDAQ Screener API ===")
for exchange in ['nasdaq', 'nyse', 'amex']:
    url = f"https://api.nasdaq.com/api/screener/stocks?tableType=most_active&exchange={exchange}&limit=5000&offset=0"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        print(f"{exchange}: status={resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            rows = data.get('data', {}).get('table', {}).get('rows', [])
            if not rows:
                rows = data.get('data', {}).get('rows', [])
            print(f"  rows: {len(rows)}")
            if rows:
                print(f"  sample keys: {list(rows[0].keys())}")
                print(f"  sample: {rows[0]}")
            else:
                dk = list(data.get('data', {}).keys())
                print(f"  data keys: {dk}")
        else:
            print(f"  body: {resp.text[:300]}")
    except Exception as e:
        print(f"{exchange}: error - {e}")
    time.sleep(1)

print()

# Test NSE India API
print("=== Testing NSE India API ===")
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
})
try:
    r = session.get('https://www.nseindia.com/', timeout=15)
    print(f"NSE home: status={r.status_code}")
    time.sleep(2)
    
    for idx in ['NIFTY 500', 'NIFTY TOTAL MARKET', 'NIFTY MICROCAP 250']:
        url = f'https://www.nseindia.com/api/equity-stockIndices?index={requests.utils.quote(idx)}'
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('data', [])
            print(f"  {idx}: {len(items)} items")
            if items:
                print(f"    sample keys: {list(items[0].keys())[:8]}")
        else:
            print(f"  {idx}: status={resp.status_code}")
        time.sleep(1)
except Exception as e:
    print(f"NSE error: {e}")

# Test S&P 500 GitHub list
print()
print("=== Testing S&P 500 GitHub CSV ===")
try:
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    resp = requests.get(url, timeout=15)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        lines = resp.text.strip().split('\n')
        print(f"Lines: {len(lines)} (incl header)")
        print(f"Header: {lines[0]}")
        print(f"Sample: {lines[1]}")
except Exception as e:
    print(f"Error: {e}")
