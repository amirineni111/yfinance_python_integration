# yfinance Python Integration

Python automation scripts for fetching and managing financial market data using yfinance API.

## Overview

This repository contains automated data collection scripts for:
- **NSE 500 Stocks** (Indian market)
- **NASDAQ 100 Stocks** (US market)
- **Forex Currency Pairs** (Global forex markets)

All data is stored in SQL Server database for analysis and visualization.

## Features

- Daily automated data updates
- Adhoc historical data imports
- Flag-based selective processing
- Comprehensive error handling and logging
- SQL Server integration with optimized batch processing

## Scripts

### NSE 500 (Indian Stocks)
- `get_data_nse500_prev1day.py` - Daily updates for previous trading day
- `get_histdata_nse500_adhoc.py` - Historical data import (250 days)
- `get_nsetop500scriptnames.py` - Fetch NSE 500 stock list

### NASDAQ 100 (US Stocks)
- `get_data_nasdaq100prev1day.py` - Daily updates for previous trading day
- `get_histdata_nasdaq100_adhoc.py` - Historical data import (250 days)
- `get_nasdaqtop100scriptnames.py` - Fetch NASDAQ 100 stock list

### Forex Currency Pairs
- `get_data_forex_prev1day.py` - Daily updates for previous trading day
- `get_histdata_forex_adhoc.py` - Historical data import (365 days)
- `fetch_audusd.py` - Test script for AUDUSD data

### Trading Dashboard
- `streamlitapp_20251123_v2.py` - Comprehensive Streamlit trading dashboard
- Features: Interactive charts, technical indicators, flight status view, dark mode

## Database Schema

### SQL Server Tables
- `nse_500_hist_data` - NSE stock historical data
- `nasdaq_100_hist_data` - NASDAQ stock historical data
- `forex_hist_data` - Forex currency pair data
- `forex_master` - Forex pair configuration and control flags

### Database Setup
- `create_forex_table.sql` - Forex table structure with indexes and views

## Prerequisites

- Python 3.8+
- SQL Server (localhost\MSSQLSERVER01)
- ODBC Driver 17 for SQL Server

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/yfinance_python_integration.git
cd yfinance_python_integration
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Configure SQL Server connection in scripts (if needed):
```python
server = "localhost\\MSSQLSERVER01"
database = "stockdata_db"
```

## Usage

### Daily Updates (Schedule with Task Scheduler)

```bash
# NSE 500
python get_data_nse500_prev1day.py

# NASDAQ 100
python get_data_nasdaq100prev1day.py

# Forex
python get_data_forex_prev1day.py
```

### Historical Data Import

1. Set process flag in master table:
```sql
-- For Forex
UPDATE forex_master SET process_flag='Y' WHERE symbol='AUDUSD';
```

2. Run adhoc script:
```bash
python get_histdata_forex_adhoc.py
```

3. Flag automatically resets to 'N' after successful import

## Configuration

### Forex Master Table
- `process_flag='Y'` - Symbol will be processed by adhoc script
- `is_active='Y'` - Symbol will be updated by daily script

## Streamlit Dashboard

Launch the trading dashboard:
```bash
streamlit run streamlitapp_20251123_v2.py
```

Features:
- Technical indicators (RSI, MACD, Bollinger Bands, EMA/SMA, ATR)
- Flight status view (all stocks in single table)
- Dark mode theme
- Interactive Plotly charts

## Data Sources

- **yfinance** - Yahoo Finance API for market data
- Supports stocks, forex, cryptocurrencies, and more

## License

MIT License

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Author

Created for automated financial data collection and analysis.
