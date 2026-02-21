# CLAUDE.md — stockanalysis (Data Ingestion ETL)

> **Project context file for AI assistants (Claude, Copilot, Cursor).**

---

## 1. SYSTEM OVERVIEW

This is the **data ingestion ETL layer** — one of **7 interconnected repositories** that form an AI-powered stock trading analytics platform. It fetches market data from external APIs and loads it into the shared SQL Server database.

### Repository Map

| Layer | Repo | Purpose |
|-------|------|---------|
| **Data Ingestion** ⭐ | **`stockanalysis`** | **THIS REPO** — ETL: yfinance/Alpha Vantage → SQL Server |
| SQL Infrastructure | `sqlserver_mcp` | .NET 8 MCP Server (Microsoft MssqlMcp) — 7 tools (ListTables, DescribeTable, ReadData, CreateTable, DropTable, InsertData, UpdateData) via stdio transport for AI IDE ↔ SQL Server |
| Dashboard | `streamlit-trading-dashboard` | 40+ views, signal tracking, Streamlit UI |
| ML: NASDAQ | `sqlserver_copilot` | Gradient Boosting → `ml_trading_predictions` |
| ML: NSE | `sqlserver_copilot_nse` | 5-model ensemble → `ml_nse_trading_predictions` |
| ML: Forex | `sqlserver_copilot_forex` | XGBoost/LightGBM → `forex_ml_predictions` |
| Agentic AI | `stockdata_agenticai` | 7 CrewAI agents, daily briefing email |

---

## 2. THIS REPO: stockanalysis

### Purpose
The **foundational data layer**. All other repos depend on data loaded by this repo. It:
1. Fetches daily OHLCV data for NASDAQ 100, NSE 500, and Forex pairs
2. Fetches fundamental data for equity stocks
3. Manages ticker master lists
4. Loads everything into SQL Server

### Data Sources
| Source | API | Data |
|--------|-----|------|
| **yfinance** | Yahoo Finance (free) | OHLCV prices for NSE 500, NASDAQ 100, Forex |
| **Alpha Vantage** | Alpha Vantage API | Fundamental data, some price data |

### Key Files

```
stockanalysis/
├── src/
│   ├── fetch_nasdaq_data.py         # NASDAQ 100 OHLCV → nasdaq_100_hist_data
│   ├── fetch_nse_data.py            # NSE 500 OHLCV → nse_500_hist_data
│   ├── fetch_forex_data.py          # Forex pairs → forex_hist_data
│   ├── fetch_fundamentals.py        # Fundamentals → *_fundamentals tables
│   ├── manage_tickers.py            # Ticker master list management
│   ├── sql_loader.py                # Generic SQL Server bulk loader
│   ├── sql_queries.py               # SQL queries for data operations
│   └── utils.py                     # Shared utilities
├── config/
│   ├── settings.py                  # .env configuration
│   ├── nasdaq_tickers.csv           # NASDAQ 100 ticker list
│   ├── nse_tickers.csv              # NSE 500 ticker list
│   └── forex_pairs.csv              # 10 forex pair definitions
├── sql/
│   ├── create_tables.sql            # Table creation scripts
│   └── create_indexes.sql           # Performance indexes
├── logs/
│   └── *.log
├── dags/                            # Airflow DAGs (UNUSED — legacy)
├── notebooks/
│   └── *.ipynb                      # Exploratory notebooks
└── requirements.txt
```

---

## 3. TABLES CREATED & POPULATED

### Market Data Tables
| Table | ~Rows | Source | Key Columns |
|-------|-------|--------|-------------|
| `nasdaq_100_hist_data` | 127,889 | yfinance | ticker, trading_date, open/high/low/close_price (**VARCHAR**), volume |
| `nse_500_hist_data` | 509,799 | yfinance | Same schema (VARCHAR prices) |
| `forex_hist_data` | — | yfinance | symbol, currency_from/to, OHLC (**DECIMAL**), daily_change, 50d/200d avg |

### Ticker Master Tables
| Table | Rows | Key Columns |
|-------|------|-------------|
| `nasdaq_top100` | 100 | ticker (PK), company_name, sector, industry, process_flag |
| `nse_500` | 500 | ticker (PK), company_name, sector, industry, process_flag |
| `forex_master` | 10 | symbol, currency_from, currency_to, is_active |

### Fundamental Tables
| Table | Source | Key Columns |
|-------|--------|-------------|
| `nasdaq_100_fundamentals` | Alpha Vantage | ticker, fetch_date, 37 financial metrics |
| `nse_500_fundamentals` | Alpha Vantage | Same schema |

### Market Context Table
| Table | Source | Key Columns |
|-------|--------|-------------|
| `market_context_daily` | yfinance | trading_date (PK), VIX/India VIX close+change, S&P 500/NASDAQ/NIFTY 50 close+return, DXY close+return, US 10Y yield+change, 11 US sector ETF returns (XLK..XLU), 5 India NIFTY sector index returns |

**ETL Script**: `get_market_context_daily.py` — batch downloads 23 tickers from yfinance, supports `--backfill` for 2-year historical load. Added as step [0/4] in `run_all_data_fetch.bat`.

**Consumers**: All 3 ML pipelines merge this data on `trading_date` for market regime features.

---

## 4. CRITICAL DATA NOTES

### VARCHAR Price Columns
The equity tables (`nasdaq_100_hist_data`, `nse_500_hist_data`) store prices as **VARCHAR**, not FLOAT/DECIMAL. This is a known legacy design decision. All downstream consumers MUST:
```sql
CAST(close_price AS FLOAT)
```
Forex tables (`forex_hist_data`) use DECIMAL — no casting needed.

### Data Freshness
- Equity data: Updated daily after market close
- Forex data: Updated daily (24/5 market)
- Fundamentals: Updated periodically (Alpha Vantage rate limits)
- `process_flag` in master tables tracks which tickers need data refresh

### Alpha Vantage API Key
There is a hardcoded API key in some scripts (known security issue — should be in .env).

---

## 5. UNUSED COMPONENTS

### Airflow DAGs (`dags/`)
Legacy Airflow DAG definitions that are NOT in use. The project moved to Windows Task Scheduler for orchestration. These files can be ignored.

### Snowflake / DataHub References
Some code references Snowflake or DataHub that were explored but never implemented. These are dead code.

---

## 6. DATABASE CONTEXT

### Connection
- **Server**: `192.168.87.27\MSSQLSERVER01` (Machine A LAN IP)
- **Database**: `stockdata_db`
- **Auth**: SQL Auth (`remote_user`, `SQL_TRUSTED_CONNECTION=no`)

### This Repo's Role
This is the **ONLY repo that performs bulk data insertion** for market data and fundamentals. All other repos either:
- Read market data (ML pipelines, agentic AI, dashboard)
- Write prediction/tracking data (ML pipelines, dashboard)
- Read everything (MCP server)

---

## 7. CODING CONVENTIONS

### ETL Pattern
```python
# 1. Fetch data from API
df = yf.download(ticker, start=start_date, end=end_date)
# 2. Transform/clean
df = transform_data(df)
# 3. Load to SQL Server
sql_loader.bulk_insert(df, table_name, connection)
```

### Error Handling
- API rate limit handling (especially Alpha Vantage: 5 calls/min free tier)
- Missing data handling (holidays, delisted stocks)
- Duplicate prevention (upsert logic on ticker + trading_date)

### Testing
```bash
python src/fetch_nasdaq_data.py --test  # Test fetch without loading
python src/manage_tickers.py --validate  # Validate ticker lists
```

---

## 8. DOWNSTREAM DEPENDENCIES

Every other repo depends on this one for base data:
- **sqlserver_copilot** — Reads `nasdaq_100_hist_data` for ML training
- **sqlserver_copilot_nse** — Reads `nse_500_hist_data` for ML training
- **sqlserver_copilot_forex** — Reads `forex_hist_data` for ML training
- **streamlit-trading-dashboard** — Reads all market data tables for visualization + creates views
- **stockdata_agenticai** — Queries market data via predefined SQL
- **sqlserver_mcp** — Exposes all tables to AI IDEs (7 MCP tools: ListTables, DescribeTable, ReadData, CreateTable, DropTable, InsertData, UpdateData)

If data ingestion fails, the ENTIRE pipeline is affected.

---

## 9. MCP SERVER FOR DEVELOPMENT

The `sqlserver_mcp` repo provides an MCP server for AI IDEs to query `stockdata_db` directly during development.

### VS Code Configuration
```json
"MSSQL MCP": {
    "type": "stdio",
    "command": "C:\\Users\\sreea\\OneDrive\\Desktop\\sqlserver_mcp\\SQL-AI-samples\\MssqlMcp\\dotnet\\MssqlMcp\\bin\\Debug\\net8.0\\MssqlMcp.exe",
    "env": {
        "CONNECTION_STRING": "Server=192.168.87.27\\MSSQLSERVER01;Database=stockdata_db;User Id=remote_user;Password=YourStrongPassword123!;TrustServerCertificate=True"
    }
}
```

### 7 MCP Tools: ListTables, DescribeTable, ReadData, CreateTable, DropTable, InsertData, UpdateData

Useful for: verifying data freshness after ETL runs, checking row counts, exploring ticker master tables, validating `market_context_daily` data.
