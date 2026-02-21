# AGENTS.md — stockanalysis (Data Ingestion ETL)

## Overview
This repo does NOT contain CrewAI agents. It is the **foundational data ingestion ETL layer** that populates the shared SQL Server database.

## ETL Architecture

```
External APIs
├── yfinance (Yahoo Finance)
│   ├── NASDAQ 100 OHLCV → nasdaq_100_hist_data
│   ├── NSE 500 OHLCV   → nse_500_hist_data
│   └── Forex 10 pairs  → forex_hist_data
│
└── Alpha Vantage
    ├── NASDAQ fundamentals → nasdaq_100_fundamentals
    └── NSE fundamentals    → nse_500_fundamentals
        │
        ▼
  sql_loader.py (bulk insert to SQL Server)
        │
        ▼
  stockdata_db (192.168.87.27\MSSQLSERVER01)
```

## Critical Data Note
- Equity price columns are **VARCHAR** (not FLOAT) — all downstream consumers must CAST
- Forex price columns are DECIMAL — no casting needed

## Ecosystem Role
This is the **foundation** — every other repo depends on data loaded here:
- 3 ML pipelines read market data for training
- Dashboard reads market data for visualization + creates views
- Agentic AI queries market data for analysis
- MCP server exposes tables to AI IDEs

If data ingestion fails, the ENTIRE pipeline is affected.

## Unused Components
- `dags/` — Legacy Airflow DAGs (not in use)
- Snowflake/DataHub references — explored but never implemented
