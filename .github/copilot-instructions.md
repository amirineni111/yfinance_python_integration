# Copilot Instructions — stockanalysis

## Project Context
This is the **data ingestion ETL layer** — the foundation of a 7-repo stock trading analytics platform. Fetches market data from yfinance/Alpha Vantage APIs and loads into shared SQL Server database.

## Key Architecture Rules
- This is the **ONLY repo** that performs bulk market data insertion
- Writes to `nasdaq_100_hist_data`, `nse_500_hist_data`, `forex_hist_data`, fundamentals tables
- Equity price columns are stored as **VARCHAR** (legacy decision) — downstream must CAST to FLOAT
- Forex columns are DECIMAL (no casting needed)
- Database: `stockdata_db` on `localhost\MSSQLSERVER01` (Windows Auth)

## Data Sources
- **yfinance**: OHLCV for NASDAQ 100, NSE 500, Forex pairs (free)
- **Alpha Vantage**: Fundamentals (37 metrics), some price data (5 calls/min free tier)

## ETL Pattern
1. Fetch from API (yfinance/Alpha Vantage)
2. Transform/clean pandas DataFrame
3. Bulk insert to SQL Server via sql_loader.py
4. Upsert logic prevents duplicates (ticker + trading_date)

## Tables Populated
- `nasdaq_100_hist_data` (~128K rows), `nse_500_hist_data` (~510K rows)
- `forex_hist_data`, `nasdaq_top100`, `nse_500`, `forex_master`
- `nasdaq_100_fundamentals`, `nse_500_fundamentals`

## Unused Components
- `dags/` — Legacy Airflow DAGs (not in use, ignore)
- Snowflake/DataHub references — dead code

## Sibling Repositories (all consume this data)
- `sqlserver_copilot` — NASDAQ ML training
- `sqlserver_copilot_nse` — NSE ML training
- `sqlserver_copilot_forex` — Forex ML training
- `streamlit-trading-dashboard` — Visualization + views
- `stockdata_agenticai` — CrewAI agents
- `sqlserver_mcp` — .NET 8 MCP Server (Microsoft MssqlMcp) with 7 tools: ListTables, DescribeTable, ReadData, CreateTable, DropTable, InsertData, UpdateData. Stdio transport. Use to explore DB schemas and verify data freshness during development.

## MCP Server for Development
Configure in `.vscode/mcp.json` to query stockdata_db directly from your AI IDE:
```json
"MSSQL MCP": {
    "type": "stdio",
    "command": "C:\\Users\\sreea\\OneDrive\\Desktop\\sqlserver_mcp\\SQL-AI-samples\\MssqlMcp\\dotnet\\MssqlMcp\\bin\\Debug\\net8.0\\MssqlMcp.exe",
    "env": {
        "CONNECTION_STRING": "Server=localhost\\MSSQLSERVER01;Database=stockdata_db;Trusted_Connection=True;TrustServerCertificate=True"
    }
}
```
Useful for: verifying data freshness after ETL runs, checking row counts in hist tables, exploring ticker master data.
