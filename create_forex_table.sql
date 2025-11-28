-- =============================================
-- Create Forex Historical Data Table for SQL Server
-- For storing AUD/USD and other currency pair data from yfinance
-- =============================================

USE stockdata_db;
GO

-- Drop table if exists (optional - comment out if you want to preserve existing data)
-- DROP TABLE IF EXISTS dbo.forex_hist_data;
-- GO

-- Create the main forex historical data table
CREATE TABLE dbo.forex_hist_data (
    -- Primary Key
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Currency Pair Information
    symbol NVARCHAR(20) NOT NULL,                    -- e.g., 'AUDUSD=X', 'EURUSD=X'
    currency_from NVARCHAR(10) NOT NULL,             -- e.g., 'AUD', 'EUR'
    currency_to NVARCHAR(10) NOT NULL,               -- e.g., 'USD'
    
    -- Date/Time Information
    trading_date DATE NOT NULL,                       -- Trading date
    trading_datetime DATETIME2(3) NULL,              -- Full timestamp if available
    
    -- OHLC Price Data
    open_price DECIMAL(18, 8) NULL,                  -- Opening price
    high_price DECIMAL(18, 8) NULL,                  -- Highest price
    low_price DECIMAL(18, 8) NULL,                   -- Lowest price
    close_price DECIMAL(18, 8) NOT NULL,             -- Closing price (required)
    
    -- Volume and Trading Data
    volume BIGINT NULL,                              -- Trading volume (often 0 for forex)
    
    -- Daily Statistics
    daily_change DECIMAL(18, 8) NULL,                -- Change from previous close
    daily_change_pct DECIMAL(10, 4) NULL,            -- Percentage change
    
    -- Market Information
    bid_price DECIMAL(18, 8) NULL,                   -- Current bid price
    ask_price DECIMAL(18, 8) NULL,                   -- Current ask price
    spread DECIMAL(18, 8) NULL,                      -- Bid-Ask spread
    
    -- Reference Prices
    previous_close DECIMAL(18, 8) NULL,              -- Previous day's closing price
    
    -- 52-Week Statistics
    fifty_two_week_high DECIMAL(18, 8) NULL,         -- 52-week high
    fifty_two_week_low DECIMAL(18, 8) NULL,          -- 52-week low
    
    -- Moving Averages
    fifty_day_avg DECIMAL(18, 8) NULL,               -- 50-day moving average
    two_hundred_day_avg DECIMAL(18, 8) NULL,         -- 200-day moving average
    
    -- Additional Information
    exchange NVARCHAR(50) NULL,                      -- Exchange name (e.g., 'CCY')
    market_state NVARCHAR(20) NULL,                  -- Market state (e.g., 'REGULAR', 'CLOSED')
    timezone_name NVARCHAR(50) NULL,                 -- Exchange timezone
    
    -- Metadata
    data_source NVARCHAR(50) DEFAULT 'yfinance',    -- Data source
    last_updated DATETIME2(3) DEFAULT GETDATE(),     -- When record was last updated
    created_date DATETIME2(3) DEFAULT GETDATE(),     -- When record was created
    
    -- Unique constraint to prevent duplicate entries
    CONSTRAINT UQ_forex_symbol_date UNIQUE (symbol, trading_date)
);
GO

-- Create indexes for better query performance
CREATE NONCLUSTERED INDEX IX_forex_symbol 
    ON dbo.forex_hist_data(symbol) 
    INCLUDE (trading_date, close_price);
GO

CREATE NONCLUSTERED INDEX IX_forex_trading_date 
    ON dbo.forex_hist_data(trading_date DESC) 
    INCLUDE (symbol, close_price);
GO

CREATE NONCLUSTERED INDEX IX_forex_symbol_date 
    ON dbo.forex_hist_data(symbol, trading_date DESC);
GO

-- Create a view for the latest forex rates
CREATE OR ALTER VIEW dbo.forex_latest_rates AS
SELECT 
    f.*,
    ROW_NUMBER() OVER (PARTITION BY f.symbol ORDER BY f.trading_date DESC) as rn
FROM dbo.forex_hist_data f;
GO

-- Create a view to get only the most recent rate for each currency pair
CREATE OR ALTER VIEW dbo.forex_current_rates AS
SELECT 
    symbol,
    currency_from,
    currency_to,
    trading_date,
    close_price as current_rate,
    previous_close,
    daily_change,
    daily_change_pct,
    bid_price,
    ask_price,
    spread,
    fifty_two_week_high,
    fifty_two_week_low,
    fifty_day_avg,
    two_hundred_day_avg,
    market_state,
    last_updated
FROM dbo.forex_latest_rates
WHERE rn = 1;
GO

-- Sample INSERT statement based on the yfinance data
-- This shows how to insert the AUD/USD data you fetched
INSERT INTO dbo.forex_hist_data (
    symbol,
    currency_from,
    currency_to,
    trading_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    previous_close,
    bid_price,
    ask_price,
    fifty_two_week_high,
    fifty_two_week_low,
    fifty_day_avg,
    two_hundred_day_avg,
    exchange,
    market_state,
    timezone_name
)
VALUES 
-- 5-Day Historical Data from your fetch
('AUDUSD=X', 'AUD', 'USD', '2025-11-20', 0.648980, 0.650210, 0.644710, 0.649001, 0, 0.6470817, NULL, NULL, 0.66920966, 0.5923083, 0.6543384, 0.646004, 'CCY', 'REGULAR', 'Europe/London'),
('AUDUSD=X', 'AUD', 'USD', '2025-11-21', 0.644890, 0.645900, 0.642160, 0.644950, 0, 0.649001, NULL, NULL, 0.66920966, 0.5923083, 0.6543384, 0.646004, 'CCY', 'REGULAR', 'Europe/London'),
('AUDUSD=X', 'AUD', 'USD', '2025-11-24', 0.646241, 0.646800, 0.644330, 0.646240, 0, 0.644950, NULL, NULL, 0.66920966, 0.5923083, 0.6543384, 0.646004, 'CCY', 'REGULAR', 'Europe/London'),
('AUDUSD=X', 'AUD', 'USD', '2025-11-25', 0.646789, 0.646900, 0.643730, 0.646580, 0, 0.646240, NULL, NULL, 0.66920966, 0.5923083, 0.6543384, 0.646004, 'CCY', 'REGULAR', 'Europe/London'),
('AUDUSD=X', 'AUD', 'USD', '2025-11-26', 0.647082, 0.652273, 0.646998, 0.651933, 0, 0.646580, 0.65193295, 0.6515083, 0.66920966, 0.5923083, 0.6543384, 0.646004, 'CCY', 'REGULAR', 'Europe/London');
GO

-- Verify the data was inserted
SELECT TOP 10 
    symbol,
    trading_date,
    open_price,
    high_price,
    low_price,
    close_price,
    daily_change_pct,
    fifty_day_avg,
    two_hundred_day_avg
FROM dbo.forex_hist_data
ORDER BY trading_date DESC;
GO

-- Query to calculate daily changes
UPDATE f
SET 
    daily_change = f.close_price - f.previous_close,
    daily_change_pct = CASE 
        WHEN f.previous_close > 0 THEN 
            ROUND(((f.close_price - f.previous_close) / f.previous_close) * 100, 4)
        ELSE NULL 
    END
FROM dbo.forex_hist_data f
WHERE daily_change IS NULL OR daily_change_pct IS NULL;
GO

PRINT 'Forex historical data table created successfully!';
PRINT 'Sample AUD/USD data inserted!';
GO
