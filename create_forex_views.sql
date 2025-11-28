-- ============================================
-- FOREX TECHNICAL INDICATOR VIEWS
-- Adapted from NSE/NASDAQ views for Forex data
-- ============================================

-- Drop existing views if they exist
IF OBJECT_ID('dbo.forex_sma_signals', 'V') IS NOT NULL DROP VIEW dbo.forex_sma_signals;
IF OBJECT_ID('dbo.forex_rsi_signals', 'V') IS NOT NULL DROP VIEW dbo.forex_rsi_signals;
IF OBJECT_ID('dbo.forex_macd_signals', 'V') IS NOT NULL DROP VIEW dbo.forex_macd_signals;
IF OBJECT_ID('dbo.forex_bb_signals', 'V') IS NOT NULL DROP VIEW dbo.forex_bb_signals;
IF OBJECT_ID('dbo.forex_atr_spikes', 'V') IS NOT NULL DROP VIEW dbo.forex_atr_spikes;
IF OBJECT_ID('dbo.forex_ema_sma_view', 'V') IS NOT NULL DROP VIEW dbo.forex_ema_sma_view;
IF OBJECT_ID('dbo.forex_atr', 'V') IS NOT NULL DROP VIEW dbo.forex_atr;
IF OBJECT_ID('dbo.forex_bollingerband', 'V') IS NOT NULL DROP VIEW dbo.forex_bollingerband;
IF OBJECT_ID('dbo.forex_macd', 'V') IS NOT NULL DROP VIEW dbo.forex_macd;
IF OBJECT_ID('dbo.forex_RSI_calculation', 'V') IS NOT NULL DROP VIEW dbo.forex_RSI_calculation;
GO

-- ============================================
-- 1. RSI CALCULATION VIEW
-- ============================================
CREATE VIEW [dbo].[forex_RSI_calculation] AS
WITH GainsLosses AS (
    SELECT
        symbol,
        trading_date,
        CAST(close_price AS FLOAT) AS close_price,
        LAG(CAST(close_price AS FLOAT), 1) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_close,

        -- Calculate Gain and Loss
        CASE 
            WHEN CAST(close_price AS FLOAT) > LAG(CAST(close_price AS FLOAT), 1) OVER (PARTITION BY symbol ORDER BY trading_date) 
            THEN CAST(close_price AS FLOAT) - LAG(CAST(close_price AS FLOAT), 1) OVER (PARTITION BY symbol ORDER BY trading_date) 
            ELSE 0 
        END AS gain,

        CASE 
            WHEN CAST(close_price AS FLOAT) < LAG(CAST(close_price AS FLOAT), 1) OVER (PARTITION BY symbol ORDER BY trading_date) 
            THEN LAG(CAST(close_price AS FLOAT), 1) OVER (PARTITION BY symbol ORDER BY trading_date) - CAST(close_price AS FLOAT) 
            ELSE 0 
        END AS loss
    FROM forex_hist_data
),
AvgGainsLosses AS (
    SELECT
        symbol,
        trading_date,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM GainsLosses
)
SELECT
    symbol,
    trading_date,
    CASE 
        WHEN avg_loss = 0 THEN 100 
        ELSE 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))
    END AS RSI
FROM AvgGainsLosses;
GO

-- ============================================
-- 2. MACD VIEW
-- ============================================
CREATE VIEW [dbo].[forex_macd] AS

WITH PriceData AS (
    SELECT
        symbol,
        trading_date,
        CAST(close_price AS FLOAT) AS close_price,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trading_date) AS RowNum
    FROM forex_hist_data
),
EMA_Calculations AS (
    SELECT
        p.symbol,
        p.trading_date,
        p.close_price,
        
        -- Compute Exponential Weight Factor for EMA Calculation
        POWER(1 - (2.0 / (12 + 1)), RowNum - 1) AS Weight_12,
        POWER(1 - (2.0 / (26 + 1)), RowNum - 1) AS Weight_26

    FROM PriceData p
),
MACD_Calculations AS (
    SELECT
        e.symbol,
        e.trading_date,
        e.close_price,

        -- Calculate EMA-12 and EMA-26 with NULLIF to prevent division by zero
        SUM(e.close_price * e.Weight_12) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) /
        NULLIF(SUM(e.Weight_12) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 0) AS EMA_12,

        SUM(e.close_price * e.Weight_26) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) /
        NULLIF(SUM(e.Weight_26) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW), 0) AS EMA_26

    FROM EMA_Calculations e
),
Signal_Line_Calculations AS (
    SELECT
        m.symbol,
        m.trading_date,
        m.EMA_12,
        m.EMA_26,
        (m.EMA_12 - m.EMA_26) AS MACD,

        -- 9-day EMA of MACD as the Signal Line with NULLIF to prevent divide by zero
        AVG(m.EMA_12 - m.EMA_26) OVER (PARTITION BY m.symbol ORDER BY m.trading_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS Signal_Line
    FROM MACD_Calculations m
)
SELECT 
    symbol,
    trading_date,
    EMA_12,
    EMA_26,
    MACD,
    Signal_Line,
    
    -- MACD Crossover Indicator
    CASE WHEN MACD > Signal_Line THEN 'Bullish Crossover' ELSE 'Bearish Crossover' END AS MACD_Signal
FROM Signal_Line_Calculations
WHERE EMA_12 IS NOT NULL AND EMA_26 IS NOT NULL;
GO

-- ============================================
-- 3. BOLLINGER BANDS VIEW
-- ============================================
CREATE VIEW [dbo].[forex_bollingerband] AS
SELECT
    symbol,
    trading_date,
    CAST(close_price AS FLOAT) AS close_price,
    
    -- 20-day Simple Moving Average (SMA)
    AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS SMA_20,

    -- Upper Bollinger Band = SMA_20 + (2 * Standard Deviation)
    AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) +
    (2 * STDEV(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS Upper_Band,

    -- Lower Bollinger Band = SMA_20 - (2 * Standard Deviation)
    AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) -
    (2 * STDEV(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS Lower_Band

FROM forex_hist_data;
GO

-- ============================================
-- 4. ATR (AVERAGE TRUE RANGE) VIEW
-- ============================================
CREATE VIEW [dbo].[forex_atr] AS
WITH TR_Calculations AS (
    SELECT 
        symbol,
        trading_date,
        CAST(close_price AS FLOAT) AS close_price,
        -- Compute True Range
        ABS(CAST(close_price AS FLOAT) - LAG(CAST(close_price AS FLOAT), 1) OVER (PARTITION BY symbol ORDER BY trading_date)) AS True_Range
    FROM forex_hist_data
)
SELECT 
    symbol,
    trading_date,
    -- 14-day ATR Calculation
    AVG(True_Range) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS ATR_14
FROM TR_Calculations;
GO

-- ============================================
-- 5. EMA & SMA VIEW (Multiple Timeframes)
-- ============================================
CREATE VIEW [dbo].[forex_ema_sma_view] AS
WITH PriceData AS (
    SELECT
        symbol,
        trading_date,
        CAST(close_price AS FLOAT) AS close_price,
        
        -- Simple Moving Averages (SMA) using window functions
        AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS SMA_200,
        AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS SMA_100,
        AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS SMA_50,
        AVG(CAST(close_price AS FLOAT)) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS SMA_20,

        -- Row Number for EMA calculation
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trading_date) AS RowNum

    FROM forex_hist_data
),
EMA_Base AS (
    SELECT 
        p.symbol, 
        p.trading_date, 
        p.close_price, 
        p.SMA_200, 
        p.SMA_100, 
        p.SMA_50, 
        p.SMA_20, 
        p.RowNum,

        -- Precompute Exponential Weight Factor
        POWER(1 - (2.0 / (200 + 1)), RowNum - 1) AS Weight_200,
        POWER(1 - (2.0 / (100 + 1)), RowNum - 1) AS Weight_100,
        POWER(1 - (2.0 / (50 + 1)), RowNum - 1) AS Weight_50,
        POWER(1 - (2.0 / (20 + 1)), RowNum - 1) AS Weight_20

    FROM PriceData p
),
EMA_Calculations AS (
    SELECT 
        e.symbol, 
        e.trading_date, 
        e.close_price, 
        e.SMA_200, 
        e.SMA_100, 
        e.SMA_50, 
        e.SMA_20,

        -- Exponential Moving Average (EMA) Calculation
        SUM(e.close_price * e.Weight_200) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) /
        CASE WHEN SUM(e.Weight_200) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)=0 THEN 1 
		ELSE SUM(e.Weight_200) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) END 
		AS EMA_200,

        SUM(e.close_price * e.Weight_100) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) /
        CASE WHEN SUM(e.Weight_100) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)=0 THEN 1 
		ELSE SUM(e.Weight_100) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) END
		AS EMA_100,

        SUM(e.close_price * e.Weight_50) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) /
        CASE WHEN SUM(e.Weight_50) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)=0 THEN 1 
		ELSE SUM(e.Weight_50) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) END
		AS EMA_50,

        SUM(e.close_price * e.Weight_20) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) /
        CASE WHEN SUM(e.Weight_20) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)=0 THEN 1
		ELSE SUM(e.Weight_20) OVER (PARTITION BY e.symbol ORDER BY e.trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) END
		AS EMA_20

    FROM EMA_Base e
)

SELECT 
    symbol,
    trading_date,
    close_price,
    SMA_200,
    SMA_100,
    SMA_50,
    SMA_20,
    EMA_200,
    EMA_100,
    EMA_50,
    EMA_20,

    -- Flags for SMA Above/Below
    CASE WHEN close_price > SMA_200 THEN 'Above' ELSE 'Below' END AS SMA_200_Flag,
    CASE WHEN close_price > SMA_100 THEN 'Above' ELSE 'Below' END AS SMA_100_Flag,
    CASE WHEN close_price > SMA_50 THEN 'Above' ELSE 'Below' END AS SMA_50_Flag,
    CASE WHEN close_price > SMA_20 THEN 'Above' ELSE 'Below' END AS SMA_20_Flag,

    -- Flags for EMA Above/Below
    CASE WHEN close_price > EMA_200 THEN 'Above' ELSE 'Below' END AS EMA_200_Flag,
    CASE WHEN close_price > EMA_100 THEN 'Above' ELSE 'Below' END AS EMA_100_Flag,
    CASE WHEN close_price > EMA_50 THEN 'Above' ELSE 'Below' END AS EMA_50_Flag,
    CASE WHEN close_price > EMA_20 THEN 'Above' ELSE 'Below' END AS EMA_20_Flag

FROM EMA_Calculations;
GO

-- ============================================
-- SIGNAL VIEWS (Build on base indicator views)
-- ============================================

-- ============================================
-- 6. ATR SPIKES (Volatility Detection)
-- ============================================
CREATE VIEW dbo.forex_atr_spikes AS
SELECT *,
  CASE 
    WHEN atr_14 > (AVG(atr_14) OVER (PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) * 1.5
         THEN 'High Volatility'
    ELSE NULL
  END AS atr_volatility_signal
FROM dbo.forex_atr
WHERE atr_14 IS NOT NULL;
GO

-- ============================================
-- 7. BOLLINGER BAND SIGNALS
-- ============================================
CREATE VIEW dbo.forex_bb_signals AS
SELECT *,
  CASE 
    WHEN close_price > upper_band THEN 'Breakout Above Upper Band (Sell Zone)'
    WHEN close_price < lower_band THEN 'Breakdown Below Lower Band (Buy Zone)'
    ELSE NULL
  END AS bb_trade_signal
FROM dbo.forex_bollingerband
WHERE upper_band IS NOT NULL AND lower_band IS NOT NULL;
GO

-- ============================================
-- 8. MACD SIGNALS (Crossover Detection)
-- ============================================
CREATE VIEW dbo.forex_macd_signals AS
WITH cte AS (
    SELECT
        symbol,
        trading_date,
        MACD,
        Signal_Line,
        LAG(MACD) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_macd,
        LAG(Signal_Line) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_signal
    FROM dbo.forex_macd
)
SELECT
    symbol,
    trading_date,
    MACD,
    Signal_Line,
    CASE
        WHEN prev_macd < prev_signal AND MACD > Signal_Line THEN 'Bullish Crossover'
        WHEN prev_macd > prev_signal AND MACD < Signal_Line THEN 'Bearish Crossover'
        ELSE 'No Signal'
    END AS MACD_Signal
FROM cte;
GO

-- ============================================
-- 9. RSI SIGNALS (Overbought/Oversold)
-- ============================================
CREATE VIEW dbo.forex_rsi_signals AS
SELECT *,
  CASE 
    WHEN rsi < 30 THEN 'Oversold (Buy)'
    WHEN rsi > 70 THEN 'Overbought (Sell)'
    ELSE NULL
  END AS rsi_trade_signal
FROM dbo.forex_RSI_calculation
WHERE rsi IS NOT NULL;
GO

-- ============================================
-- 10. SMA SIGNALS (Golden/Death Cross)
-- ============================================
CREATE VIEW dbo.forex_sma_signals AS
SELECT *,
  CASE 
    WHEN LAG(sma_20, 1) OVER (PARTITION BY symbol ORDER BY trading_date) < LAG(sma_50, 1) OVER (PARTITION BY symbol ORDER BY trading_date)
         AND sma_20 > sma_50 THEN 'Golden Cross'
    WHEN LAG(sma_20, 1) OVER (PARTITION BY symbol ORDER BY trading_date) > LAG(sma_50, 1) OVER (PARTITION BY symbol ORDER BY trading_date)
         AND sma_20 < sma_50 THEN 'Death Cross'
    ELSE NULL
  END AS sma_trade_signal
FROM dbo.forex_ema_sma_view
WHERE sma_20 IS NOT NULL AND sma_50 IS NOT NULL;
GO

-- ============================================
-- VERIFICATION QUERIES
-- ============================================
PRINT '============================================';
PRINT 'Forex Technical Indicator Views Created:';
PRINT '============================================';
PRINT '1. forex_RSI_calculation';
PRINT '2. forex_macd';
PRINT '3. forex_bollingerband';
PRINT '4. forex_atr';
PRINT '5. forex_ema_sma_view';
PRINT '6. forex_atr_spikes';
PRINT '7. forex_bb_signals';
PRINT '8. forex_macd_signals';
PRINT '9. forex_rsi_signals';
PRINT '10. forex_sma_signals';
PRINT '============================================';
PRINT 'Run these queries to verify:';
PRINT 'SELECT TOP 10 * FROM forex_RSI_calculation;';
PRINT 'SELECT TOP 10 * FROM forex_macd;';
PRINT 'SELECT TOP 10 * FROM forex_rsi_signals WHERE rsi_trade_signal IS NOT NULL;';
PRINT '============================================';
