-- ============================================================
-- Cleanup Duplicate Rows & Add Unique Indexes
-- ============================================================
-- Run this ONCE to fix existing duplicate data and prevent
-- future duplicates at the database level.
--
-- Problem: Holiday runs of ETL scripts re-inserted the previous
-- trading day's data, creating duplicate rows.
-- ============================================================

USE stockdata_db;
GO

-- ============================================================
-- STEP 1: Check how many duplicates exist (preview)
-- ============================================================
PRINT '=== Duplicate Preview ===';

SELECT 'nasdaq_100_hist_data' AS table_name,
       COUNT(*) AS duplicate_rows
FROM (
    SELECT ticker, trading_date, COUNT(*) AS cnt
    FROM nasdaq_100_hist_data
    GROUP BY ticker, trading_date
    HAVING COUNT(*) > 1
) dups;

SELECT 'nse_500_hist_data' AS table_name,
       COUNT(*) AS duplicate_rows
FROM (
    SELECT ticker, trading_date, COUNT(*) AS cnt
    FROM nse_500_hist_data
    GROUP BY ticker, trading_date
    HAVING COUNT(*) > 1
) dups;

-- ============================================================
-- STEP 2: Remove duplicates from nasdaq_100_hist_data
-- Keeps the row with the lowest internal row ID (first inserted)
-- ============================================================
PRINT 'Removing duplicates from nasdaq_100_hist_data...';

;WITH cte_nasdaq AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ticker, trading_date
               ORDER BY (SELECT NULL)  -- keep any one row
           ) AS rn
    FROM nasdaq_100_hist_data
)
DELETE FROM cte_nasdaq WHERE rn > 1;

PRINT CONCAT('  Deleted ', @@ROWCOUNT, ' duplicate rows from nasdaq_100_hist_data');
GO

-- ============================================================
-- STEP 3: Remove duplicates from nse_500_hist_data
-- ============================================================
PRINT 'Removing duplicates from nse_500_hist_data...';

;WITH cte_nse AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ticker, trading_date
               ORDER BY (SELECT NULL)
           ) AS rn
    FROM nse_500_hist_data
)
DELETE FROM cte_nse WHERE rn > 1;

PRINT CONCAT('  Deleted ', @@ROWCOUNT, ' duplicate rows from nse_500_hist_data');
GO

-- ============================================================
-- STEP 4: Add unique indexes to prevent future duplicates
-- ============================================================
PRINT 'Creating unique indexes...';

-- NASDAQ 100
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_nasdaq100_ticker_date'
      AND object_id = OBJECT_ID('nasdaq_100_hist_data')
)
BEGIN
    CREATE UNIQUE INDEX UQ_nasdaq100_ticker_date
    ON nasdaq_100_hist_data (ticker, trading_date);
    PRINT '  Created UQ_nasdaq100_ticker_date';
END
ELSE
    PRINT '  UQ_nasdaq100_ticker_date already exists';
GO

-- NSE 500
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_nse500_ticker_date'
      AND object_id = OBJECT_ID('nse_500_hist_data')
)
BEGIN
    CREATE UNIQUE INDEX UQ_nse500_ticker_date
    ON nse_500_hist_data (ticker, trading_date);
    PRINT '  Created UQ_nse500_ticker_date';
END
ELSE
    PRINT '  UQ_nse500_ticker_date already exists';
GO

-- ============================================================
-- STEP 5: Verify — should return 0 duplicates
-- ============================================================
PRINT '=== Post-Cleanup Verification ===';

SELECT 'nasdaq_100_hist_data' AS table_name,
       COUNT(*) AS remaining_duplicates
FROM (
    SELECT ticker, trading_date
    FROM nasdaq_100_hist_data
    GROUP BY ticker, trading_date
    HAVING COUNT(*) > 1
) dups;

SELECT 'nse_500_hist_data' AS table_name,
       COUNT(*) AS remaining_duplicates
FROM (
    SELECT ticker, trading_date
    FROM nse_500_hist_data
    GROUP BY ticker, trading_date
    HAVING COUNT(*) > 1
) dups;

PRINT 'Done! Duplicates cleaned and unique indexes created.';
GO
