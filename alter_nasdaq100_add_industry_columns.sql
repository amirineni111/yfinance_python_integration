-- SQL Script to alter nasdaq_top100 table and add industry/theme classification columns
-- This will add columns to categorize companies by sector, industry, and sub-industry

USE stockdata_db;
GO

-- Check if columns exist before adding them
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'nasdaq_top100' AND COLUMN_NAME = 'sector')
BEGIN
    ALTER TABLE nasdaq_top100
    ADD sector VARCHAR(100) NULL;
    PRINT '✅ Column "sector" added to nasdaq_top100 table';
END
ELSE
BEGIN
    PRINT 'ℹ Column "sector" already exists in nasdaq_top100 table';
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'nasdaq_top100' AND COLUMN_NAME = 'industry')
BEGIN
    ALTER TABLE nasdaq_top100
    ADD industry VARCHAR(150) NULL;
    PRINT '✅ Column "industry" added to nasdaq_top100 table';
END
ELSE
BEGIN
    PRINT 'ℹ Column "industry" already exists in nasdaq_top100 table';
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'nasdaq_top100' AND COLUMN_NAME = 'sub_industry')
BEGIN
    ALTER TABLE nasdaq_top100
    ADD sub_industry VARCHAR(200) NULL;
    PRINT '✅ Column "sub_industry" added to nasdaq_top100 table';
END
ELSE
BEGIN
    PRINT 'ℹ Column "sub_industry" already exists in nasdaq_top100 table';
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'nasdaq_top100' AND COLUMN_NAME = 'business_summary')
BEGIN
    ALTER TABLE nasdaq_top100
    ADD business_summary VARCHAR(MAX) NULL;
    PRINT '✅ Column "business_summary" added to nasdaq_top100 table';
END
ELSE
BEGIN
    PRINT 'ℹ Column "business_summary" already exists in nasdaq_top100 table';
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'nasdaq_top100' AND COLUMN_NAME = 'last_updated')
BEGIN
    ALTER TABLE nasdaq_top100
    ADD last_updated DATETIME NULL;
    PRINT '✅ Column "last_updated" added to nasdaq_top100 table';
END
ELSE
BEGIN
    PRINT 'ℹ Column "last_updated" already exists in nasdaq_top100 table';
END
GO

-- Display updated table structure
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'nasdaq_top100'
ORDER BY ORDINAL_POSITION;
GO

PRINT '✅ nasdaq_top100 table structure updated successfully!';
PRINT 'New columns added: sector, industry, sub_industry, business_summary, last_updated';
GO
