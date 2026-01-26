-- Sample queries to analyze nasdaq_top100 companies by industry and sector
USE stockdata_db;
GO

-- 1. Count of companies by sector
SELECT 
    sector,
    COUNT(*) as company_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nasdaq_top100) AS DECIMAL(5,2)) as percentage
FROM nasdaq_top100
WHERE sector IS NOT NULL
GROUP BY sector
ORDER BY company_count DESC;
GO

-- 2. Count of companies by industry
SELECT 
    industry,
    COUNT(*) as company_count
FROM nasdaq_top100
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY company_count DESC;
GO

-- 3. Sector and Industry breakdown
SELECT 
    sector,
    industry,
    COUNT(*) as company_count
FROM nasdaq_top100
WHERE sector IS NOT NULL AND industry IS NOT NULL
GROUP BY sector, industry
ORDER BY sector, company_count DESC;
GO

-- 4. List all companies in a specific sector (e.g., Technology)
SELECT 
    ticker,
    company_name,
    sector,
    industry,
    sub_industry
FROM nasdaq_top100
WHERE sector = 'Technology'
ORDER BY company_name;
GO

-- 5. Find companies without industry classification
SELECT 
    ticker,
    company_name,
    sector,
    industry
FROM nasdaq_top100
WHERE sector IS NULL OR industry IS NULL
ORDER BY company_name;
GO

-- 6. Get unique sectors
SELECT DISTINCT sector
FROM nasdaq_top100
WHERE sector IS NOT NULL
ORDER BY sector;
GO

-- 7. Get unique industries within each sector
SELECT 
    sector,
    STRING_AGG(industry, ', ') WITHIN GROUP (ORDER BY industry) as industries
FROM (
    SELECT DISTINCT sector, industry
    FROM nasdaq_top100
    WHERE sector IS NOT NULL AND industry IS NOT NULL
) AS unique_combos
GROUP BY sector
ORDER BY sector;
GO

-- 8. Companies by sector with ticker symbols
SELECT 
    sector,
    STRING_AGG(ticker, ', ') WITHIN GROUP (ORDER BY ticker) as tickers,
    COUNT(*) as count
FROM nasdaq_top100
WHERE sector IS NOT NULL
GROUP BY sector
ORDER BY count DESC;
GO
