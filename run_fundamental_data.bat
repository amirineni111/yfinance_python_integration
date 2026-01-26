@echo off
REM Batch file to fetch fundamental data for NSE500 and NASDAQ100
echo ========================================
echo Starting Fundamental Data Fetch
echo Date: %date% Time: %time%
echo ========================================

cd /d "C:\Users\sreea\OneDrive\Documents\stockanalysis"

echo.
echo Fetching fundamental data...
python get_fundamental_data.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo ✅ Fundamental data fetch completed successfully!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ❌ Error occurred during fundamental data fetch
    echo ========================================
)

REM echo.
REM echo Press any key to exit...
REM pause > nul
