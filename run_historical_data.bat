@echo off
REM Batch file to fetch historical price data for NSE500 and NASDAQ100
echo ========================================
echo Starting Historical Data Fetch
echo Date: %date% Time: %time%
echo ========================================

cd /d "C:\Users\sreea\OneDrive\Documents\stockanalysis"

echo.
echo Fetching NSE 500 historical data...
python get_histdata_nse500_adhoc.py

echo.
echo Fetching NASDAQ 100 historical data...
python get_histdata_nasdaq100_adhoc.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo ✅ Historical data fetch completed successfully!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ❌ Error occurred during historical data fetch
    echo ========================================
)

echo.
echo Press any key to exit...
pause > nul
