@echo off
REM Master batch file to fetch both fundamental and historical data
echo ========================================
echo Starting Complete Data Fetch
echo Date: %date% Time: %time%
echo ========================================

cd /d "C:\Users\sreea\OneDrive\Documents\stockanalysis"

echo.
echo [1/3] Fetching NSE 500 historical data...
python get_histdata_nse500_adhoc.py

echo.
echo [2/3] Fetching NASDAQ 100 historical data...
python get_histdata_nasdaq100_adhoc.py

echo.
echo [3/3] Fetching fundamental data...
python get_fundamental_data.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo ✅ All data fetch operations completed!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ❌ Some errors occurred during data fetch
    echo ========================================
)

echo.
echo Press any key to exit...
pause > nul
