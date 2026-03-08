@echo off
REM Batch file to fetch fundamental data for NSE500 and NASDAQ100
REM Usage: run_fundamental_data.bat [nse|nasdaq|all]
REM   Default: all (fetches both markets)
echo ========================================
echo Starting Fundamental Data Fetch
echo Date: %date% Time: %time%
echo ========================================

cd /d "C:\Users\sreea\OneDrive\Documents\stockanalysis"

REM Default to 'all' if no argument provided
set MARKET=%1
if "%MARKET%"=="" set MARKET=all

echo.
echo Fetching fundamental data for market: %MARKET%
python get_fundamental_data.py --market %MARKET%

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo ✅ Fundamental data fetch completed successfully!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo ❌ Fundamental data fetch completed with failures - check email
    echo ========================================
)

REM echo.
REM echo Press any key to exit...
REM pause > nul
