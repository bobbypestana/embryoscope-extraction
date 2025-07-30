@echo off
REM Full Embryoscope Dataflow Batch File
REM Runs: extract -> bronze to silver -> consolidate -> gold

echo ========================================
echo Full Embryoscope Dataflow
setlocal enabledelayedexpansion
echo ========================================
echo.

REM Set the working directory to the script location
cd /d "%~dp0"

REM Activate the try_request environment
echo Activating try_request environment...
call conda activate try_request
if errorlevel 1 (
    echo ERROR: Failed to activate try_request environment
    echo Please ensure the environment exists: conda create -n try_request python=3.9
    pause
    exit /b 1
)

echo Environment activated successfully.
echo.

REM Check if Python is available in the environment
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not available in try_request environment
    echo Please check your conda installation and environment
    pause
    exit /b 1
)

echo Python found in try_request environment. Checking dependencies...

REM Check if required packages are installed
python -c "import pandas, requests, pyyaml, duckdb, urllib3, tqdm" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ERROR: Failed to install required packages
        pause
        exit /b 1
    )
)

echo Dependencies OK.
echo.

REM Create logs directory if it doesn't exist
if not exist "logs" mkdir logs

REM Create database directory if it doesn't exist
if not exist "..\database" mkdir "..\database"

echo Starting full embryoscope dataflow...
echo Timestamp: %date% %time%
echo.

REM Step 1: Extract from API to bronze
echo Running extraction step...
python 01_source_to_bronze.py
if errorlevel 1 (
    echo ERROR: Extraction step failed.
    pause
    exit /b 1
) else (
    echo Extraction step completed successfully
)
echo.

REM Step 2: Bronze to Silver
echo Running bronze to silver step...
python 02_01_bronze_to_silver.py
if errorlevel 1 (
    echo ERROR: Bronze to Silver step failed.
    pause
    exit /b 1
)
echo Bronze to silver step completed successfully
echo.

REM Step 3: Cleanup Silver Layer (remove orphaned records)
echo Running silver layer cleanup step...
python 02_02_cleanup_silver_layer.py
if errorlevel 1 (
    echo ERROR: Silver layer cleanup step failed.
    pause
    exit /b 1
)
echo Silver layer cleanup step completed successfully
echo.

REM Step 4: Silver to Consolidate
echo Running consolidation step...
python 02_03_consolidate_embryoscope_dbs.py
if errorlevel 1 (
    echo ERROR: Consolidation step failed.
    pause
    exit /b 1
)
echo Consolidation step completed successfully
echo.

REM Step 5: Silver to Gold (Embryoscope only)
echo Running embryoscope gold layer creation step...
python 03_consolidated_to_gold.py
if errorlevel 1 (
    echo ERROR: Embryoscope gold layer creation step failed.
    pause
    exit /b 1
)
echo Embryoscope gold layer creation step completed successfully
echo.

echo Dataflow complete. Check logs in embryoscope\logs\
echo.
echo Press any key to exit...
pause >nul
endlocal 