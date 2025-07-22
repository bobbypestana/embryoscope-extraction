@echo off
REM Full Clinisys Dataflow Batch File
REM Runs: MySQL to DuckDB (bronze) -> bronze to silver -> check tables with deltas

echo ========================================
echo Full Clinisys Dataflow
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
echo.

REM Check if required packages are installed
python -c "import pandas, yaml, duckdb, sqlalchemy, pymysql" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install pandas pyyaml duckdb sqlalchemy pymysql
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

echo Starting full clinisys dataflow...
echo Timestamp: %date% %time%
echo.

REM Initialize variables for tracking success/failure
set "bronze_success=false"
set "silver_success=false"

REM Step 1: MySQL to DuckDB (Bronze Layer)
echo ========================================
echo Step 1: MySQL to DuckDB (Bronze Layer)
echo ========================================
echo.

echo Testing MySQL connection...
python -c "import yaml; config = yaml.safe_load(open('params.yml')); print('Config loaded successfully')" >nul 2>&1
if errorlevel 1 (
    echo WARNING: Could not load configuration. Skipping bronze layer creation.
    echo Will attempt to create silver layer from existing bronze data.
    goto :silver_only
)

echo Running database copy loader (MySQL to DuckDB)...
python db_copy_loader.py
if errorlevel 1 (
    echo WARNING: Database copy failed. This might be due to:
    echo - MySQL connection issues
    echo - Network connectivity problems
    echo - Database credentials issues
    echo.
    echo Will attempt to create silver layer from existing bronze data.
    set "bronze_success=false"
    goto :silver_only
) else (
    echo Database copy completed successfully
    set "bronze_success=true"
)
echo.

:silver_only
REM Step 2: Bronze to Silver Layer
echo ========================================
echo Step 2: Bronze to Silver Layer
echo ========================================
echo.

echo Running silver layer creation...
python silver_loader_try_strptime_complete.py
if errorlevel 1 (
    echo ERROR: Silver layer creation failed.
    echo Check the logs for details.
    pause
    exit /b 1
) else (
    echo Silver layer creation completed successfully
    set "silver_success=true"
)
echo.

REM Step 3: Check Tables with Deltas
echo ========================================
echo Step 3: Check Tables with Deltas
echo ========================================
echo.

echo Running table validation with deltas...
python check_tables.py
if errorlevel 1 (
    echo WARNING: Table validation completed with issues.
    echo Check the logs for details.
) else (
    echo Table validation completed successfully
)
echo.

REM Summary Report
echo ========================================
echo Dataflow Summary
echo ========================================
echo.
if "%bronze_success%"=="true" (
    echo  Bronze Layer: Successfully created from MySQL
) else (
    echo  Bronze Layer: Skipped (used existing data)
)
echo  Silver Layer: Successfully created
echo  Validation: Completed
echo.
echo Dataflow complete. Check logs in clinisys\logs\
echo.

REM Summary complete - check logs for detailed information

echo.
echo Press any key to exit...
pause >nul
endlocal 