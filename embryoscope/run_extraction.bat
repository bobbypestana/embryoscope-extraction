@echo off
REM Embryoscope Data Extraction Batch File
REM This script runs the embryoscope data extraction process

echo ========================================
echo Embryoscope Data Extraction
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

echo Starting embryoscope data extraction...
echo Timestamp: %date% %time%
echo.

REM Run the extraction script
python embryoscope_extractor.py

REM Check the exit code
if errorlevel 1 (
    echo.
    echo ERROR: Extraction failed with exit code %errorlevel%
    echo Check the logs for details
) else (
    echo.
    echo Extraction completed successfully
)

echo.
echo Press any key to exit...
pause >nul 