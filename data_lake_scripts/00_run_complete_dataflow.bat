@echo off
REM Complete Data Flow Batch File
REM Runs: clinisys -> embryoscope -> data_lake_scripts
REM This script combines all three data flows in the correct order

echo ========================================
echo COMPLETE DATA FLOW - ALL SYSTEMS
echo ========================================
echo.
echo This script will run all data flows in sequence:
echo 1. Clinisys data flow
echo 2. Embryoscope data flow  
echo 3. Data Lake consolidation
echo.
echo Timestamp: %date% %time%
echo ========================================
echo.

setlocal enabledelayedexpansion

REM Set the working directory to the script location
cd /d "%~dp0"
echo Current working directory: %CD%

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

echo Python found in try_request environment.
echo.

REM Create database directory if it doesn't exist
if not exist "..\database" mkdir "..\database"


REM Check if required packages are installed 
python -c "import pandas, requests, pyyaml, duckdb, urllib3, tqdm" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages for clinisys...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ERROR: Failed to install required packages for clinisys
        pause
        exit /b 1
    )
)


echo ========================================
echo PHASE 1: CLINISYS DATA FLOW
echo ========================================
echo.

REM Change to clinisys directory
cd /d "%~dp0\..\clinisys"

echo Current working directory: %CD%
echo.


REM Create logs directory if it doesn't exist
if not exist "logs" mkdir logs

echo Starting clinisys dataflow...
echo Timestamp: %date% %time%
echo.

REM Step 1: Extract from source to bronze
echo Running clinisys extraction step...
python 01_source_to_bronze.py
if errorlevel 1 (
    echo ERROR: Clinisys extraction step failed.
    pause
    exit /b 1
) else (
    echo Clinisys extraction step completed successfully
)
echo.

REM Step 2: Bronze to Silver
echo Running clinisys bronze to silver step...
python 02_01_bronze_to_silver.py
if errorlevel 1 (
    echo ERROR: Clinisys bronze to silver step failed.
    pause
    exit /b 1
)
echo Clinisys bronze to silver step completed successfully
echo.

REM Step 3: Check Tables
echo Running clinisys table validation step...
python 02_02_compare_bronze_and_silver.py
if errorlevel 1 (
    echo WARNING: Clinisys table validation step had issues, but continuing...
) else (
    echo Clinisys table validation step completed successfully
)
echo.

REM Step 4: Silver to Gold
echo Running clinisys silver to gold step...
python 03_silver_to_gold.py
if errorlevel 1 (
    echo ERROR: Clinisys silver to gold step failed.
    pause
    exit /b 1
)
echo Clinisys silver to gold step completed successfully
echo.

echo Clinisys dataflow complete.
echo.

echo ========================================
echo PHASE 2: EMBRYOSCOPE DATA FLOW
echo ========================================
echo.

REM Change to embryoscope directory
cd /d "%~dp0\..\embryoscope"

echo Current working directory: %CD%
echo.


REM Create logs directory if it doesn't exist
if not exist "logs" mkdir logs

echo Starting embryoscope dataflow...
echo Timestamp: %date% %time%
echo.

REM Step 1: Extract from API to bronze
echo Running embryoscope extraction step...
python 01_source_to_bronze.py
if errorlevel 1 (
    echo ERROR: Embryoscope extraction step failed.
    pause
    exit /b 1
) else (
    echo Embryoscope extraction step completed successfully
)
echo.

REM Step 2: Bronze to Silver
echo Running embryoscope bronze to silver step...
python 02_01_bronze_to_silver.py
if errorlevel 1 (
    echo ERROR: Embryoscope bronze to silver step failed.
    pause
    exit /b 1
)
echo Embryoscope bronze to silver step completed successfully
echo.

REM Step 3: Cleanup Silver Layer (remove orphaned records)
echo Running embryoscope silver layer cleanup step...
python 02_02_cleanup_silver_layer.py
if errorlevel 1 (
    echo ERROR: Embryoscope silver layer cleanup step failed.
    pause
    exit /b 1
)
echo Embryoscope silver layer cleanup step completed successfully
echo.

REM Step 4: Consolidate embryoscope databases
echo Running embryoscope consolidation step...
python 02_03_consolidate_embryoscope_dbs.py
if errorlevel 1 (
    echo ERROR: Embryoscope consolidation step failed.
    pause
    exit /b 1
)
echo Embryoscope consolidation step completed successfully
echo.

REM Step 5: Consolidated to Gold (Embryoscope only)
echo Running embryoscope gold layer creation step...
python 03_consolidated_to_gold.py
if errorlevel 1 (
    echo ERROR: Embryoscope gold layer creation step failed.
    pause
    exit /b 1
)
echo Embryoscope gold layer creation step completed successfully
echo.

echo Embryoscope dataflow complete.
echo.

echo ========================================
echo PHASE 3: DATA LAKE CONSOLIDATION
echo ========================================
echo.

REM Change back to data_lake_scripts directory
cd /d "%~dp0"

echo Current working directory: %CD%
echo.

echo Starting data lake consolidation...
echo Timestamp: %date% %time%
echo.

REM Run the combined gold loader
echo Running combined gold loader...
python 01_merge_clinisys_embryoscope.py
if errorlevel 1 (
    echo ERROR: Data lake consolidation step failed.
    pause
    exit /b 1
)
echo Data lake consolidation step completed successfully
echo.

REM Step 4: Export full table to Excel
echo Running export to Excel step...
python 02_export_full_table_to_excel.py
if errorlevel 1 (
    echo ERROR: Export to Excel step failed.
    pause
    exit /b 1
)
echo Export to Excel step completed successfully
echo.

echo ========================================
echo COMPLETE DATA FLOW FINISHED
echo ========================================
echo.
echo All data flows completed successfully!
echo.
echo Summary of completed flows:
echo - Clinisys: source_to_bronze -> bronze_to_silver -> silver_to_gold
echo - Embryoscope: source_to_bronze -> bronze_to_silver -> cleanup -> consolidate -> gold
echo - Data Lake: combined consolidation -> export to Excel
echo.
echo Check logs in respective directories:
echo - Clinisys logs: ..\clinisys\logs\
echo - Embryoscope logs: ..\embryoscope\logs\
echo - Data Lake logs: logs\
echo.
echo Final timestamp: %date% %time%
echo.
echo Press any key to exit...
pause >nul
endlocal 