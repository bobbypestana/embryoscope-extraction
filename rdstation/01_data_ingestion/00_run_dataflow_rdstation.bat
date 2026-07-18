@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=1

echo ========================================
echo RUNNING COMPLETE RD STATION CRM DATAFLOW
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to 01_data_ingestion directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    pause
    exit /b 1
)

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP %PARENT_STEP%.1: Extract from API to Bronze
echo ========================================
python -u 01_source_to_bronze.py %*
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Bronze to Silver
echo ========================================
python -u 02_bronze_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Silver to Gold
echo ========================================
python -u 03_silver_to_gold.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.4: Ingestion Dashboard
echo ========================================
python -u 04_generate_dashboard.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo RD STATION CRM DATAFLOW COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in rdstation\01_data_ingestion\logs\
echo.

exit /b 0
