@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=1

echo ========================================
echo RUNNING REDLARA DATA INGESTION PIPELINE
echo ========================================
echo.

REM Change to the script directory
cd /d "%~dp0"

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP %PARENT_STEP%.1: Ingest Excel to Bronze
echo ========================================
python "01_ingest_bronze.py"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Unify Tables to Silver
echo ========================================
python "02_unify_silver.py"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Generate Filling Report
echo ========================================
python "03_generate_filling_report.py"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo REDLARA INGESTION PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check the logs folder for execution details.
echo.
exit /b 0
