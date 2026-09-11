@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=7

echo ======================================================================
echo RUNNING REDLARA DATA INGESTION & SILVER TRANSFORMATION PIPELINE
echo ======================================================================
echo.

REM Change to project directory
cd /d "%~dp0"
cd ..\..

REM Activate conda environment
echo Activating conda environment try_request...
call conda activate try_request

echo.
echo ======================================================================
echo STEP %PARENT_STEP%.1: REDLARA to Bronze (All 12 files / All sheets)
echo ======================================================================
python redlara/01_data_ingestion/01_redlara_to_bronze.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 Bronze ingestion failed
    exit /b 1
)

echo.
echo ======================================================================
echo STEP %PARENT_STEP%.2: REDLARA Bronze to Silver (6 Dedicated Tables)
echo ======================================================================
python redlara/01_data_ingestion/02_redlara_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 Silver transformation failed
    exit /b 1
)

echo.
echo ======================================================================
echo STEP %PARENT_STEP%.3: Generate REDLARA Validation & Outcomes Report
echo ======================================================================
python redlara/01_data_ingestion/03_generate_redlara_report.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 Validation report failed
    exit /b 1
)

echo.
echo ======================================================================
echo REDLARA PIPELINE COMPLETED SUCCESSFULLY!
echo ======================================================================
echo All 6 Silver tables created and validated.
echo Report available at: redlara/silver_validation_report.md
echo.
exit /b 0
