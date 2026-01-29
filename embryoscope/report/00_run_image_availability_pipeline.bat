@echo off
REM ========================================
REM Embryo Image Availability Pipeline
REM Runs the complete 4-step ETL process
REM ========================================

REM Parse command line arguments
set MODE=%1
set LIMIT=%2

if "%MODE%"=="" set MODE=retry

echo ========================================
echo EMBRYO IMAGE AVAILABILITY PIPELINE
echo ========================================
echo Mode: %MODE%
if not "%LIMIT%"=="" echo Limit: %LIMIT% embryos
echo.
echo Default mode is 'retry' (new embryos + errors/no images)
echo Use: 00_run_image_availability_pipeline.bat [MODE] [LIMIT]
echo Modes: new, retry, all
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to report directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    pause
    exit /b 1
)

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request
if %errorlevel% neq 0 (
    echo ERROR: Failed to activate conda environment
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 1: Check Image Availability via API
echo ========================================
if "%LIMIT%"=="" (
    python 01_check_image_availability.py --mode %MODE%
) else (
    python 01_check_image_availability.py --mode %MODE% --limit %LIMIT%
)
if %errorlevel% neq 0 (
    echo ERROR: Step 1 failed
    pause
    exit /b 1
)

REM Find the most recent results directory
echo.
echo Finding most recent results directory...
for /f "delims=" %%i in ('dir /b /ad /o-d api_results\%MODE%_* 2^>nul') do (
    set RESULTS_DIR=api_results\%%i
    goto :found_dir
)
echo WARNING: No results directory found - no embryos were checked
echo Pipeline complete (no embryos to process)
pause
exit /b 0

:found_dir
echo Results directory: %RESULTS_DIR%

echo.
echo ========================================
echo STEP 2: Ingest JSON Results to Bronze
echo ========================================
python 02_logs_to_bronze.py --input-dir "%RESULTS_DIR%"
if %errorlevel% neq 0 (
    echo ERROR: Step 2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 3: Update Silver from Bronze
echo ========================================
python 03_bronze_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step 3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 4: Track Status Changes to Gold
echo ========================================
python 04_track_changes_to_gold.py
if %errorlevel% neq 0 (
    echo ERROR: Step 4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 5: Cleanup Old Logs and Results
echo ========================================
python 05_cleanup_logs.py
if %errorlevel% neq 0 (
    echo WARNING: Cleanup step failed (non-critical)
)

echo.
echo ========================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in embryoscope\report\logs\
echo.
pause
exit /b 0
