@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=2

echo ========================================
echo RUNNING COMPLETE EMBRYOSCOPE DATAFLOW
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to embryoscope directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    pause
    exit /b 1
)

REM Activate conda environment (based on user memory)
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP %PARENT_STEP%.1: Extract from API to Bronze
echo ========================================
python 01_source_to_bronze.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Bronze to Silver
echo ========================================
python 02_01_bronze_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Cleanup Silver Layer
echo ========================================
python 02_02_cleanup_silver_layer.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.4: Consolidate Embryoscope Databases
echo ========================================
python 02_03_consolidate_embryoscope_dbs.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.5: Consolidated to Gold
echo ========================================
python 03_consolidated_to_gold.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.5 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo DATAFLOW COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in embryoscope\logs\
echo.
exit /b 0 