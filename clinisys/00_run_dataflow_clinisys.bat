@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=1

echo ========================================
echo RUNNING COMPLETE CLINISYS DATAFLOW
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to clinisys directory
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
echo STEP %PARENT_STEP%.3: Compare Bronze and Silver Tables
echo ========================================
python 02_02_compare_bronze_and_silver.py
if %errorlevel% neq 0 (
    echo WARNING: Step %PARENT_STEP%.3 had issues, but continuing...
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.4: Silver to Gold
echo ========================================
python 03_silver_to_gold.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo DATAFLOW COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in clinisys\logs\
echo.
exit /b 0 