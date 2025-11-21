@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=3

echo ========================================
echo RUNNING DATA LAKE CONSOLIDATION
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to data_lake_scripts directory
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
echo STEP %PARENT_STEP%.1: Merging Clinisys and Embryoscope Data
echo ========================================
python 01_merge_clinisys_embryoscope.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

@REM echo.
@REM echo ========================================
@REM echo STEP 2: Exporting Full Table to Excel
@REM echo ========================================
@REM python 02_export_full_table_to_excel.py
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 2 failed
@REM     pause
@REM     exit /b 1
@REM )

echo.
echo ========================================
echo DATA LAKE CONSOLIDATION COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in data_lake_scripts\logs\
echo.
exit /b 0 