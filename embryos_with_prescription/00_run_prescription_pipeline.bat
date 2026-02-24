@echo off
echo ========================================
echo RUNNING PRESCRIPTION DATA PIPELINE
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to directory
    pause
    exit /b 1
)

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP 1: Joining Prescriptions (Long Table)
echo ========================================
python 01_join_prescriptions.py
if %errorlevel% neq 0 (
    echo ERROR: Step 1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Creating Wide Table
echo ========================================
python 02_create_wide_table.py
if %errorlevel% neq 0 (
    echo ERROR: Step 2 failed
    pause
    exit /b 1
)

@REM echo.
@REM echo ========================================
@REM echo STEP 3: Exporting to Excel
@REM echo ========================================
@REM python 03_export_to_excel.py
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 3 failed
@REM     pause
@REM     exit /b 1
@REM )

echo.
echo ========================================
echo PRESCRIPTION DATA PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in embryos_with_prescription\logs\
echo Check exports in embryos_with_prescription\exports\
echo.
exit /b 0
