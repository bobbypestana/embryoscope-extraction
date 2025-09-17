@echo off
echo ========================================
echo RUNNING COMPLETE FINOPS PIPELINE
echo ========================================
echo.

REM Change to the project directory
cd /d "%~dp0"
cd ..\..

REM Activate conda environment (based on user memory)
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP 1: Creating All Patient Timeline
echo ========================================
python finops/02_create_tables/01_create_all_patient_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step 1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Creating Clean Timeline View
echo ========================================
python finops/02_create_tables/02_create_clean_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step 2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 3: Creating FinOps Summary Table
echo ========================================
python finops/02_create_tables/03_create_finops_summary.py
if %errorlevel% neq 0 (
    echo ERROR: Step 3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 4: Exporting to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table finops_summary
if %errorlevel% neq 0 (
    echo ERROR: Step 4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check the data_export folder for the final CSV file.
echo.
pause
