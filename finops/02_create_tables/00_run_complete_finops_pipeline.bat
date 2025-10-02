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
python finops/02_create_tables/03_01_create_finops_summary.py
if %errorlevel% neq 0 (
    echo ERROR: Step 3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 4: Creating Patient Info Table
echo ========================================
python finops/02_create_tables/03_00_create_patient_info.py
if %errorlevel% neq 0 (
    echo ERROR: Step 4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 5: Creating Biopsy PGT-A Timeline Tables
echo ========================================
python finops/02_create_tables/03_02_biopsy_pgta_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step 5 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 6: Creating Embryo Timeline Tables
echo ========================================
python finops/02_create_tables/03_03_a_embryo_freeze_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step 6 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 7: Exporting Patient Info to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table patient_info
if %errorlevel% neq 0 (
    echo ERROR: Step 7 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 8: Exporting Resumed Biopsy PGT-A Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table resumed_biopsy_pgta_timeline
if %errorlevel% neq 0 (
    echo ERROR: Step 8 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 9: Exporting FinOps Summary to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table finops_summary
if %errorlevel% neq 0 (
    echo ERROR: Step 9 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 10: Exporting Comprehensive Embryo Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table comprehensive_embryo_timeline
if %errorlevel% neq 0 (
    echo ERROR: Step 10 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 11: Exporting Cryopreservation Events Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table cryopreservation_events_timeline
if %errorlevel% neq 0 (
    echo ERROR: Step 11 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check the data_export folder for the following CSV files:
echo   - gold_patient_info.csv
echo   - gold_resumed_biopsy_pgta_timeline.csv
echo   - gold_finops_summary.csv
echo   - gold_comprehensive_embryo_timeline.csv
echo   - gold_cryopreservation_events_timeline.csv
echo.
pause
