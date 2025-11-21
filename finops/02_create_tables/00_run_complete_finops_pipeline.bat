@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=5

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
echo STEP %PARENT_STEP%.1: Creating All Patient Timeline
echo ========================================
python finops/02_create_tables/01_create_all_patient_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Creating Clean Timeline View
echo ========================================
python finops/02_create_tables/02_create_clean_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Creating FinOps Summary Table
echo ========================================
python finops/02_create_tables/03_01_create_finops_summary.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.4: Creating Patient Info Table
echo ========================================
python finops/02_create_tables/03_00_create_patient_info.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.4 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.5: Creating Biopsy PGT-A Timeline Tables
echo ========================================
python finops/02_create_tables/03_02_biopsy_pgta_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.5 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.6: Creating Embryoscope Timeline Tables
echo ========================================
python finops/02_create_tables/03_03_embryoscope_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.6 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.7: Creating Embryo Timeline Tables
echo ========================================
python finops/02_create_tables/03_04_a_embryo_freeze_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.7 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.8: Creating Cryopreservation Events Timeline Tables
echo ========================================
python finops/02_create_tables/03_04_b_cryopreservation_events_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.8 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.9: Creating Consultation Timeline Tables
echo ========================================
python finops/02_create_tables/03_05_consultas_timeline.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.9 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.10: Exporting All Patients Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table all_patients_timeline --prefix "01_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.10 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.11: Exporting Patient Info to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table patient_info --prefix "03_00_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.11 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.12: Exporting Resumed Biopsy PGT-A Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table resumed_biopsy_pgta_timeline --prefix "03_02_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.12 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.13: Exporting Embryoscope Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table embryoscope_timeline --prefix "03_03_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.13 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.14: Exporting Resumed Embryoscope Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table resumed_embryoscope_timeline --prefix "03_03_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.14 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.15: Exporting FinOps Summary to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table finops_summary --prefix "03_01_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.15 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.16: Exporting Resumed Cryopreservation Events Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table resumed_cryopreservation_events_timeline --prefix "03_04_b_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.16 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.17: Exporting Consultation Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table consultas_timeline --prefix "03_05_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.17 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.18: Exporting Resumed Consultation Timeline to CSV
echo ========================================
python finops/02_create_tables/04_export_table_to_csv.py --schema gold --table resumed_consultas_timeline --prefix "03_05_"
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.18 failed
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
echo   - 01_gold_all_patients_timeline.csv
echo   - 03_00_gold_patient_info.csv
echo   - 03_02_gold_resumed_biopsy_pgta_timeline.csv
echo   - 03_03_gold_embryoscope_timeline.csv
echo   - 03_03_gold_resumed_embryoscope_timeline.csv
echo   - 03_01_gold_finops_summary.csv
echo   - 03_04_b_gold_resumed_cryopreservation_events_timeline.csv
echo   - 03_05_gold_consultas_timeline.csv
echo   - 03_05_gold_resumed_consultas_timeline.csv
echo.
exit /b 0
