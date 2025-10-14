@echo off
echo ========================================
echo RUNNING MESCLADA DATA INGESTION PIPELINE
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
echo STEP 1: Mesclada to Bronze
echo ========================================
python finops/01_data_ingestion/03_01_mesclada_to_bronze.py
if %errorlevel% neq 0 (
    echo ERROR: Step 1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Mesclada Bronze to Silver
echo ========================================
python finops/01_data_ingestion/03_02_mesclada_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step 2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo MESCLADA PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check the logs folder for execution details.
echo.
pause
