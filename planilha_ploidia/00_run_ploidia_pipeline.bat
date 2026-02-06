@echo off
echo ========================================
echo RUNNING PLOIDIA DATA PIPELINE
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to planilha_ploidia directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    pause
    exit /b 1
)

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP 1: Creating Data Ploidia Table
echo ========================================
python 01_create_data_ploidia_table.py
if %errorlevel% neq 0 (
    echo ERROR: Step 1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Filling Missing Values
echo ========================================
python 02_fill_missing_values.py
if %errorlevel% neq 0 (
    echo ERROR: Step 2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 3: Joining Image Availability
echo ========================================
python 03_join_image_availability.py
if %errorlevel% neq 0 (
    echo ERROR: Step 3 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo PLOIDIA DATA PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in planilha_ploidia\logs\
echo.
exit /b 0
