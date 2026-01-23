@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=6

echo ========================================
echo RUNNING PLANILHA EMBRIOLOGIA DATA INGESTION PIPELINE
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
echo STEP %PARENT_STEP%.1: Planilha Embriologia to Bronze
echo ========================================
python planilha_embriologia/01_data_ingestion/01_planilha_embriologia_to_bronze.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Planilha Embriologia Bronze to Silver
echo ========================================
python planilha_embriologia/01_data_ingestion/02_planilha_embriologia_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo PLANILHA EMBRIOLOGIA PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check the logs folder for execution details.
echo.
exit /b 0

