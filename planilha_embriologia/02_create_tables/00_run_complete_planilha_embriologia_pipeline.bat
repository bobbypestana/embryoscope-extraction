@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=7

echo ========================================
echo RUNNING COMPLETE PLANILHA EMBRIOLOGIA PIPELINE
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
echo STEP %PARENT_STEP%.1: Combining Embryoscope and Planilha Embriologia
echo ========================================
python planilha_embriologia/02_create_tables/01_combine_embryoscope_planilha.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    pause
    exit /b 1
)

@REM echo.
@REM echo ========================================
@REM echo STEP %PARENT_STEP%.2: Exporting Combined Table to Excel
@REM echo ========================================
@REM python planilha_embriologia/02_create_tables/02_export_to_excel.py
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step %PARENT_STEP%.2 failed
@REM     pause
@REM     exit /b 1
@REM )

echo.
echo ========================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check the data_output folder for the Excel export file.
echo Check the logs folder for execution details.
echo.
exit /b 0

