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

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Combining Redlara and Planilha Data
echo ========================================
python 02_combine_redlara_planilha.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Combining Embryoscope and Planilha Data
echo ========================================
python 03_combine_embryoscope_planilha.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 failed
    pause
    exit /b 1
)

@REM echo.
@REM echo ========================================
@REM echo STEP %PARENT_STEP%.4: Exporting Redlara-Planilha Table to Excel
@REM echo ========================================
@REM python 04_01_export_redlara_planilha.py
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step %PARENT_STEP%.4 failed
@REM     pause
@REM     exit /b 1
@REM )

@REM echo.
@REM echo ========================================
@REM echo STEP %PARENT_STEP%.5: Exporting Final Combined Table to Excel
@REM echo ========================================
@REM python 04_02_export_clinsisy_embryoscope_planilha.py
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step %PARENT_STEP%.5 failed
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