@echo off
REM Batch file to consolidate embryoscope DBs into the central data lake

cd /d "%~dp0"

REM Activate the try_request environment
call conda activate try_request
if errorlevel 1 (
    echo ERROR: Failed to activate try_request environment
    pause
    exit /b 1
)

REM Run the consolidation script
python consolidate_embryoscope_dbs.py

REM Pause for user review
pause 