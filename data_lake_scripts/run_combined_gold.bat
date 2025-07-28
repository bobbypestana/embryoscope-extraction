@echo off
echo ========================================
echo Running Combined Gold Loader
echo ========================================
echo.

REM Activate conda environment
call conda activate try_request
if errorlevel 1 (
    echo Error: Failed to activate conda environment 'try_request'
    pause
    exit /b 1
)

REM Change to the data_lake_scripts directory
cd /d "%~dp0"

REM Run the combined gold loader
echo Running combined gold loader...
python combined_gold_loader.py
if errorlevel 1 (
    echo Error: Combined gold loader failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo Combined Gold Loader completed successfully
echo ========================================
pause 