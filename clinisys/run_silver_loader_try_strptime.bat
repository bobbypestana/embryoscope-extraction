@echo off
echo Starting Try_Strptime Silver Loader
echo ===================================

cd /d "%~dp0"

echo Current directory: %CD%
echo.

echo Activating Python environment...
call conda activate try_request
if errorlevel 1 (
    echo Failed to activate try_request environment
    echo Please ensure the environment exists: conda create -n try_request python=3.9
    pause
    exit /b 1
)

echo.
echo Running try_strptime silver loader...
python silver_loader_try_strptime.py

if errorlevel 1 (
    echo.
    echo ERROR: Try_strptime silver loader failed!
    echo Check the logs for details.
    pause
    exit /b 1
) else (
    echo.
    echo SUCCESS: Try_strptime silver loader completed successfully!
    echo Check the logs for processing details.
)

echo.
pause 