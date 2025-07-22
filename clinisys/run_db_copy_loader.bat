@echo off
echo Starting Database Copy Loader
echo =============================

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
echo Running database copy loader...
python db_copy_loader.py

if errorlevel 1 (
    echo.
    echo ERROR: Database copy loader failed!
    echo Check the logs for details.
    pause
    exit /b 1
) else (
    echo.
    echo SUCCESS: Database copy loader completed successfully!
    echo Check the logs for details.
)

echo.
pause 