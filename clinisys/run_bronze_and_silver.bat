@echo off
REM Move to clinisys folder
cd /d %~dp0

REM Activate the try_request environment
echo Activating try_request environment...
call conda activate try_request
if errorlevel 1 (
    echo ERROR: Failed to activate try_request environment
    echo Please ensure the environment exists: conda create -n try_request python=3.9
    pause
    exit /b 1
)

REM Run bronze loader (optional)
echo Running bronze loader...
powershell -Command "python bronze_loader.py"
if errorlevel 1 (
    echo WARNING: Bronze loader failed, but continuing with silver loader...
)

REM Run silver loader independently
echo Running silver loader...
powershell -Command "python silver_loader_ultra_robust.py"
if errorlevel 1 (
    echo ERROR: Silver loader failed
    pause
    exit /b 1
)

echo Both loaders completed successfully!
pause 