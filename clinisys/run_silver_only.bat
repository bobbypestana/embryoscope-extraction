@echo off
REM Move to clinisys folder
cd /d %~dp0

REM Activate the huntington environment
echo Activating huntington environment...
echo Activating huntington environment...
call conda activate huntington
if errorlevel 1 (
    echo ERROR: Failed to activate huntington environment
    echo Please ensure the environment exists: conda create -n huntington python=3.9
    pause
    exit /b 1
)

REM Run silver loader independently
echo Running silver loader...
powershell -Command "python silver_loader_ultra_robust.py"

echo Silver loader completed!
pause 