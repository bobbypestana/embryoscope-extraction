@echo off
echo Starting view_pacientes table fix...

REM Change to the clinisys directory
cd /d "%~dp0"

REM Activate conda environment
call conda activate try_request

REM Run the fix script
python fix_view_pacientes.py

echo Fix completed.
pause
