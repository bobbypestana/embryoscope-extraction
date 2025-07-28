@echo off
echo Starting combined gold loader (fixed)...
cd /d "%~dp0"
call conda activate try_request
python combined_gold_loader_fixed.py
pause 