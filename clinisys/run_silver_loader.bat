@echo off
REM Activate the conda environment and run the silver loader using PowerShell
set CONDA_BASE=%USERPROFILE%\anaconda3
call "%CONDA_BASE%\Scripts\activate.bat"
powershell -Command "conda activate try_request; python clinisys/silver_loader.py" 