@echo off
REM Activate the conda environment and run the bronze loader using PowerShell

REM Set the path to your Anaconda installation if needed
set CONDA_BASE=%USERPROFILE%\anaconda3

REM Initialize conda for PowerShell
call "%CONDA_BASE%\Scripts\activate.bat"

REM Activate the try_request environment and run the loader
powershell -Command "conda activate try_request; python clinisys/bronze_loader.py" 