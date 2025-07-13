@echo off
echo Testing Patients Extraction and Save...

REM Set the working directory to the parent directory
cd /d "%~dp0\.."

call conda activate try_request
python test\test_patients_save.py
pause 