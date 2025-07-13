@echo off
echo Testing API Retry Logic...

REM Set the working directory to the parent directory
cd /d "%~dp0\.."

call conda activate try_request
python test\test_api_retries.py
pause 