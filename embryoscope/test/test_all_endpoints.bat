@echo off
echo Testing All Endpoints with Generic Processing...
echo.

REM Set the working directory to the parent directory
cd /d "%~dp0\.."

REM Activate conda environment
call conda activate try_request

REM Run the test script
python test\test_all_endpoints.py

echo.
echo Test completed.
pause 