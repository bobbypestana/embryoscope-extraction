@echo off
echo Testing Database Connection Manager...

REM Set the working directory to the parent directory
cd /d "%~dp0\.."

call conda activate try_request
python test\test_connection_manager.py
pause 