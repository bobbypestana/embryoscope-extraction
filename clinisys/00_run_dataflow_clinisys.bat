@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=1

REM ========================================
REM VPN CONFIGURATION (Sophos Connect)
REM ========================================
REM Only manage VPN if not running as part of the complete dataflow
set "VPN_NAME=189.108.75.147"
set "SOPHOS_CLI=C:\Program Files (x86)\Sophos\Connect\sccli.exe"
set "EXIT_CODE=0"

if "%PROJECT_ROOT%"=="" if "%VPN_NAME%" neq "" (
    echo.
    echo Connecting to Sophos VPN: %VPN_NAME%...
    if exist "%SOPHOS_CLI%" (
        "%SOPHOS_CLI%" enable -n "%VPN_NAME%"
        echo Waiting 5 seconds for VPN to establish...
        timeout /t 5 >nul
    ) else (
        echo WARNING: Sophos Connect CLI not found at "%SOPHOS_CLI%"
    )
)

echo ========================================
echo RUNNING COMPLETE CLINISYS DATAFLOW
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to clinisys directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    @REM pause (removed for automated execution)
    set "EXIT_CODE=1"
    goto cleanup
)

REM Activate conda environment (based on user memory)
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo STEP %PARENT_STEP%.1: Extract from API to Bronze
echo ========================================
python 01_source_to_bronze.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.1 failed
    @REM pause (removed for automated execution)
    set "EXIT_CODE=1"
    goto cleanup
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.2: Bronze to Silver
echo ========================================
python 02_01_bronze_to_silver.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.2 failed
    @REM pause (removed for automated execution)
    set "EXIT_CODE=1"
    goto cleanup
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Compare Bronze and Silver Tables
echo ========================================
python 02_02_compare_bronze_and_silver.py
if %errorlevel% neq 0 (
    echo WARNING: Step %PARENT_STEP%.3 had issues, but continuing...
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.4: Silver to Gold
echo ========================================
python 03_silver_to_gold.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.4 failed
    @REM pause (removed for automated execution)
    set "EXIT_CODE=1"
    goto cleanup
)

echo.
echo ========================================
echo DATAFLOW COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in clinisys\logs\
echo.

:cleanup
if "%PROJECT_ROOT%"=="" if "%VPN_NAME%" neq "" (
    echo.
    echo Disconnecting from Sophos VPN: %VPN_NAME%...
    if exist "%SOPHOS_CLI%" (
        "%SOPHOS_CLI%" disable -n "%VPN_NAME%"
    )
)
exit /b %EXIT_CODE% 