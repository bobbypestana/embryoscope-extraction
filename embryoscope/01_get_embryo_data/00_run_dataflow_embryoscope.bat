@echo off
set PARENT_STEP=%1
if "%PARENT_STEP%"=="" set PARENT_STEP=2

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
echo RUNNING COMPLETE EMBRYOSCOPE DATAFLOW
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to 01_get_embryo_data directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    pause
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
    pause
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
    pause
    set "EXIT_CODE=1"
    goto cleanup
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.3: Cleanup Silver Layer
echo ========================================
python 02_02_cleanup_silver_layer.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.3 failed
    pause
    set "EXIT_CODE=1"
    goto cleanup
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.4: Consolidate Embryoscope Databases
echo ========================================
python 02_03_consolidate_embryoscope_dbs.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.4 failed
    pause
    set "EXIT_CODE=1"
    goto cleanup
)

echo.
echo ========================================
echo STEP %PARENT_STEP%.5: Consolidated to Gold
echo ========================================
python 03_consolidated_to_gold.py
if %errorlevel% neq 0 (
    echo ERROR: Step %PARENT_STEP%.5 failed
    pause
    set "EXIT_CODE=1"
    goto cleanup
)


echo.
echo ========================================
echo DATAFLOW COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors.
echo Check logs in embryoscope\logs\
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