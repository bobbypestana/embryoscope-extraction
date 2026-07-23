@echo off
setlocal enabledelayedexpansion

:: Resolve script directories to run from any location
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."

echo ==============================================================================
echo HUNTINGTON PATIENT MATCHING PIPELINE
echo Working Directory: %CD%
echo Conda Environment : try_request
echo ==============================================================================
echo.

echo [1/4] Recreating the test database...
call conda run -n try_request python test_prontuario_matching/create_test_db.py
if !ERRORLEVEL! neq 0 (
    echo ERROR: Failed to recreate test database.
    exit /b !ERRORLEVEL!
)
echo.

echo [2/4] Running matching benchmark strategies (A to L)...
call conda run -n try_request python test_prontuario_matching/run_all_strategies.py
if !ERRORLEVEL! neq 0 (
    echo ERROR: Failed to run matching benchmark strategies.
    exit /b !ERRORLEVEL!
)
echo.

echo [3/4] Running production matching strategy (fills main prontuario)...
call conda run -n try_request python test_prontuario_matching/run_production_strategy_v1.py
if !ERRORLEVEL! neq 0 (
    echo ERROR: Failed to run production matching strategy.
    exit /b !ERRORLEVEL!
)
echo.

echo [4/4] Running false positive validation tests...
call conda run -n try_request python test_prontuario_matching/run_validation_tests.py
if !ERRORLEVEL! neq 0 (
    echo ERROR: Failed to run false positive tests.
    exit /b !ERRORLEVEL!
)
echo.

echo ==============================================================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ==============================================================================
@REM pause (removed for automated execution)
