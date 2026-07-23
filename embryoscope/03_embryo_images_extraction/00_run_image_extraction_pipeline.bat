@echo off
REM ========================================
REM Embryo Image Extraction Pipeline
REM Runs Extraction Followed by Sync/Export
REM ========================================

REM Parse command line arguments
set LIMIT=%1
set PLANES=%2
set MODE=%3
set RETRY=%4

if "%LIMIT%"=="" set LIMIT=1000000
if "%PLANES%"=="" set PLANES=0
if "%MODE%"=="" set MODE=without_biopsy
if "%RETRY%"=="" set RETRY=True

echo ========================================
echo EMBRYO IMAGE EXTRACTION PIPELINE
echo ========================================
echo Limit: %LIMIT% (Default: 1,000,000)
echo Planes: %PLANES%
echo Mode: %MODE% (Default: with_biopsy)
echo Retry Failures: %RETRY% (Default: True)
echo.

REM Change to the batch file's directory
cd /d "%~dp0"

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request
if %errorlevel% neq 0 (
    echo ERROR: Failed to activate conda environment
    @REM pause (removed for automated execution)
    exit /b 1
)

echo.
echo ========================================
echo STEP 1: Extract Images from Embryoscope
echo ========================================
python 01_extract_embryo_images.py --limit %LIMIT% --planes %PLANES% --mode %MODE% --retry %RETRY%
if %errorlevel% neq 0 (
    echo ERROR: Step 1 Extraction failed
    @REM pause (removed for automated execution)
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Sync Logs and Export Metadata
echo ========================================
python 02_sync_and_export_metadata.py
if %errorlevel% neq 0 (
    echo ERROR: Step 2 Sync-Export failed
    @REM pause (removed for automated execution)
    exit /b 1
)

echo.
echo ========================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo Check extracted images in: export_images\
echo Check Excel reports in: export_images\
echo.
@REM pause (removed for automated execution)
exit /b 0
