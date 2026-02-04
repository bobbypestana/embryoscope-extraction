@echo off
REM ========================================
REM Embryo Image Extraction Pipeline
REM Runs Extraction Followed by Sync/Export
REM ========================================

REM Parse command line arguments
set LIMIT=%1
set PLANES=%2
set MODE=%3

if "%LIMIT%"=="" set LIMIT=1000000
if "%PLANES%"=="" set PLANES=0
if "%MODE%"=="" set MODE=with_biopsy

echo ========================================
echo EMBRYO IMAGE EXTRACTION PIPELINE
echo ========================================
echo Limit: %LIMIT% (Default: 1,000,000)
echo Planes: %PLANES%
echo Mode: %MODE% (Default: with_biopsy)
echo.

REM Change to the batch file's directory
cd /d "%~dp0"

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request
if %errorlevel% neq 0 (
    echo ERROR: Failed to activate conda environment
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 1: Extract Images from Embryoscope
echo ========================================
python 01_extract_embryo_images.py --limit %LIMIT% --planes %PLANES% --mode %MODE%
if %errorlevel% neq 0 (
    echo ERROR: Step 1 Extraction failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo STEP 2: Sync Logs and Export Metadata
echo ========================================
python 02_sync_and_export_metadata.py
if %errorlevel% neq 0 (
    echo ERROR: Step 2 Sync-Export failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo Check extracted images in: export_images\
echo Check Excel reports in: export_images\
echo.
pause
exit /b 0
