@echo off
setlocal

if "%~1"=="" (
    echo =======================================================
    echo ERROR: Missing Input File
    echo =======================================================
    echo Usage: recover_db.bat ^<path_to_pdb_file^>
    echo Example: recover_db.bat "files\D2025.12.14_S04489_I3027_P.pdb"
    exit /b 1
)

set "PDB_FILE=%~1"

echo =======================================================
echo Embryoscope Database Recovery Pipeline
echo Target File: %PDB_FILE%
echo =======================================================
echo.

echo -------------------------------------------------------
echo [Step 1/3] Official Extraction ^& RowID Jumping
echo -------------------------------------------------------
call conda run --no-capture-output -n embryo_extractor python extract_images.py "%PDB_FILE%"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] extract_images.py encountered a fatal error.
    exit /b %ERRORLEVEL%
)
echo.

echo -------------------------------------------------------
echo [Step 2/3] Binary Carver (Forensic Image Sweep)
echo -------------------------------------------------------
call conda run --no-capture-output -n embryo_extractor python binary_carver.py "%PDB_FILE%"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] binary_carver.py encountered a fatal error.
    exit /b %ERRORLEVEL%
)
echo.

echo -------------------------------------------------------
echo [Step 3/3] Metadata Carver (GENERAL ^& Blastomere CSVs)
echo -------------------------------------------------------
call conda run --no-capture-output -n embryo_extractor python metadata_carver.py "%PDB_FILE%"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] metadata_carver.py encountered a fatal error.
    exit /b %ERRORLEVEL%
)
echo.

echo =======================================================
echo Pipeline Completed Successfully! All available data has
echo been rescued to the 'extracted_images' folder.
echo =======================================================
endlocal
