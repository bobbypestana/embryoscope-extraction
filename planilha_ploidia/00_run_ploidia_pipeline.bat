@echo off
echo ========================================
echo RUNNING PLOIDIA DATA PIPELINE
echo ========================================
echo.

REM Change to the batch file's directory
cd /d "%~dp0"
if %errorlevel% neq 0 (
    echo ERROR: Failed to change to planilha_ploidia directory
    echo Current directory: %CD%
    echo Batch file path: %~dp0
    @REM pause (removed for automated execution)
    exit /b 1
)

REM Activate conda environment
echo Activating conda environment...
call conda activate try_request

echo.
echo ========================================
echo BUILDING PESQUISA_DADOS_PARA_IA (CONSOLIDATED)
echo ========================================
python 01_create_data_ploidia_table.py
if %errorlevel% neq 0 (
    echo ERROR: Failed to build pesquisa_dados_para_ia
    @REM pause (removed for automated execution)
    exit /b 1
)

echo.
echo ========================================
echo PLOIDIA DATA PIPELINE COMPLETED SUCCESSFULLY!
echo ========================================
echo.
echo All steps completed without errors in a single consolidated pass.
echo Check logs in planilha_ploidia\logs\
echo.
exit /b 0
