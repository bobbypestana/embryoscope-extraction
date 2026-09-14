@echo off
setlocal enabledelayedexpansion

echo ===============================================================================
echo     Huntington Data Lake - Dashboard Financeiro (Venda Direta)
echo ===============================================================================

echo [1/3] Activating conda environment 'try_request'...
call conda activate try_request
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to activate conda environment try_request.
    pause
    exit /b 1
)

echo [2/3] Generating Dashboard from gold.protheus_vendas_consolidadas...
python "%~dp0generate_venda_direta_dashboard.py"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Script execution failed. Check logs in logs/ folder.
    pause
    exit /b 1
)

echo [3/3] Opening dashboard in your default browser...
start "" "%~dp0dashboard_venda_direta.html"

echo.
echo ===============================================================================
echo Dashboard generated successfully!
echo ===============================================================================
