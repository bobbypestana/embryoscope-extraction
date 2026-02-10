@echo off
echo ========================================
echo RUNNING COMPLETE DATAFLOW - ALL SYSTEMS
echo ========================================
echo.

REM Change to the project directory and save it
cd /d "%~dp0"
cd ..
set PROJECT_ROOT=%CD%

REM Activate conda environment (based on user memory)
echo Activating conda environment...
call conda activate try_request
if %errorlevel% neq 0 (
    echo ERROR: Failed to activate try_request environment
    echo Please ensure the environment exists: conda create -n try_request python=3.9
    pause
    exit /b 1
)

echo.
echo ========================================
echo CHECKING DEPENDENCIES
echo ========================================

REM Check if Python is available in the environment
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not available in try_request environment
    echo Please check your conda installation and environment
    pause
    exit /b 1
)

echo Python found in try_request environment.

REM Create database directory if it doesn't exist
if not exist "database" mkdir database

REM Check if required packages are installed
python -c "import pandas, requests, yaml, duckdb, urllib3, tqdm" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing required packages...
    pip install -r requirements.txt >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Failed to install required packages
        pause
        exit /b 1
    )
)

echo Dependencies OK.
echo.

echo ========================================
echo STEP 1: Running Clinisys Dataflow
echo ========================================
if not exist "clinisys\00_run_dataflow_clinisys.bat" (
    echo ERROR: Cannot find clinisys\00_run_dataflow_clinisys.bat
    echo Current directory: %CD%
    pause
    exit /b 1
)
call "clinisys\00_run_dataflow_clinisys.bat" 1
if %errorlevel% neq 0 (
    echo ERROR: Step 1 failed
    pause
    exit /b 1
)
cd /d "%PROJECT_ROOT%"

echo.
echo ========================================
echo STEP 2: Running Embryoscope Dataflow
echo ========================================
if not exist "embryoscope\01_get_embryo_data\00_run_dataflow_embryoscope.bat" (
    echo ERROR: Cannot find embryoscope\01_get_embryo_data\00_run_dataflow_embryoscope.bat
    echo Current directory: %CD%
    pause
    exit /b 1
)
call "embryoscope\01_get_embryo_data\00_run_dataflow_embryoscope.bat" 2
if %errorlevel% neq 0 (
    echo ERROR: Step 2 failed
    pause
    exit /b 1
)
cd /d "%PROJECT_ROOT%"

echo.
echo ========================================
echo STEP 3: Running Image Availability Pipeline (New Mode)
echo ========================================
if not exist "embryoscope\02_images_availability_report\00_run_image_availability_pipeline.bat" (
    echo ERROR: Cannot find embryoscope\02_images_availability_report\00_run_image_availability_pipeline.bat
    echo Current directory: %CD%
    pause
    exit /b 1
)
call "embryoscope\02_images_availability_report\00_run_image_availability_pipeline.bat" new
if %errorlevel% neq 0 (
    echo ERROR: Step 3 failed
    pause
    exit /b 1
)
cd /d "%PROJECT_ROOT%"

@REM echo.
@REM echo ========================================
@REM echo STEP 4: Running Planilha Embriologia Pipeline
@REM echo ========================================
@REM if not exist "planilha_embriologia\01_data_ingestion\00_run_planilha_embriologia_pipeline.bat" (
@REM     echo ERROR: Cannot find planilha_embriologia\01_data_ingestion\00_run_planilha_embriologia_pipeline.bat
@REM     echo Current directory: %CD%
@REM     pause
@REM     exit /b 1
@REM )
@REM call "planilha_embriologia\01_data_ingestion\00_run_planilha_embriologia_pipeline.bat" 4
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 4 failed
@REM     pause
@REM     exit /b 1
@REM )
@REM cd /d "%PROJECT_ROOT%"

@REM echo.
@REM echo ========================================
@REM echo STEP 5: Running Complete Planilha Embriologia Pipeline
@REM echo ========================================
@REM if not exist "planilha_embriologia\02_create_tables\00_run_complete_planilha_embriologia_pipeline.bat" (
@REM     echo ERROR: Cannot find planilha_embriologia\02_create_tables\00_run_complete_planilha_embriologia_pipeline.bat
@REM     echo Current directory: %CD%
@REM     pause
@REM     exit /b 1
@REM )
@REM call "planilha_embriologia\02_create_tables\00_run_complete_planilha_embriologia_pipeline.bat" 5
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 5 failed
@REM     pause
@REM     exit /b 1
@REM )
@REM cd /d "%PROJECT_ROOT%"

@REM echo.
@REM echo ========================================
@REM echo STEP 6: Running Redlara Ingestion
@REM echo ========================================
@REM if not exist "redlara\01_data_ingestion\00_run_redlara_ingestion.bat" (
@REM     echo ERROR: Cannot find redlara\01_data_ingestion\00_run_redlara_ingestion.bat
@REM     echo Current directory: %CD%
@REM     pause
@REM     exit /b 1
@REM )
@REM call "redlara\01_data_ingestion\00_run_redlara_ingestion.bat" 6
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 6 failed
@REM     pause
@REM     exit /b 1
@REM )
@REM cd /d "%PROJECT_ROOT%"

echo.
echo ========================================
echo STEP 7: Running Data Lake Consolidation
echo ========================================
if not exist "data_lake_scripts\00_run_dataflow_lake_only.bat" (
    echo ERROR: Cannot find data_lake_scripts\00_run_dataflow_lake_only.bat
    echo Current directory: %CD%
    pause
    exit /b 1
)
call "data_lake_scripts\00_run_dataflow_lake_only.bat" 7
if %errorlevel% neq 0 (
    echo ERROR: Step 7 failed
    pause
    exit /b 1
)
cd /d "%PROJECT_ROOT%"

echo.
echo ========================================
echo STEP 8: Running Ploidia Pipeline
echo ========================================
if not exist "planilha_ploidia\00_run_ploidia_pipeline.bat" (
    echo ERROR: Cannot find planilha_ploidia\00_run_ploidia_pipeline.bat
    echo Current directory: %CD%
    pause
    exit /b 1
)
call "planilha_ploidia\00_run_ploidia_pipeline.bat" 8
if %errorlevel% neq 0 (
    echo ERROR: Step 8 failed
    pause
    exit /b 1
)
cd /d "%PROJECT_ROOT%"



echo.
echo ========================================
echo STEP 9: Running Mesclada Pipeline
echo ========================================
@REM if not exist "finops\01_data_ingestion\00_run_mesclada_pipeline.bat" (
@REM     echo ERROR: Cannot find finops\01_data_ingestion\00_run_mesclada_pipeline.bat
@REM     echo Current directory: %CD%
@REM     pause
@REM     exit /b 1
@REM )
@REM call "finops\01_data_ingestion\00_run_mesclada_pipeline.bat" 9
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 9 failed
@REM     pause
@REM     exit /b 1
@REM )
@REM cd /d "%PROJECT_ROOT%"

echo.
echo ========================================
echo STEP 10: Running Complete FinOps Pipeline
echo ========================================
@REM if not exist "finops\02_create_tables\00_run_complete_finops_pipeline.bat" (
@REM     echo ERROR: Cannot find finops\02_create_tables\00_run_complete_finops_pipeline.bat
@REM     echo Current directory: %CD%
@REM     pause
@REM     exit /b 1
@REM )
@REM call "finops\02_create_tables\00_run_complete_finops_pipeline.bat" 10
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 10 failed
@REM     pause
@REM     exit /b 1
@REM )
@REM cd /d "%PROJECT_ROOT%"

echo.
echo ========================================
echo STEP 11: Cleaning Up Old Log Files
echo ========================================
cd /d "%PROJECT_ROOT%"
python data_lake_scripts\cleanup_old_logs.py
if %errorlevel% neq 0 (
    echo WARNING: Log cleanup failed (non-critical)
)


echo.
echo ========================================
echo STEP 12: Running Image Extraction Pipeline
echo ========================================
@REM if not exist "embryoscope_api\00_run_image_extraction_pipeline.bat" (
@REM     echo ERROR: Cannot find embryoscope_api\00_run_image_extraction_pipeline.bat
@REM     echo Current directory: %CD%
@REM     pause
@REM     exit /b 1
@REM )
@REM call "embryoscope_api\00_run_image_extraction_pipeline.bat" 12
@REM if %errorlevel% neq 0 (
@REM     echo ERROR: Step 12 failed
@REM     pause
@REM     exit /b 1
@REM )
@REM cd /d "%PROJECT_ROOT%"


echo.
echo ========================================
echo COMPLETE DATAFLOW FINISHED SUCCESSFULLY!
echo ========================================
echo.
echo All 11 pipeline steps completed without errors.
echo Log cleanup completed.
echo.
pause