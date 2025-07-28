@echo off
echo Starting PatientID analysis against clinisys_all.view_pacientes...
echo.

REM Change to the correct directory and activate conda environment
cd /d "G:\My Drive\projetos_individuais\Huntington\data_lake_scripts"
call conda activate try_request

REM Run the analysis
python check_patientid_in_clinisys_pacientes.py

echo.
echo Analysis completed.
pause 