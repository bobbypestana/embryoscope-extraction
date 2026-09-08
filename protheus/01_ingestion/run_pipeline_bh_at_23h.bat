@echo off
echo =======================================================
echo Waiting until 23:00 to run BH Pipeline...
echo =======================================================
C:\Users\FilipeFurlanBellotti\anaconda3\envs\try_request\python.exe -c "import time, datetime; now = datetime.datetime.now(); target = now.replace(hour=23, minute=0, second=0, microsecond=0); diff = (target - now).total_seconds(); diff = diff if diff > 0 else 0; print(f'Sleeping for {diff:.0f} seconds until 23:00...'); time.sleep(diff)"
echo 23:00 reached! Launching BH Pipeline...
cd /d "g:\My Drive\projetos_individuais\Huntington\protheus\01_ingestion"
C:\Users\FilipeFurlanBellotti\anaconda3\envs\try_request\python.exe run_pipeline_bh_scheduled.py
pause
