#!/usr/bin/env python3
import time
import datetime
import subprocess
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
now = datetime.datetime.now()
target = now.replace(hour=23, minute=0, second=0, microsecond=0)
diff = (target - now).total_seconds()

if diff > 0:
    print(f"[{now}] Sleeping for {diff:.1f} seconds until 23:00:00...", flush=True)
    time.sleep(diff)

print(f"[{datetime.datetime.now()}] 23:00 reached! Running pipeline...", flush=True)
subprocess.run([sys.executable, "run_pipeline_bh_scheduled.py"], cwd=SCRIPT_DIR)
