#!/usr/bin/env python3
"""
Scheduled BH Pipeline Runner:
1. Completes bronze_bh.venda_direta force backfill (from 2022 to present).
2. Promotes all conformed tables to Silver (02_bronze_to_silver.py).
3. Rebuilds Gold models (03_silver_to_gold.py) with updated direct sales dates.
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'scheduled_bh_run_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PYTHON_EXE = sys.executable

def run_step(desc, cmd):
    logger.info(f"=== STARTING STEP: {desc} ===")
    logger.info(f"Command: {' '.join(cmd)}")
    t0 = datetime.now()
    res = subprocess.run(cmd, cwd=SCRIPT_DIR)
    dt = (datetime.now() - t0).total_seconds()
    if res.returncode != 0:
        logger.error(f"Step '{desc}' FAILED with code {res.returncode} after {dt:.1f}s")
        return False
    logger.info(f"Step '{desc}' SUCCEEDED in {dt:.1f}s")
    return True

def main():
    logger.info("=== SCHEDULED BH PIPELINE RUNNER STARTED ===")
    
    # Step 1: Bronze venda_direta backfill
    s1 = run_step(
        "Ingest bronze_bh.venda_direta (Force Backfill)",
        [PYTHON_EXE, "01_source_to_bronze.py", "--instance", "bh", "--table", "venda_direta", "--force-backfill"]
    )
    if not s1:
        logger.error("Aborting subsequent steps due to Bronze backfill failure.")
        sys.exit(1)

    # Step 2: Silver Promotion
    s2 = run_step(
        "Promote Bronze to Silver (Huntington + BH)",
        [PYTHON_EXE, "02_bronze_to_silver.py"]
    )
    if not s2:
        logger.error("Aborting Gold consolidation due to Silver promotion failure.")
        sys.exit(1)

    # Step 3: Gold Consolidation
    s3 = run_step(
        "Consolidate Silver to Gold",
        [PYTHON_EXE, "03_silver_to_gold.py"]
    )
    if not s3:
        logger.error("Gold consolidation completed with errors.")
        sys.exit(1)

    logger.info("=== ALL SCHEDULED STEPS COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    main()
