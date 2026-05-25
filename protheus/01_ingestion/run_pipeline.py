#!/usr/bin/env python3
"""
Protheus Ingestion Pipeline Orchestrator
Executes Bronze, Silver, and Gold scripts sequentially and logs the results.
"""

import os
import subprocess
import sys
import logging
from datetime import datetime

# Setup logging standard
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'run_pipeline_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    logger.info(f"Starting script: {script_name}...")
    
    # We run using the same python executable
    res = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    
    if res.returncode == 0:
        logger.info(f"Script {script_name} completed successfully.")
        # Log stdout/stderr summaries
        for line in res.stdout.split('\n')[-10:]:
            if line:
                logger.info(f"  [STDOUT] {line}")
    else:
        logger.error(f"Script {script_name} failed with exit code {res.returncode}.")
        logger.error(f"Error output:\n{res.stderr}")
        sys.exit(res.returncode)

def main():
    logger.info("==============================================")
    logger.info("=== PROTHEUS PIPELINE ORCHESTRATION STARTED ===")
    logger.info("==============================================")
    
    # Step 1: Bronze Ingestion
    # Note: Since the initial backfill is already running, this script is ready for future runs
    run_script("01_source_to_bronze.py")
    
    # Step 2: Silver Promotion
    run_script("02_bronze_to_silver.py")
    
    # Step 3: Gold Promotion
    run_script("03_silver_to_gold.py")
    
    logger.info("==============================================")
    logger.info("=== PROTHEUS PIPELINE ORCHESTRATION FINISHED ===")
    logger.info("==============================================")

if __name__ == "__main__":
    main()
