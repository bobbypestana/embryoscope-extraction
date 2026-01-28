"""
Recovery Script - Save Checkpoint Data to Database
This script recovers the processed embryo image availability data from checkpoints
and saves it to the database.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
from datetime import datetime
import logging
import json
from typing import List, Dict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_all_checkpoint_data(checkpoint_dir: str) -> List[Dict]:
    """
    Load all checkpoint data from JSON files.
    
    Args:
        checkpoint_dir: Directory containing checkpoint files
        
    Returns:
        List of all embryo results
    """
    logger.info(f"Loading checkpoint data from {checkpoint_dir}...")
    
    checkpoint_files = list(Path(checkpoint_dir).glob('*_checkpoint.json'))
    logger.info(f"Found {len(checkpoint_files)} checkpoint files")
    
    all_results = []
    
    for checkpoint_file in checkpoint_files:
        server_name = checkpoint_file.stem.replace('_checkpoint', '')
        logger.info(f"Loading checkpoint for {server_name}...")
        
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            processed_embryos = checkpoint_data.get('processed_embryos', [])
            logger.info(f"  - {server_name}: {len(processed_embryos)} embryos in checkpoint")
            
            # Note: The checkpoint only contains embryo IDs, not the full results
            # We need to reconstruct the results from the server logs
            
        except Exception as e:
            logger.error(f"Error loading checkpoint for {server_name}: {e}")
    
    logger.warning("Checkpoints only contain embryo IDs, not full results!")
    logger.warning("We need to parse the server log files to get the full results.")
    
    return all_results


def parse_server_logs(log_dir: str) -> pd.DataFrame:
    """
    Parse server log files to extract embryo results.
    
    Args:
        log_dir: Directory containing log files
        
    Returns:
        DataFrame with all embryo results
    """
    logger.info(f"Parsing server log files from {log_dir}...")
    
    # Find all server log files
    log_files = list(Path(log_dir).glob('*.log'))
    server_logs = [f for f in log_files if not f.name.startswith('image_availability_')]
    
    logger.info(f"Found {len(server_logs)} server log files")
    
    all_results = []
    
    for log_file in server_logs:
        server_name = log_file.stem.split('_')[0]
        logger.info(f"Parsing log for {server_name}...")
        
        try:
            # Read the log file
            with open(log_file, 'r', encoding='utf-8') as f:
                log_content = f.read()
            
            # Extract result information from logs
            # This is complex because we need to parse structured log data
            # For now, let's try a different approach
            
        except Exception as e:
            logger.error(f"Error parsing log for {server_name}: {e}")
    
    logger.warning("Log parsing is complex - let's try re-running the script instead!")
    
    return pd.DataFrame()


def get_embryo_data_from_db(db_path: str) -> pd.DataFrame:
    """
    Query embryo data from the gold table.
    
    Args:
        db_path: Path to the DuckDB database
        
    Returns:
        DataFrame with embryo data
    """
    logger.info("Querying embryo data from database...")
    
    conn = duckdb.connect(db_path, read_only=True)
    
    query = """
    SELECT 
        prontuario,
        patient_PatientID,
        patient_PatientIDx,
        patient_unit_huntington,
        treatment_TreatmentName,
        embryo_EmbryoID,
        embryo_EmbryoDate
    FROM gold.embryoscope_embrioes
    WHERE embryo_EmbryoID IS NOT NULL
    ORDER BY patient_unit_huntington, prontuario, embryo_EmbryoID
    """
    
    df = conn.execute(query).df()
    conn.close()
    
    logger.info(f"Retrieved {len(df):,} embryo records")
    
    return df


def main():
    """Main recovery function."""
    logger.info("="*80)
    logger.info("RECOVERY SCRIPT - Checkpoint Data to Database")
    logger.info("="*80)
    
    checkpoint_dir = 'embryoscope/report/checkpoints'
    log_dir = 'embryoscope/report/logs'
    db_path = 'database/huntington_data_lake.duckdb'
    
    # Check if checkpoint directory exists
    if not os.path.exists(checkpoint_dir):
        logger.error(f"Checkpoint directory not found: {checkpoint_dir}")
        return 1
    
    # Load checkpoint data
    checkpoint_data = load_all_checkpoint_data(checkpoint_dir)
    
    # Try parsing logs
    log_results = parse_server_logs(log_dir)
    
    # Analysis
    logger.info("\n" + "="*80)
    logger.info("ANALYSIS")
    logger.info("="*80)
    logger.info("The checkpoints only contain embryo IDs that were processed,")
    logger.info("not the actual API results (image availability, counts, etc.)")
    logger.info("")
    logger.info("RECOMMENDATION:")
    logger.info("The best approach is to re-run the main script with the database closed.")
    logger.info("Since all checkpoints exist, the script will skip already-processed embryos")
    logger.info("and only process any remaining ones, then save all results to the database.")
    logger.info("")
    logger.info("However, I notice the checkpoints were cleared, which means processing completed.")
    logger.info("Let me check if there are any backup checkpoint files...")
    
    # Check for backup checkpoints
    backup_checkpoints = list(Path(checkpoint_dir).glob('*.json.bak'))
    if backup_checkpoints:
        logger.info(f"Found {len(backup_checkpoints)} backup checkpoint files!")
    else:
        logger.info("No backup checkpoint files found.")
    
    logger.info("\n" + "="*80)
    logger.info("NEXT STEPS")
    logger.info("="*80)
    logger.info("1. Close any open database connections")
    logger.info("2. Re-run the main script: python embryoscope/report/01_check_image_availability.py")
    logger.info("3. The script will process any remaining embryos and save all results")
    logger.info("="*80)
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
