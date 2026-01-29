"""
Check Image Availability for Embryos
This script queries embryo data from gold.embryoscope_embrioes and checks
if images are available on each server via the API.

Features:
- Checkpoint/resume capability - can restart from last saved point
- Parallel processing - processes each server in parallel
- Rate limiting - 10 requests/second per server
- Auto re-authentication on token expiration
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
from typing import Dict, List, Optional
import time
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

from utils.api_client import EmbryoscopeAPIClient
from utils.config_manager import EmbryoscopeConfigManager


# Setup logging
def setup_logging():
    """Setup logging configuration."""
    log_dir = 'embryoscope/report/logs'
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'image_availability_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# Checkpoint management
CHECKPOINT_DIR = 'embryoscope/report/checkpoints'
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def save_checkpoint(server_name: str, processed_embryos: List[str]):
    """Save checkpoint for a server."""
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f'{server_name}_checkpoint.json')
    checkpoint_data = {
        'server': server_name,
        'processed_embryos': processed_embryos,
        'last_updated': datetime.now().isoformat()
    }
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    logger.debug(f"Checkpoint saved for {server_name}: {len(processed_embryos)} embryos")


def load_checkpoint(server_name: str) -> set:
    """Load checkpoint for a server."""
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f'{server_name}_checkpoint.json')
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        processed = set(checkpoint_data.get('processed_embryos', []))
        logger.info(f"Loaded checkpoint for {server_name}: {len(processed)} embryos already processed")
        return processed
    return set()


def clear_checkpoint(server_name: str):
    """Clear checkpoint for a server after successful completion."""
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f'{server_name}_checkpoint.json')
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        logger.info(f"Checkpoint cleared for {server_name}")


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
    
    logger.info(f"Retrieved {len(df):,} embryo records from {df['patient_unit_huntington'].nunique()} units")
    
    return df


def check_image_availability(
    client: EmbryoscopeAPIClient,
    embryo_id: str,
    max_retries: int = 3
) -> Dict[str, any]:
    """
    Check if images are available for a specific embryo.
    
    Args:
        client: API client instance
        embryo_id: Embryo ID to check
        max_retries: Maximum number of retries on failure
        
    Returns:
        Dictionary with availability status and metadata
    """
    result = {
        'embryo_id': embryo_id,
        'image_available': False,
        'image_runs_count': 0,
        'api_response_status': 'unknown',
        'error_message': None,
        'checked_at': datetime.now()
    }
    
    for attempt in range(max_retries):
        try:
            # Try to get image runs for this embryo
            response = client.get_image_runs(embryo_id)
            
            if response is None:
                result['api_response_status'] = 'no_response'
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                else:
                    result['error_message'] = 'No response from API after retries'
                    break
            
            # Check if ImageRuns exists in response
            if 'ImageRuns' in response:
                image_runs = response['ImageRuns']
                if isinstance(image_runs, list):
                    result['image_runs_count'] = len(image_runs)
                    result['image_available'] = len(image_runs) > 0
                    result['api_response_status'] = 'success'
                else:
                    result['api_response_status'] = 'invalid_format'
                    result['error_message'] = 'ImageRuns is not a list'
            else:
                result['api_response_status'] = 'missing_field'
                result['error_message'] = 'ImageRuns field not in response'
            
            break  # Success, exit retry loop
            
        except Exception as e:
            result['api_response_status'] = 'error'
            result['error_message'] = str(e)
            
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            break
    
    return result


def process_server_embryos(server_name: str, embryos_data: List[Dict], config_dict: Dict) -> List[Dict]:
    """
    Process all embryos for a specific server.
    This function runs in a separate process.
    
    Args:
        server_name: Name of the server/unit
        embryos_data: List of embryo dictionaries for this server
        config_dict: Configuration dictionary for this server
        
    Returns:
        List of results for each embryo
    """
    # Setup logging for this process
    process_logger = logging.getLogger(f"server_{server_name}")
    process_logger.setLevel(logging.INFO)
    
    # Add handler if not already present
    if not process_logger.handlers:
        log_file = os.path.join('embryoscope/report/logs', f'{server_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        process_logger.addHandler(fh)
        
        # Also add console handler
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        process_logger.addHandler(ch)
    
    process_logger.info(f"{'='*60}")
    process_logger.info(f"Processing server: {server_name}")
    process_logger.info(f"Total embryos: {len(embryos_data):,}")
    process_logger.info(f"{'='*60}")
    
    # Load checkpoint
    processed_embryo_ids = load_checkpoint(server_name)
    
    # Filter out already processed embryos
    embryos_to_process = [e for e in embryos_data if e['embryo_EmbryoID'] not in processed_embryo_ids]
    
    if len(embryos_to_process) < len(embryos_data):
        process_logger.info(f"Resuming from checkpoint: {len(embryos_to_process):,} embryos remaining")
    
    # Initialize API client with 10 req/s rate limit (0.1s delay)
    rate_limit_delay = 0.1  # 10 requests per second
    client = EmbryoscopeAPIClient(server_name, config_dict, rate_limit_delay=rate_limit_delay)
    
    # Authenticate
    if not client.authenticate():
        process_logger.error(f"Failed to authenticate with {server_name}")
        return []
    
    results = []
    checkpoint_interval = 100  # Save checkpoint every 100 embryos
    
    for idx, embryo_data in enumerate(embryos_to_process):
        embryo_id = embryo_data['embryo_EmbryoID']
        
        # Log progress every 100 embryos
        if (idx + 1) % 100 == 0:
            total_processed = len(processed_embryo_ids) + idx + 1
            total = len(embryos_data)
            process_logger.info(f"Progress: {total_processed:,}/{total:,} ({total_processed/total*100:.1f}%)")
        
        # Check image availability
        result = check_image_availability(client, embryo_id)
        
        # Add embryo data to result
        result.update(embryo_data)
        
        results.append(result)
        processed_embryo_ids.add(embryo_id)
        
        # Save checkpoint periodically
        if (idx + 1) % checkpoint_interval == 0:
            save_checkpoint(server_name, list(processed_embryo_ids))
        
        # Refresh token periodically (every 1000 embryos)
        if (idx + 1) % 1000 == 0:
            process_logger.info("Refreshing authentication token...")
            if not client.refresh_token_if_needed():
                process_logger.warning("Token refresh failed, attempting re-authentication...")
                if not client.authenticate():
                    process_logger.error("Re-authentication failed, stopping processing for this server")
                    # Save checkpoint before stopping
                    save_checkpoint(server_name, list(processed_embryo_ids))
                    break
    
    # Save final checkpoint
    save_checkpoint(server_name, list(processed_embryo_ids))
    
    process_logger.info(f"Completed {server_name}: {len(results):,} embryos processed in this run")
    
    # Clear checkpoint on successful completion
    if len(embryos_to_process) == len(results):
        clear_checkpoint(server_name)
    
    return results


def save_results_to_db(results_df: pd.DataFrame, db_path: str, table_name: str = 'gold.embryo_image_availability_raw'):
    """
    Save results to the database as a raw table with all original columns + status.
    
    Args:
        results_df: DataFrame with results
        db_path: Path to the DuckDB database
        table_name: Name of the table to create/replace
    """
    logger.info(f"Saving results to {table_name}...")
    
    # Ensure columns are in the right order: original columns first, then status columns
    column_order = [
        'prontuario',
        'patient_PatientID',
        'patient_PatientIDx',
        'patient_unit_huntington',
        'treatment_TreatmentName',
        'embryo_EmbryoID',
        'embryo_EmbryoDate',
        'image_available',
        'image_runs_count',
        'api_response_status',
        'error_message',
        'checked_at'
    ]
    
    # Reorder columns
    results_df = results_df[column_order]
    
    conn = duckdb.connect(db_path)
    
    # Create or replace the raw table
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS gold")
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM results_df")
    
    # Get summary statistics
    summary = conn.execute(f"""
        SELECT 
            patient_unit_huntington,
            COUNT(*) as total_embryos,
            SUM(CASE WHEN image_available THEN 1 ELSE 0 END) as with_images,
            SUM(CASE WHEN NOT image_available THEN 1 ELSE 0 END) as without_images,
            ROUND(AVG(CASE WHEN image_available THEN 1.0 ELSE 0.0 END) * 100, 2) as pct_with_images,
            COUNT(DISTINCT api_response_status) as distinct_statuses
        FROM {table_name}
        GROUP BY patient_unit_huntington
        ORDER BY patient_unit_huntington
    """).df()
    
    logger.info("\n" + "="*60)
    logger.info("SUMMARY BY SERVER:")
    logger.info("="*60)
    print(summary.to_string(index=False))
    
    # Get status breakdown
    status_summary = conn.execute(f"""
        SELECT 
            api_response_status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM {table_name}
        GROUP BY api_response_status
        ORDER BY count DESC
    """).df()
    
    logger.info("\n" + "="*60)
    logger.info("API RESPONSE STATUS BREAKDOWN:")
    logger.info("="*60)
    print(status_summary.to_string(index=False))
    
    conn.close()
    logger.info(f"\nResults saved to {table_name}")
    logger.info(f"Total records: {len(results_df):,}")


def main():
    """Main execution function."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("Starting image availability check...")
    logger.info(f"Start time: {start_time}")
    logger.info("="*80)
    
    # Create logs directory
    os.makedirs('embryoscope/report/logs', exist_ok=True)
    
    # Configuration
    db_path = 'database/huntington_data_lake.duckdb'
    config_manager = EmbryoscopeConfigManager()
    
    # Get embryo data
    embryos_df = get_embryo_data_from_db(db_path)
    
    # Group by server
    servers = embryos_df['patient_unit_huntington'].unique()
    logger.info(f"\nServers to process: {', '.join(servers)}")
    
    # Get enabled servers configuration
    enabled_servers = config_manager.get_enabled_embryoscopes()
    
    # Prepare data for parallel processing
    server_tasks = []
    for server in servers:
        if server not in enabled_servers:
            logger.warning(f"Server {server} not found in enabled servers, skipping...")
            continue
        
        server_embryos = embryos_df[embryos_df['patient_unit_huntington'] == server]
        embryos_data = server_embryos.to_dict('records')
        config_dict = enabled_servers[server]
        
        server_tasks.append((server, embryos_data, config_dict))
    
    logger.info(f"\nProcessing {len(server_tasks)} servers in parallel...")
    
    all_results = []
    
    # Process servers in parallel
    max_workers = min(len(server_tasks), multiprocessing.cpu_count())
    logger.info(f"Using {max_workers} parallel workers")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_server = {
            executor.submit(process_server_embryos, server, embryos_data, config_dict): server
            for server, embryos_data, config_dict in server_tasks
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_server):
            server = future_to_server[future]
            try:
                results = future.result()
                all_results.extend(results)
                logger.info(f"✓ Completed {server}: {len(results):,} results")
            except Exception as e:
                logger.error(f"✗ Error processing server {server}: {e}")
                continue
    
    # Convert results to DataFrame
    if all_results:
        results_df = pd.DataFrame(all_results)
        
        # Save to database
        save_results_to_db(results_df, db_path)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\n" + "="*80)
        logger.info("PROCESSING COMPLETE!")
        logger.info("="*80)
        logger.info(f"Start time: {start_time}")
        logger.info(f"End time: {end_time}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Total embryos processed: {len(results_df):,}")
        logger.info("="*80)
    else:
        logger.error("No results to save!")


if __name__ == "__main__":
    main()
