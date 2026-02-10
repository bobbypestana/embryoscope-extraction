"""
Step 1: Check Image Availability via API
Queries the Embryoscope API and writes structured logs to disk.
Does NOT write to database - logs are processed by subsequent scripts.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
from datetime import datetime
import logging
import argparse
from typing import Dict, List
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import json

from utils.api_client import EmbryoscopeAPIClient
from utils.config_manager import EmbryoscopeConfigManager


def setup_logging(mode: str):
    """Setup logging configuration."""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'api_check_{mode}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__), log_file


# Resolve paths relative to script location
SCRIPT_DIR = Path(__file__).parent
DB_PATH = str((SCRIPT_DIR / "../../database/huntington_data_lake.duckdb").resolve())


def get_embryos_to_check(mode: str, limit: int = None) -> pd.DataFrame:
    """Determine which embryos to check based on execution mode."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    if mode == 'all':
        query = """
        SELECT * FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID IS NOT NULL
        ORDER BY patient_unit_huntington, prontuario, embryo_EmbryoID
        """
    elif mode == 'new':
        query = """
        SELECT s.*
        FROM gold.embryoscope_embrioes s
        LEFT JOIN silver.embryo_image_availability_latest l
            ON s.embryo_EmbryoID = l.embryo_EmbryoID
        WHERE s.embryo_EmbryoID IS NOT NULL
          AND l.embryo_EmbryoID IS NULL
        ORDER BY s.patient_unit_huntington, s.prontuario, s.embryo_EmbryoID
        """
    elif mode == 'retry':
        query = """
        SELECT s.*
        FROM gold.embryoscope_embrioes s
        LEFT JOIN silver.embryo_image_availability_latest l
            ON s.embryo_EmbryoID = l.embryo_EmbryoID
        WHERE s.embryo_EmbryoID IS NOT NULL
          AND (l.embryo_EmbryoID IS NULL OR l.image_available = FALSE OR l.api_response_code IN (500, 0))
        ORDER BY s.patient_unit_huntington, s.prontuario, s.embryo_EmbryoID
        """
    else:
        raise ValueError(f"Invalid mode: {mode}")
    
    df = conn.execute(query).df()
    conn.close()
    
    # Apply limit if specified
    if limit and limit > 0:
        df = df.head(limit)
    
    return df


def check_image_availability(client: EmbryoscopeAPIClient, embryo_id: str) -> Dict:
    """Check if images are available for a specific embryo."""
    result = {
        'embryo_EmbryoID': embryo_id,
        'image_available': False,
        'image_runs_count': 0,
        'api_response_code': 0,
        'error_message': None,
        'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    }
    
    try:
        response = client.get_image_runs(embryo_id)
        
        # Store raw status code from the client
        status_code = getattr(client, 'last_status_code', 0)
        result['api_response_code'] = status_code
        
        if response is not None and 'ImageRuns' in response:
            runs = response.get('ImageRuns', [])
            result['image_runs_count'] = len(runs) if isinstance(runs, list) else 0
            result['image_available'] = result['image_runs_count'] > 0
            result['error_message'] = 'OK'
        else:
            # Keep the raw error message if it exists
            result['error_message'] = getattr(client, 'last_error', 'Empty body or API error')
            
    except Exception as e:
        result['api_response_code'] = getattr(client, 'last_status_code', 500)
        result['error_message'] = str(e)
    
    return result


def process_server_embryos(server_name: str, embryos_data: List[Dict], config_dict: Dict, mode: str, output_dir: str) -> str:
    """Process all embryos for a specific server and write results to JSON."""
    process_logger = logging.getLogger(f"server_{server_name}")
    process_logger.setLevel(logging.INFO)
    
    if not process_logger.handlers:
        log_file = os.path.join(SCRIPT_DIR, 'logs', f'{server_name}_{mode}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        process_logger.addHandler(fh)
    
    process_logger.info(f"Processing {server_name}: {len(embryos_data):,} embryos")
    
    # Initialize API client
    client = EmbryoscopeAPIClient(server_name, config_dict, rate_limit_delay=0.1)
    
    if not client.authenticate():
        process_logger.error(f"Failed to authenticate with {server_name}")
        return None
    
    results = []
    
    for idx, embryo_data in enumerate(embryos_data):
        embryo_id = embryo_data['embryo_EmbryoID']
        
        if (idx + 1) % 100 == 0:
            process_logger.info(f"Progress: {idx + 1:,}/{len(embryos_data):,}")
        
        result = check_image_availability(client, embryo_id)
        result.update(embryo_data)
        results.append(result)
        
        if (idx + 1) % 1000 == 0:
            client.authenticate()
    
    # Write results to JSON file
    output_file = os.path.join(output_dir, f'{server_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    process_logger.info(f"Completed {server_name}: {len(results):,} results written to {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(description='Check embryo image availability via API')
    parser.add_argument('--mode', choices=['new', 'retry', 'all'], default='retry',
                        help='Execution mode (default: retry - new embryos + errors/no images)')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of embryos to check (for testing)')
    args = parser.parse_args()
    
    logger, main_log_file = setup_logging(args.mode)
    start_time = datetime.now()
    
    logger.info("="*80)
    logger.info(f"Starting API check (mode: {args.mode})")
    logger.info(f"Start time: {start_time}")
    logger.info("="*80)
    
    # Create output directory for JSON results, local to the 'report' folder
    script_dir = Path(__file__).parent
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    output_dir = script_dir / 'api_results' / f'{args.mode}_{timestamp}'
    os.makedirs(output_dir, exist_ok=True)
    
    # Get embryos to check
    embryos_df = get_embryos_to_check(args.mode, args.limit)
    
    if len(embryos_df) == 0:
        logger.info("No embryos to check!")
        return
    
    logger.info(f"Found {len(embryos_df):,} embryos to check")
    
    # Group by server
    servers = embryos_df['patient_unit_huntington'].unique()
    config_manager = EmbryoscopeConfigManager(str((SCRIPT_DIR / '../params.yml').resolve()))
    enabled_servers = config_manager.get_enabled_embryoscopes()
    
    # Prepare server tasks
    server_tasks = []
    for server in servers:
        if server not in enabled_servers:
            logger.warning(f"Server {server} not enabled, skipping...")
            continue
        
        server_embryos = embryos_df[embryos_df['patient_unit_huntington'] == server]
        embryos_data = server_embryos.to_dict('records')
        config_dict = enabled_servers[server]
        
        server_tasks.append((server, embryos_data, config_dict, args.mode, output_dir))
    
    logger.info(f"Processing {len(server_tasks)} servers in parallel...")
    
    # Process servers
    max_workers = min(len(server_tasks), multiprocessing.cpu_count())
    output_files = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_server = {
            executor.submit(process_server_embryos, *task): task[0]
            for task in server_tasks
        }
        
        for future in as_completed(future_to_server):
            server = future_to_server[future]
            try:
                output_file = future.result()
                if output_file:
                    output_files.append(output_file)
                logger.info(f"Completed {server}")
            except Exception as e:
                logger.error(f"✗ Error processing {server}: {e}")
    
    end_time = datetime.now()
    
    logger.info("\n" + "="*80)
    logger.info("API CHECK COMPLETE!")
    logger.info("="*80)
    logger.info(f"Duration: {end_time - start_time}")
    logger.info(f"Results directory: {output_dir}")
    logger.info(f"Output files: {len(output_files)}")
    logger.info("\nNext step: Run 02_logs_to_bronze.py to process these results")
    logger.info("="*80)


if __name__ == "__main__":
    main()
