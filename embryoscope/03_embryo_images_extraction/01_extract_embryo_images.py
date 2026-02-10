"""
Main script for extracting embryo images from the Embryoscope API.

This script:
1. Queries embryos from gold.data_ploidia (first 3 distinct Slide IDs)
2. Maps embryos to clinic locations
3. Checks if images already exist
4. Extracts images as ZIP files using the API
5. Saves ZIP files and updates metadata table
"""

import os
import sys
import json
import logging
import duckdb
import yaml
from datetime import datetime
from pathlib import Path
import argparse
from typing import List, Dict, Optional, Tuple
import time
import concurrent.futures
from collections import defaultdict
import threading

# Add parent directory to path to import utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.api_client import EmbryoscopeAPIClient
from utils.config_manager import EmbryoscopeConfigManager
import image_extraction_utils as utils

# Setup logging with thread-safe handlers
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'embryo_image_extraction_{timestamp}.log')

# Create a lock for thread-safe logging
log_lock = threading.Lock()

class ThreadSafeFileHandler(logging.FileHandler):
    """FileHandler that uses a lock to prevent concurrent write errors."""
    def emit(self, record):
        with log_lock:
            super().emit(record)

class ThreadSafeStreamHandler(logging.StreamHandler):
    """StreamHandler that uses a lock to prevent concurrent write errors."""
    def emit(self, record):
        with log_lock:
            super().emit(record)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        ThreadSafeFileHandler(LOG_PATH),
        ThreadSafeStreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'huntington_data_lake.duckdb')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'export_images')
LIMIT = 3  # Number of embryos to extract
TEMP_RESULTS_LOG = os.path.join(LOGS_DIR, f'extraction_results_{timestamp}.jsonl')


# Focal planes to extract (start only with plane 0)
FOCAL_PLANES = [0]



def extract_embryo_images(
    embryo_id: str, 
    location: str, 
    api_client: EmbryoscopeAPIClient, 
    focal_planes: List[int],
    prontuario: Optional[str] = None,
    embryo_description_id: Optional[str] = None
) -> Dict:
    """
    Extract specific focal planes for a single embryo in parallel.
    
    Args:
        embryo_id: Embryo ID to extract
        location: Clinic location name
        api_client: Initialized API client for the clinic
        focal_planes: List of focal plane numbers to extract
        prontuario: Patient ID (prontuario)
        embryo_description_id: Embryo description ID
        
    Returns:
        Summary dictionary
    """
    logger.info(f"=" * 80)
    logger.info(f"Extracting images for {embryo_id} from {location}")
    logger.info(f"Targeting focal planes: {focal_planes}")
    logger.info(f"=" * 80)
    sys.stdout.flush()
    
    if not focal_planes:
        logger.info(f"All focal planes already extracted for {embryo_id}. Skipping.")
        return {'status': 'skipped', 'planes_extracted': 0}

    try:
        # Record start time for API response time tracking
        start_time = time.time()
        
        # Call API to get image runs (only needed once per embryo)
        image_runs = api_client.get_image_runs(embryo_id)
        
        if image_runs is None:
            status_code = getattr(api_client, 'last_status_code', 'Unknown')
            last_error = getattr(api_client, 'last_error', 'No error captured')
            error_msg = f"API request failed (Status: {status_code}) - {last_error}"
            logger.error(error_msg)
            # Log failure for all requested planes
            for plane in focal_planes:
                utils.append_extraction_result_to_log(TEMP_RESULTS_LOG, {
                    'embryo_id': embryo_id, 'focal_plane': plane, 'clinic_location': location,
                    'status': 'failed', 'error_message': error_msg,
                    'prontuario': prontuario, 'embryo_description_id': embryo_description_id,
                    'api_response_time_ms': int((time.time() - start_time) * 1000)
                })
            return {
                'status': 'failed', 
                'planes_extracted': 0,
                'total_size_mb': 0
            }

        # Counters for summary
        success_count = 0
        failure_count = 0
        total_size_bytes = 0
        
        # Function to download and log a single focal plane
        def process_plane(plane):
            logger.info(f"  Fetching focal plane {plane}...")
            sys.stdout.flush()
            
            p_start = time.time()
            response = api_client.get_all_images(embryo_id, image_overlay=True, focal_plane=plane)
            p_duration = int((time.time() - p_start) * 1000)
            
            result_data = {
                'embryo_id': embryo_id,
                'focal_plane': plane,
                'clinic_location': location,
                'prontuario': prontuario,
                'embryo_description_id': embryo_description_id,
                'api_response_time_ms': p_duration,
                'status': 'failed',
                'error_message': None,
                'file_size_bytes': 0,
                'image_runs_count': len(image_runs.get('ImageRuns', [])) if isinstance(image_runs.get('ImageRuns'), list) else 0
            }

            if response is not None and response.status_code == 200:
                content = response.content
                file_size = len(content)
                
                # Save just this plane's ZIP file
                folder_name = f"{prontuario}_{embryo_id}"
                folder_path = os.path.join(OUTPUT_DIR, folder_name)
                os.makedirs(folder_path, exist_ok=True)
                
                zip_filename = f'images_F{plane}.zip'
                zip_path = os.path.join(folder_path, zip_filename)
                
                try:
                    with open(zip_path, 'wb') as f:
                        f.write(content)
                    
                    result_data.update({
                        'status': 'success',
                        'file_size_bytes': file_size
                    })
                    logger.info(f"  [OK] Plane {plane} saved ({file_size/1024/1024:.2f} MB)")
                except Exception as e:
                    result_data['error_message'] = f"Save error: {str(e)}"
                    logger.error(f"  [FAIL] Failed to save plane {plane}: {e}")
            else:
                status_code = response.status_code if response else 'None'
                error_msg = f"API error (Status: {status_code})"
                result_data['error_message'] = error_msg
                logger.warning(f"  [FAIL] Failed plane {plane}: {error_msg}")

            # IMMEDIATE LOGGING to file for sync later
            utils.append_extraction_result_to_log(TEMP_RESULTS_LOG, result_data)
            sys.stdout.flush()
            return result_data

        # Use ThreadPoolExecutor for parallel plane downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_plane, plane) for plane in focal_planes]
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res['status'] == 'success':
                    success_count += 1
                    total_size_bytes += res['file_size_bytes']
                else:
                    failure_count += 1

        # Save image runs JSON if at least one plane succeeded
        if success_count > 0:
            folder_name = f"{prontuario}_{embryo_id}"
            folder_path = os.path.join(OUTPUT_DIR, folder_name)
            runs_file_path = os.path.join(folder_path, f'{embryo_id}_imageruns.json')
            with open(runs_file_path, 'w', encoding='utf-8') as f:
                json.dump(image_runs, f, indent=2, ensure_ascii=False)

        logger.info(f"--- Finished {embryo_id}: {success_count} success, {failure_count} failed ---")
        return {
            'status': 'success' if success_count > 0 else 'failed',
            'planes_extracted': success_count,
            'total_size_mb': total_size_bytes / (1024 * 1024)
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'embryo_id': embryo_id,
            'clinic_location': location,
            'prontuario': prontuario,
            'embryo_description_id': embryo_description_id,
            'status': 'failed',
            'error_message': error_msg,
            'planes_extracted': 0,
            'total_size_mb': 0,
            'api_response_time_ms': int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0
        }


def main():
    """Main extraction workflow."""
    parser = argparse.ArgumentParser(description='Embryo Image Extraction Pipeline')
    parser.add_argument('--limit', type=int, default=3, help='Limit number of embryos to extract (default: 3)')
    parser.add_argument('--planes', type=str, default='0', help='Comma-separated focal planes to extract (default: 0)')
    parser.add_argument('--mode', type=str, default='all', choices=['all', 'with_biopsy', 'without_biopsy'], 
                        help='Extraction mode: all, with_biopsy, or without_biopsy (default: all)')
    args = parser.parse_args()

    # Parse planes
    global FOCAL_PLANES
    try:
        FOCAL_PLANES = [int(p.strip()) for p in args.planes.split(',')]
    except ValueError:
        print("Invalid planes format. Use comma-separated integers.")
        sys.exit(1)

    limit = args.limit

    logger.info("=" * 80)
    logger.info("EMBRYO IMAGE EXTRACTION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Extraction limit: {limit} embryos")
    logger.info(f"Target planes: {FOCAL_PLANES}")
    logger.info("")
    
    # Verify database exists
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found: {DB_PATH}")
        return
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        # Connect to database to check for embryos
        logger.info("Connecting to database...")
        conn = duckdb.connect(DB_PATH)
        
        try:
            # Initialize metadata table
            logger.info("Initializing metadata table...")
            utils.initialize_metadata_table(DB_PATH)
            
            # Query embryos to extract (now filtered in SQL to exclude successes)
            logger.info(f"Querying embryos from gold.data_ploidia (limit {limit}, mode {args.mode})...")
            # We only look for embryos that are missing ANY of the requested FOCAL_PLANES
            embryos = utils.get_embryos_to_extract(conn, limit=limit, planes=FOCAL_PLANES, mode=args.mode)
            
        finally:
            # CLOSE CONNECTION as soon as we have the list to avoid locks during extraction
            conn.close()
            logger.info("Database connection closed for extraction phase.")
        
        if not embryos:
            logger.warning("No new embryos found to extract")
            return
        
        # Group embryos by location (unit) for parallel processing
        embryos_by_unit = defaultdict(list)
        for e in embryos:
            embryos_by_unit[e['location']].append(e)
            
        logger.info(f"Found {len(embryos)} embryos across {len(embryos_by_unit)} units.")
        for loc, group in embryos_by_unit.items():
            logger.info(f"  - {loc}: {len(group)} embryos")
        logger.info("")
        sys.stdout.flush()
        
        # Load API configuration
        params_path = os.path.join(os.path.dirname(__file__), '..', 'params.yml')
        config_manager = EmbryoscopeConfigManager(config_path=params_path)
        
        # Function to process all embryos for a single unit
        def process_unit(location, unit_embryos):
            logger.info(f"--- Starting worker for UNIT: {location} ({len(unit_embryos)} embryos) ---")
            unit_results = {'success': [], 'failed': [], 'skipped': []}
            
            # Map location to API config
            try:
                api_config_key = utils.map_location_to_api_config(location)
            except ValueError as e:
                logger.error(f"[{location}] [FAIL] {e}")
                for e in unit_embryos:
                    utils.append_extraction_result_to_log(TEMP_RESULTS_LOG, {
                        'embryo_id': e['embryo_id'], 'focal_plane': 0, 'clinic_location': location,
                        'status': 'failed', 'error_message': str(e),
                        'prontuario': e.get('prontuario'), 'embryo_description_id': e.get('embryo_description_id')
                    })
                return unit_results

            # Get API configuration for this location
            enabled_embryoscopes = config_manager.get_enabled_embryoscopes()
            if api_config_key not in enabled_embryoscopes:
                error_msg = f"API config not found/enabled for {api_config_key}"
                logger.error(f"[{location}] [FAIL] {error_msg}")
                for e in unit_embryos:
                    utils.append_extraction_result_to_log(TEMP_RESULTS_LOG, {
                        'embryo_id': e['embryo_id'], 'focal_plane': 0, 'clinic_location': location,
                        'status': 'failed', 'error_message': error_msg,
                        'prontuario': e.get('prontuario'), 'embryo_description_id': e.get('embryo_description_id')
                    })
                return unit_results
            
            # Initialize API client for this clinic
            api_client = EmbryoscopeAPIClient(location, enabled_embryoscopes[api_config_key])
            
            # Process embryos for this unit SEQUENTIALLY
            for j, embryo in enumerate(unit_embryos, 1):
                embryo_id = embryo['embryo_id']
                prontuario = embryo.get('prontuario')
                embryo_description_id = embryo.get('embryo_description_id')
                
                logger.info(f"[{location}] [{j}/{len(unit_embryos)}] Processing {embryo_id}...")
                
                # Extract all requested focal planes (already filtered by initial query)
                summary = extract_embryo_images(
                    embryo_id, location, api_client, FOCAL_PLANES, prontuario, embryo_description_id
                )
                
                if summary['status'] == 'success' or summary['planes_extracted'] > 0:
                    unit_results['success'].append(embryo_id)
                else:
                    unit_results['failed'].append(embryo_id)
                sys.stdout.flush()
                
            return unit_results

        # Track overall extraction results
        overall_results = {'success': [], 'failed': [], 'skipped': []}
        
        # Use ThreadPoolExecutor to process each UNIT in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(embryos_by_unit)) as executor:
            future_to_unit = {executor.submit(process_unit, loc, group): loc for loc, group in embryos_by_unit.items()}
            
            for future in concurrent.futures.as_completed(future_to_unit):
                unit_name = future_to_unit[future]
                try:
                    res = future.result()
                    overall_results['success'].extend(res['success'])
                    overall_results['failed'].extend(res['failed'])
                    overall_results['skipped'].extend(res['skipped'])
                except Exception as exc:
                    logger.error(f"Unit {unit_name} generated an exception: {exc}")
        
        # Set results for final summary
        results = overall_results
        
        # EXTRACTION SUMMARY
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Success: {len(results['success'])}")
        logger.info(f"Failed: {len(results['failed'])}")
        logger.info(f"Skipped: {len(results['skipped'])}")
        logger.info("=" * 80)
        logger.info(f"Results logged to: {TEMP_RESULTS_LOG}")
        logger.info("Please run 02_sync_and_export_metadata.py to update the database and generate Excel reports.")
        logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"Fatal error in main workflow: {e}", exc_info=True)
        raise
    finally:
        logger.info("Extraction script finished.")


if __name__ == '__main__':
    main()
