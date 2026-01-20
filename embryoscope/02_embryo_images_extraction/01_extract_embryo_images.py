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
import logging
import duckdb
import yaml
from datetime import datetime
from pathlib import Path
from typing import Optional
import time

# Add parent directory to path to import utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.api_client import EmbryoscopeAPIClient
from utils.config_manager import EmbryoscopeConfigManager
import image_extraction_utils as utils

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'embryo_image_extraction_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'huntington_data_lake.duckdb')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'export_images')
LIMIT = 3  # Number of embryos to extract


def extract_embryo_images(
    embryo_id: str, 
    location: str, 
    api_client: EmbryoscopeAPIClient, 
    conn: duckdb.DuckDBPyConnection,
    prontuario: Optional[str] = None,
    embryo_description_id: Optional[str] = None
) -> bool:
    """
    Extract images for a single embryo.
    
    Args:
        embryo_id: Embryo ID to extract
        location: Clinic location name
        api_client: Initialized API client for the clinic
        conn: Database connection
        prontuario: Patient ID (prontuario)
        embryo_description_id: Embryo description ID
        
    Returns:
        True if extraction successful, False otherwise
    """
    logger.info(f"=" * 80)
    logger.info(f"Extracting images for {embryo_id} from {location}")
    logger.info(f"=" * 80)
    
    try:
        # Record start time for API response time tracking
        start_time = time.time()
        
        # Call API to get all images
        response = api_client.get_all_images(embryo_id, image_overlay=True, focal_plane=0)
        
        # Call API to get image runs
        image_runs = api_client.get_image_runs(embryo_id)
        
        # Calculate API response time
        api_response_time_ms = int((time.time() - start_time) * 1000)
        
        if response is None or response.status_code != 200 or image_runs is None:
            error_msg = f"API request failed - Images: {response.status_code if response else 'None'}, Runs: {'OK' if image_runs else 'Failed'}"
            logger.error(error_msg)
            utils.update_metadata(
                conn, embryo_id, location, 'failed',
                error_message=error_msg,
                api_response_time_ms=api_response_time_ms,
                prontuario=prontuario,
                embryo_description_id=embryo_description_id
            )
            return False
        
        # Save both ZIP file and image runs JSON in embryo folder
        embryo_folder_path, file_size_bytes, runs_count = utils.save_embryo_files(
            response.content, image_runs, embryo_id, prontuario, OUTPUT_DIR
        )
        
        # Validate ZIP file
        zip_file_path = os.path.join(embryo_folder_path, 'images.zip')
        is_valid, image_count, error_message = utils.validate_zip_file(zip_file_path)
        
        if not is_valid:
            logger.error(f"ZIP validation failed: {error_message}")
            utils.update_metadata(
                conn, embryo_id, location, 'failed',
                embryo_folder_path=embryo_folder_path,
                file_size_bytes=file_size_bytes,
                error_message=f"ZIP validation failed: {error_message}",
                api_response_time_ms=api_response_time_ms,
                prontuario=prontuario,
                embryo_description_id=embryo_description_id
            )
            return False
        
        # Update metadata with success
        utils.update_metadata(
            conn, embryo_id, location, 'success',
            embryo_folder_path=embryo_folder_path,
            file_size_bytes=file_size_bytes,
            image_count=image_count,
            image_runs_count=runs_count,
            api_response_time_ms=api_response_time_ms,
            prontuario=prontuario,
            embryo_description_id=embryo_description_id
        )
        
        logger.info(f"✓ Successfully extracted {image_count} images and {runs_count} runs for {embryo_id}")
        logger.info(f"  Folder: {embryo_folder_path}")
        logger.info(f"  Images ZIP: {file_size_bytes:,} bytes")
        logger.info(f"  API response time: {api_response_time_ms}ms")
        
        return True
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        utils.update_metadata(
            conn, embryo_id, location, 'failed',
            error_message=error_msg,
            prontuario=prontuario,
            embryo_description_id=embryo_description_id
        )
        return False


def main():
    """Main extraction workflow."""
    logger.info("=" * 80)
    logger.info("EMBRYO IMAGE EXTRACTION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Log file: {LOG_PATH}")
    logger.info(f"Extraction limit: {LIMIT} embryos")
    logger.info("")
    
    # Verify database exists
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found: {DB_PATH}")
        return
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Initialize database connection
    logger.info("Connecting to database...")
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Initialize metadata table
        logger.info("Initializing metadata table...")
        utils.initialize_metadata_table(DB_PATH)
        
        # Query embryos to extract
        logger.info(f"Querying first {LIMIT} embryos from gold.data_ploidia...")
        embryos = utils.get_embryos_to_extract(conn, limit=LIMIT)
        
        if not embryos:
            logger.warning("No embryos found to extract")
            return
        
        logger.info(f"Found {len(embryos)} embryos to process:")
        for embryo in embryos:
            logger.info(f"  - {embryo['embryo_id']}: {embryo['location']}")
        logger.info("")
        
        # Load API configuration
        logger.info("Loading API configuration...")
        # Get correct path to params.yml (go up to embryoscope directory)
        params_path = os.path.join(os.path.dirname(__file__), '..', 'params.yml')
        config_manager = EmbryoscopeConfigManager(config_path=params_path)
        
        # Track extraction results
        results = {
            'success': [],
            'failed': [],
            'skipped': []
        }
        
        # Process each embryo
        for i, embryo in enumerate(embryos, 1):
            embryo_id = embryo['embryo_id']
            location = embryo['location']
            prontuario = embryo.get('prontuario')
            embryo_description_id = embryo.get('embryo_description_id')
            
            logger.info(f"\n[{i}/{len(embryos)}] Processing {embryo_id}...")
            
            # Check if already extracted
            existing = utils.check_existing_extraction(conn, embryo_id)
            if existing and existing['status'] == 'success':
                logger.info(f"✓ Images already extracted on {existing['extraction_timestamp']}")
                logger.info(f"  Skipping (use --force to re-extract)")
                results['skipped'].append(embryo_id)
                continue
            
            # Map location to API config
            try:
                api_config_key = utils.map_location_to_api_config(location)
            except ValueError as e:
                logger.error(f"✗ {e}")
                results['failed'].append(embryo_id)
                utils.update_metadata(conn, embryo_id, location, 'failed', 
                                    error_message=str(e),
                                    prontuario=prontuario,
                                    embryo_description_id=embryo_description_id)
                continue
            
            # Get API configuration for this location
            enabled_embryoscopes = config_manager.get_enabled_embryoscopes()
            if api_config_key not in enabled_embryoscopes:
                error_msg = f"API configuration not found or not enabled for {api_config_key}"
                logger.error(f"✗ {error_msg}")
                results['failed'].append(embryo_id)
                utils.update_metadata(conn, embryo_id, location, 'failed', 
                                    error_message=error_msg,
                                    prontuario=prontuario,
                                    embryo_description_id=embryo_description_id)
                continue
            
            api_config = enabled_embryoscopes[api_config_key]
            
            # Initialize API client
            logger.info(f"Connecting to {api_config_key} API...")
            api_client = EmbryoscopeAPIClient(api_config_key, api_config)
            
            # Extract images
            success = extract_embryo_images(embryo_id, location, api_client, conn,
                                          prontuario, embryo_description_id)
            
            if success:
                results['success'].append(embryo_id)
            else:
                results['failed'].append(embryo_id)
        
        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total embryos processed: {len(embryos)}")
        logger.info(f"✓ Successful extractions: {len(results['success'])}")
        logger.info(f"✗ Failed extractions: {len(results['failed'])}")
        logger.info(f"⊘ Skipped (already extracted): {len(results['skipped'])}")
        
        if results['success']:
            logger.info(f"\nSuccessfully extracted:")
            for embryo_id in results['success']:
                logger.info(f"  ✓ {embryo_id}")
        
        if results['failed']:
            logger.info(f"\nFailed extractions:")
            for embryo_id in results['failed']:
                logger.info(f"  ✗ {embryo_id}")
        
        if results['skipped']:
            logger.info(f"\nSkipped (already extracted):")
            for embryo_id in results['skipped']:
                logger.info(f"  ⊘ {embryo_id}")
        
        logger.info(f"\nLog file: {LOG_PATH}")
        logger.info(f"Output directory: {OUTPUT_DIR}")
        logger.info("=" * 80)
        
        # Export metadata to Excel
        try:
            logger.info("\nExporting metadata to Excel...")
            metadata_df = conn.execute('''
                SELECT 
                    embryo_id,
                    prontuario,
                    embryo_description_id,
                    clinic_location,
                    extraction_timestamp,
                    image_count,
                    image_runs_count,
                    file_size_bytes,
                    status,
                    error_message,
                    api_response_time_ms
                FROM gold.embryo_images_metadata
                ORDER BY extraction_timestamp DESC
            ''').df()
            
            # Format file size in MB
            metadata_df['file_size_mb'] = (metadata_df['file_size_bytes'] / 1024 / 1024).round(2)
            
            # Save to Excel
            excel_path = os.path.join(OUTPUT_DIR, '00_extraction_metadata.xlsx')
            metadata_df.to_excel(excel_path, index=False, sheet_name='Extraction Metadata')
            logger.info(f"✓ Metadata exported to: {excel_path}")
            logger.info(f"  Total records: {len(metadata_df)}")
        except Exception as e:
            logger.warning(f"Failed to export metadata to Excel: {e}")

        # Export clinical data (from gold.data_ploidia) to Excel
        try:
            logger.info("\nExporting clinical data to Excel...")
            # Get list of processed embryo IDs
            if results['success'] or results['skipped']:
                processed_ids = results['success'] + results['skipped']
                # Format list for SQL IN clause
                ids_str = "', '".join(processed_ids)
                
                clinical_query = f'''
                    SELECT *
                    FROM gold.data_ploidia
                    WHERE "Slide ID" IN ('{ids_str}')
                '''
                
                clinical_df = conn.execute(clinical_query).df()
                
                # Save to Excel
                clinical_excel_path = os.path.join(OUTPUT_DIR, '01_clinical_data.xlsx')
                clinical_df.to_excel(clinical_excel_path, index=False, sheet_name='Clinical Data')
                logger.info(f"✓ Clinical data exported to: {clinical_excel_path}")
                logger.info(f"  Total records: {len(clinical_df)}")
            else:
                logger.info("No embryos processed effectively, skipping clinical data export.")
                
        except Exception as e:
            logger.warning(f"Failed to export clinical data to Excel: {e}")
            
    except Exception as e:
        logger.error(f"Fatal error in main workflow: {e}", exc_info=True)
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == '__main__':
    main()
