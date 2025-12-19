"""
Utility functions for embryo image extraction.
Handles database queries, file operations, and metadata tracking.
"""

import os
import duckdb
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import hashlib
from zipfile import ZipFile, is_zipfile

logger = logging.getLogger(__name__)


def get_embryos_to_extract(conn: duckdb.DuckDBPyConnection, limit: int = 3) -> List[Dict[str, str]]:
    """
    Query distinct Slide IDs with their clinic locations from gold.data_ploidia.
    
    Args:
        conn: DuckDB connection
        limit: Maximum number of embryos to return
        
    Returns:
        List of dictionaries with 'embryo_id' and 'location' keys
    """
    query = '''
        SELECT DISTINCT 
            dp."Slide ID" as embryo_id,
            ee.patient_unit_huntington as location,
            ee.patient_PatientID as prontuario,
            ee.embryo_EmbryoDescriptionID as embryo_description_id
        FROM gold.data_ploidia dp
        LEFT JOIN gold.embryoscope_embrioes ee 
            ON dp."Slide ID" = ee.embryo_EmbryoID
        WHERE ee.patient_unit_huntington IS NOT NULL
        ORDER BY dp."Slide ID"
        LIMIT ?
    '''
    
    df = conn.execute(query, [limit]).df()
    embryos = df.to_dict('records')
    
    logger.info(f"Found {len(embryos)} embryos to extract")
    for embryo in embryos:
        logger.debug(f"  - {embryo['embryo_id']}: {embryo['location']}")
    
    return embryos


def map_location_to_api_config(location_name: str) -> str:
    """
    Map patient_unit_huntington location name to API credential key.
    
    Args:
        location_name: Location name from patient_unit_huntington
        
    Returns:
        API credential key name
        
    Raises:
        ValueError: If location name is not recognized
    """
    # Mapping from database location names to API credential keys
    location_mapping = {
        'Ibirapuera': 'Ibirapuera',
        'Vila Mariana': 'Vila Mariana',
        'Belo Horizonte': 'Belo Horizonte',
        'Brasilia': 'Brasilia',
        'Santa Joana': 'Santa Joana',
        'Salvador': 'Salvador'
    }
    
    if location_name not in location_mapping:
        raise ValueError(f"Unknown location: {location_name}. Valid locations: {list(location_mapping.keys())}")
    
    return location_mapping[location_name]


def check_existing_extraction(conn: duckdb.DuckDBPyConnection, embryo_id: str) -> Optional[Dict]:
    """
    Check if images for an embryo have already been extracted.
    
    Args:
        conn: DuckDB connection
        embryo_id: Embryo ID to check
        
    Returns:
        Dictionary with extraction metadata if exists, None otherwise
    """
    query = '''
        SELECT 
            embryo_id,
            clinic_location,
            extraction_timestamp,
            image_count,
            embryo_folder_path,
            file_size_bytes,
            status,
            error_message
        FROM gold.embryo_images_metadata
        WHERE embryo_id = ?
    '''
    
    result = conn.execute(query, [embryo_id]).df()
    
    if len(result) == 0:
        return None
    
    return result.iloc[0].to_dict()


def save_embryo_files(zip_content: bytes, image_runs_data: dict, embryo_id: str, prontuario: str, output_dir: str) -> Tuple[str, int, int]:
    """
    Save ZIP file and image runs JSON in embryo-specific folder.
    
    Args:
        zip_content: ZIP file content as bytes
        image_runs_data: Image runs data dictionary
        embryo_id: Embryo ID for naming
        prontuario: Patient ID for folder naming
        output_dir: Base output directory path
        
    Returns:
        Tuple of (folder_path, file_size_bytes, runs_count)
    """
    import json
    
    # Create embryo-specific folder with prontuario prefix
    folder_name = f"{prontuario}_{embryo_id}"
    folder_path = os.path.join(output_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    
    # Save images ZIP
    zip_file_path = os.path.join(folder_path, 'images.zip')
    with open(zip_file_path, 'wb') as f:
        f.write(zip_content)
    file_size = len(zip_content)
    
    # Save image runs JSON
    runs_file_path = os.path.join(folder_path, f'{embryo_id}_imageruns.json')
    with open(runs_file_path, 'w', encoding='utf-8') as f:
        json.dump(image_runs_data, f, indent=2, ensure_ascii=False)
    
    # Count image runs
    runs_count = len(image_runs_data.get('ImageRuns', [])) if isinstance(image_runs_data.get('ImageRuns'), list) else 0
    
    logger.info(f"Saved embryo files to: {folder_path}")
    logger.info(f"  - images.zip ({file_size:,} bytes)")
    logger.info(f"  - imageruns.json ({runs_count} runs)")
    
    return folder_path, file_size, runs_count


def validate_zip_file(zip_path: str) -> Tuple[bool, int, Optional[str]]:
    """
    Validate ZIP file integrity and count images.
    
    Args:
        zip_path: Path to ZIP file
        
    Returns:
        Tuple of (is_valid, image_count, error_message)
    """
    try:
        if not os.path.exists(zip_path):
            return False, 0, f"File not found: {zip_path}"
        
        if not is_zipfile(zip_path):
            return False, 0, "File is not a valid ZIP archive"
        
        with ZipFile(zip_path, 'r') as zip_file:
            # Test ZIP integrity
            bad_file = zip_file.testzip()
            if bad_file:
                return False, 0, f"Corrupted file in ZIP: {bad_file}"
            
            # Count image files (JPEG/JPG)
            image_files = [f for f in zip_file.namelist() 
                          if f.lower().endswith(('.jpg', '.jpeg'))]
            image_count = len(image_files)
            
            logger.debug(f"ZIP validation successful: {image_count} images found")
            return True, image_count, None
            
    except Exception as e:
        error_msg = f"Error validating ZIP file: {str(e)}"
        logger.error(error_msg)
        return False, 0, error_msg


def update_metadata(
    conn: duckdb.DuckDBPyConnection,
    embryo_id: str,
    clinic_location: str,
    status: str,
    embryo_folder_path: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    image_count: Optional[int] = None,
    image_runs_count: Optional[int] = None,
    error_message: Optional[str] = None,
    api_response_time_ms: Optional[int] = None,
    prontuario: Optional[str] = None,
    embryo_description_id: Optional[str] = None
) -> None:
    """
    Insert or update metadata for an embryo extraction.
    
    Args:
        conn: DuckDB connection
        embryo_id: Embryo ID
        clinic_location: Clinic location name
        status: Extraction status ('success', 'failed', 'pending')
        zip_file_path: Path to saved ZIP file
        file_size_bytes: Size of ZIP file
        image_count: Number of images in ZIP
        error_message: Error message if failed
        api_response_time_ms: API response time in milliseconds
    """
    # Check if record exists
    existing = check_existing_extraction(conn, embryo_id)
    
    if existing:
        # Update existing record
        query = '''
            UPDATE gold.embryo_images_metadata
            SET 
                clinic_location = ?,
                extraction_timestamp = CURRENT_TIMESTAMP,
                image_count = ?,
                embryo_folder_path = ?,
                file_size_bytes = ?,
                image_runs_count = ?,
                status = ?,
                error_message = ?,
                api_response_time_ms = ?,
                prontuario = ?,
                embryo_description_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE embryo_id = ?
        '''
        params = [
            clinic_location,
            image_count,
            embryo_folder_path,
            file_size_bytes,
            image_runs_count,
            status,
            error_message,
            api_response_time_ms,
            prontuario,
            embryo_description_id,
            embryo_id
        ]
        logger.debug(f"Updating metadata for {embryo_id}")
    else:
        # Insert new record
        query = '''
            INSERT INTO gold.embryo_images_metadata (
                embryo_id,
                clinic_location,
                extraction_timestamp,
                image_count,
                embryo_folder_path,
                file_size_bytes,
                image_runs_count,
                status,
                error_message,
                api_response_time_ms,
                prontuario,
                embryo_description_id
            ) VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = [
            embryo_id,
            clinic_location,
            image_count,
            embryo_folder_path,
            file_size_bytes,
            image_runs_count,
            status,
            error_message,
            api_response_time_ms,
            prontuario,
            embryo_description_id
        ]
        logger.debug(f"Inserting new metadata for {embryo_id}")
    
    conn.execute(query, params)
    logger.info(f"Metadata updated for {embryo_id}: status={status}")


def initialize_metadata_table(db_path: str) -> None:
    """
    Initialize the metadata table if it doesn't exist.
    
    Args:
        db_path: Path to DuckDB database
    """
    logger.info(f"Initializing metadata table in {db_path}")
    
    # Read SQL script
    sql_script_path = os.path.join(os.path.dirname(__file__), '01_create_metadata_table.sql')
    
    if not os.path.exists(sql_script_path):
        logger.warning(f"SQL script not found: {sql_script_path}")
        logger.info("Creating table programmatically instead")
        
        # Create table programmatically if SQL file doesn't exist
        with duckdb.connect(db_path) as conn:
            conn.execute('''
                CREATE SCHEMA IF NOT EXISTS gold;
                
                CREATE TABLE IF NOT EXISTS gold.embryo_images_metadata (
                    embryo_id VARCHAR PRIMARY KEY,
                    clinic_location VARCHAR NOT NULL,
                    extraction_timestamp TIMESTAMP NOT NULL,
                    image_count INTEGER,
                    zip_file_path VARCHAR,
                    file_size_bytes BIGINT,
                    status VARCHAR NOT NULL,
                    error_message VARCHAR,
                    api_response_time_ms INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_embryo_images_location 
                    ON gold.embryo_images_metadata(clinic_location);
                CREATE INDEX IF NOT EXISTS idx_embryo_images_status 
                    ON gold.embryo_images_metadata(status);
                CREATE INDEX IF NOT EXISTS idx_embryo_images_extraction_timestamp 
                    ON gold.embryo_images_metadata(extraction_timestamp);
                CREATE INDEX IF NOT EXISTS idx_embryo_images_location_status 
                    ON gold.embryo_images_metadata(clinic_location, status);
            ''')
        logger.info("Metadata table created successfully")
    else:
        # Execute SQL script
        with open(sql_script_path, 'r') as f:
            sql_script = f.read()
        
        with duckdb.connect(db_path) as conn:
            conn.execute(sql_script)
        
        logger.info("Metadata table initialized from SQL script")
