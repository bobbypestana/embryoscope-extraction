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

METADATA_TABLE_SCHEMA = """
-- Create the metadata table
CREATE TABLE IF NOT EXISTS gold.embryo_images_metadata (
    embryo_id VARCHAR NOT NULL,
    focal_plane INTEGER NOT NULL,
    prontuario VARCHAR,
    embryo_description_id VARCHAR,
    clinic_location VARCHAR NOT NULL,
    extraction_timestamp TIMESTAMP NOT NULL,
    image_count INTEGER,
    file_size_bytes BIGINT,
    image_runs_count INTEGER,
    status VARCHAR NOT NULL,
    error_message VARCHAR,
    api_response_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (embryo_id, focal_plane)
);

CREATE INDEX IF NOT EXISTS idx_embryo_images_location ON gold.embryo_images_metadata(clinic_location);
CREATE INDEX IF NOT EXISTS idx_embryo_images_status ON gold.embryo_images_metadata(status);
CREATE INDEX IF NOT EXISTS idx_embryo_images_extraction_timestamp ON gold.embryo_images_metadata(extraction_timestamp);
"""


def get_embryos_to_extract(conn: duckdb.DuckDBPyConnection, limit: int = 3, planes: List[int] = [0], mode: str = "all") -> List[Dict[str, str]]:
    """
    Query distinct Slide IDs with their clinic locations from gold.data_ploidia.
    
    Args:
        conn: DuckDB connection
        limit: Maximum number of embryos to return
        planes: List of focal planes we are interested in.
        
    Returns:
        List of dictionaries with 'embryo_id' and 'location' keys
    """
    # Query distinct Slide IDs that are missing at least one of the requested planes.

    # Biopsy filters based on user request:
    # with_biopsy: (Embryo Description NOT NULL or Embryo Description Clinisys NOT NULL)
    # without_biopsy: (Embryo Description IS NULL AND Embryo Description Clinisys IS NULL)
    biopsy_filter = ""
    if mode == "with_biopsy":
        biopsy_filter = 'AND (dp."Embryo Description" IS NOT NULL OR dp."Embryo Description Clinisys" IS NOT NULL)'
    elif mode == "without_biopsy":
        biopsy_filter = '''AND (dp."Embryo Description" IS NULL AND dp."Embryo Description Clinisys" IS NULL)
              AND (dp.outcome_type IS NOT NULL OR 
                   dp.merged_numero_de_nascidos IS NOT NULL OR 
                   dp.fet_gravidez_clinica IS NOT NULL OR 
                   dp.trat2_resultado_tratamento IS NOT NULL OR 
                   dp.trat1_resultado_tratamento IS NOT NULL OR 
                   dp.fet_tipo_resultado IS NOT NULL)'''

    # Refined Query:
    # 1. Joins with silver.embryo_image_availability_latest to filter by api_response_code = 200
    # 2. Skips ANY embryo already in gold.embryo_images_metadata (already queried)
    query = f'''
        SELECT 
            dp."Slide ID" as embryo_id,
            ee.patient_unit_huntington as location,
            dp."Patient ID" as prontuario,
            ee.embryo_EmbryoDescriptionID as embryo_description_id
        FROM gold.data_ploidia dp
        JOIN gold.embryoscope_embrioes ee
            ON dp."Slide ID" = ee.embryo_EmbryoID
        JOIN silver.embryo_image_availability_latest l
            ON dp."Slide ID" = l.embryo_EmbryoID
        LEFT JOIN (SELECT DISTINCT embryo_id FROM gold.embryo_images_metadata) eim
            ON dp."Slide ID" = eim.embryo_id
        WHERE l.api_response_code = 200
          AND eim.embryo_id IS NULL
          AND ee.patient_unit_huntington IS NOT NULL
          {biopsy_filter}
        ORDER BY prontuario DESC
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


def check_existing_extraction(conn: duckdb.DuckDBPyConnection, embryo_id: str, focal_plane: int = 0) -> Optional[Dict]:
    """
    Check if a specific focal plane for an embryo has already been extracted.
    
    Args:
        conn: DuckDB connection
        embryo_id: Embryo ID to check
        focal_plane: Focal plane to check
        
    Returns:
        Dictionary with extraction metadata if exists, None otherwise
    """
    query = '''
        SELECT 
            embryo_id,
            focal_plane,
            clinic_location,
            extraction_timestamp,
            image_count,
        FROM gold.embryo_images_metadata
        WHERE embryo_id = ? AND focal_plane = ?
    '''
    
    result = conn.execute(query, [embryo_id, focal_plane]).df()
    
    if len(result) == 0:
        return None
    
    return result.iloc[0].to_dict()


def get_extracted_planes(conn: duckdb.DuckDBPyConnection, embryo_id: str) -> List[int]:
    """
    Get a list of focal planes that were successfully extracted for an embryo.
    
    Args:
        conn: DuckDB connection
        embryo_id: Embryo ID
        
    Returns:
        List of successful focal plane integers
    """
    query = '''
        SELECT focal_plane 
        FROM gold.embryo_images_metadata 
        WHERE embryo_id = ? AND status = 'success'
    '''
    try:
        df = conn.execute(query, [embryo_id]).df()
        return df['focal_plane'].tolist()
    except Exception as e:
        logger.error(f"Error fetching extracted planes for {embryo_id}: {e}")
        return []


def save_embryo_files(zip_contents: Dict[int, bytes], image_runs_data: dict, embryo_id: str, prontuario: str, output_dir: str) -> Tuple[str, int, int]:
    """
    Save separate ZIP files for each focal plane and image runs JSON.
    
    Args:
        zip_contents: Dictionary mapping focal plane (int) to ZIP file content (bytes)
        image_runs_data: Image runs data dictionary
        embryo_id: Embryo ID for naming
        prontuario: Patient ID for folder naming
        output_dir: Base output directory path
        
    Returns:
        Tuple of (folder_path, total_size_bytes, runs_count)
    """
    import json
    
    # Create embryo-specific folder with prontuario prefix
    folder_name = f"{prontuario}_{embryo_id}"
    folder_path = os.path.join(output_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    
    total_size = 0
    saved_files = []
    
    # Save a separate ZIP for each focal plane
    for focal_plane, content in zip_contents.items():
        zip_filename = f'images_F{focal_plane}.zip'
        zip_path = os.path.join(folder_path, zip_filename)
        
        try:
            with open(zip_path, 'wb') as f:
                f.write(content)
            
            file_size = len(content)
            total_size += file_size
            saved_files.append(zip_filename)
            
        except Exception as e:
            logger.error(f"Failed to save {zip_filename}: {e}")

    # Save image runs JSON
    runs_file_path = os.path.join(folder_path, f'{embryo_id}_imageruns.json')
    with open(runs_file_path, 'w', encoding='utf-8') as f:
        json.dump(image_runs_data, f, indent=2, ensure_ascii=False)
    
    # Count image runs
    runs_count = len(image_runs_data.get('ImageRuns', [])) if isinstance(image_runs_data.get('ImageRuns'), list) else 0
    
    logger.info(f"Saved embryo files to: {folder_path}")
    logger.info(f"  - {len(saved_files)} focal plane ZIPs (Total: {total_size/1024/1024:.2f} MB)")
    logger.info(f"  - imageruns.json ({runs_count} runs)")
    
    return folder_path, total_size, runs_count


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
    focal_plane: int = 0,
    file_size_bytes: Optional[int] = None,
    image_count: Optional[int] = None,
    image_runs_count: Optional[int] = None,
    error_message: Optional[str] = None,
    api_response_time_ms: Optional[int] = None,
    prontuario: Optional[str] = None,
    embryo_description_id: Optional[str] = None
) -> None:
    """
    Insert or update metadata for an embryo focal plane extraction.
    """
    # Check if record exists
    existing = check_existing_extraction(conn, embryo_id, focal_plane)
    
    if existing:
        # Update existing record
        query = '''
            UPDATE gold.embryo_images_metadata
            SET 
                clinic_location = ?,
                extraction_timestamp = CURRENT_TIMESTAMP,
                image_count = ?,
                file_size_bytes = ?,
                image_runs_count = ?,
                status = ?,
                error_message = ?,
                api_response_time_ms = ?,
                prontuario = ?,
                embryo_description_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE embryo_id = ? AND focal_plane = ?
        '''
        params = [
            clinic_location,
            image_count,
            file_size_bytes,
            image_runs_count,
            status,
            error_message,
            api_response_time_ms,
            prontuario,
            embryo_description_id,
            embryo_id,
            focal_plane
        ]
        logger.debug(f"Updating metadata for {embryo_id} F{focal_plane}")
    else:
        # Insert new record
        query = '''
            INSERT INTO gold.embryo_images_metadata (
                embryo_id,
                focal_plane,
                clinic_location,
                extraction_timestamp,
                image_count,
                file_size_bytes,
                image_runs_count,
                status,
                error_message,
                api_response_time_ms,
                prontuario,
                embryo_description_id
            ) VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = [
            embryo_id,
            focal_plane,
            clinic_location,
            image_count,
            file_size_bytes,
            image_runs_count,
            status,
            error_message,
            api_response_time_ms,
            prontuario,
            embryo_description_id
        ]
        logger.debug(f"Inserting new metadata for {embryo_id} F{focal_plane}")
    
    conn.execute(query, params)
    logger.info(f"Metadata updated for {embryo_id} F{focal_plane}: status={status}")


def append_extraction_result_to_log(log_path: str, result_data: Dict) -> None:
    """
    Append an extraction result to a temporary JSONL log file.
    
    Args:
        log_path: Path to the JSONL log file
        result_data: Dictionary containing extraction result and metadata
    """
    import json
    # Add timestamp to the result
    result_data['log_timestamp'] = datetime.now().isoformat()
    
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(result_data, ensure_ascii=False) + '\n')
    
    logger.debug(f"Result for {result_data.get('embryo_id')} logged to {log_path}")


def sync_results_log_to_db(conn: duckdb.DuckDBPyConnection, log_path: str) -> bool:
    """
    Read results from log file and synchronize them to the database.
    
    Args:
        conn: DuckDB connection
        log_path: Path to the JSONL log file
        
    Returns:
        True if sync successful, False otherwise
    """
    import json
    
    if not os.path.exists(log_path):
        logger.info(f"No results log found at {log_path}, skipping sync.")
        return True
    
    logger.info(f"Synchronizing results from {log_path} to database...")
    
    count = 0
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    result = json.loads(line)
                    # Use the existing update_metadata function to handle each record
                    update_metadata(
                        conn,
                        embryo_id=result.get('embryo_id'),
                        focal_plane=result.get('focal_plane', 0),
                        clinic_location=result.get('clinic_location'),
                        status=result.get('status'),
                        file_size_bytes=result.get('file_size_bytes'),
                        image_count=result.get('image_count'),
                        image_runs_count=result.get('image_runs_count'),
                        error_message=result.get('error_message'),
                        api_response_time_ms=result.get('api_response_time_ms'),
                        prontuario=result.get('prontuario'),
                        embryo_description_id=result.get('embryo_description_id')
                    )
                    count += 1
                except Exception as e:
                    logger.error(f"Error parsing log line: {e}")
        
        # After successful sync, delete the temp log
        os.remove(log_path)
        logger.info(f"Successfully synchronized {count} results to database and removed log file.")
        return True
        
    except Exception as e:
        logger.error(f"Failed to synchronize results: {e}")
        return False


def initialize_metadata_table(db_path: str) -> None:
    """
    Initialize the metadata table if it doesn't exist.
    
    Args:
        db_path: Path to DuckDB database
    """
    logger.info(f"Initializing metadata table in {db_path}")
    
    with duckdb.connect(db_path) as conn:
        # Create schema if not exists
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        
        # Execute schema
        conn.execute(METADATA_TABLE_SCHEMA)
        
    logger.info("Metadata table initialized successfully")
