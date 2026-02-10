#!/usr/bin/env python3
"""
Cleanup Silver Layer Script
Removes orphaned treatments and embryo data for patients that were discarded
due to non-numeric PatientID during the bronze-to-silver transformation.
"""

import os
import sys
import logging
import duckdb
from datetime import datetime
from pathlib import Path

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """Setup logging configuration."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    log_dir = os.path.join(parent_dir, 'logs')  # logs in parent embryoscope directory
    os.makedirs(log_dir, exist_ok=True)
    
    log_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'{script_name}_{log_ts}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(script_name)
    logger.info(f"Logging to: {log_file}")
    return logger

def find_database_files(database_dir):
    """Find all embryoscope database files."""
    database_path = Path(database_dir)
    db_files = list(database_path.glob("embryoscope_*.db"))
    return [str(f) for f in db_files]

def get_discarded_patient_ids(conn, logger):
    """
    Get PatientIDx values that were discarded during bronze-to-silver transformation.
    These are patients that exist in bronze but not in silver.
    """
    try:
        # Get all PatientIDx and PatientID from bronze.raw_patients
        bronze_patients = conn.execute("""
            SELECT DISTINCT 
                raw_json->>'$.PatientIDx' as patient_idx,
                raw_json->>'$.PatientID' as patient_id
            FROM bronze.raw_patients
        """).fetchall()
        
        # Get all PatientIDx from silver.patients
        silver_patients = conn.execute("""
            SELECT DISTINCT PatientIDx
            FROM silver.patients
        """).fetchall()
        
        bronze_set = {row[0] for row in bronze_patients if row[0]}
        silver_set = {row[0] for row in silver_patients if row[0]}
        
        # Discarded patients are those in bronze but not in silver
        discarded_patient_idx_set = bronze_set - silver_set
        
        # Create a mapping of PatientIDx to PatientID for discarded patients
        discarded_patients_info = {}
        for row in bronze_patients:
            patient_idx, patient_id = row[0], row[1]
            if patient_idx in discarded_patient_idx_set:
                discarded_patients_info[patient_idx] = patient_id
        
        logger.info(f"Found {len(discarded_patient_idx_set)} discarded PatientIDx values")
        
        # Log detailed list of discarded patients
        if discarded_patients_info:
            logger.info("=== DETAILED LIST OF DISCARDED PATIENTS ===")
            for i, (patient_idx, patient_id) in enumerate(sorted(discarded_patients_info.items()), 1):
                patient_id_display = patient_id if patient_id else "N/A"
                logger.info(f"  {i:3d}. {patient_idx} (PatientID: {patient_id_display})")
            logger.info("=== END OF DISCARDED PATIENTS LIST ===")
        else:
            logger.info("No discarded patients found")
        
        return discarded_patient_idx_set
        
    except Exception as e:
        logger.error(f"Error getting discarded patient IDs: {e}")
        return set()

def cleanup_orphaned_records(conn, discarded_patients, logger):
    """
    Remove treatments and embryo data for discarded patients.
    """
    if not discarded_patients:
        logger.info("No discarded patients found, skipping cleanup")
        return
    
    # Convert set to list for SQL IN clause
    patient_list = list(discarded_patients)
    
    try:
        # Clean up treatments
        logger.info("Cleaning up orphaned treatments...")
        treatments_deleted = conn.execute("""
            DELETE FROM silver.treatments 
            WHERE PatientIDx IN ({})
        """.format(','.join(['?'] * len(patient_list))), patient_list).rowcount
        
        logger.info(f"Deleted {treatments_deleted} orphaned treatment records")
        
        # Clean up embryo data
        logger.info("Cleaning up orphaned embryo data...")
        embryo_deleted = conn.execute("""
            DELETE FROM silver.embryo_data 
            WHERE PatientIDx IN ({})
        """.format(','.join(['?'] * len(patient_list))), patient_list).rowcount
        
        logger.info(f"Deleted {embryo_deleted} orphaned embryo data records")
        
        # Clean up idascore (if it has PatientIDx column)
        try:
            idascore_deleted = conn.execute("""
                DELETE FROM silver.idascore 
                WHERE PatientIDx IN ({})
            """.format(','.join(['?'] * len(patient_list))), patient_list).rowcount
            
            logger.info(f"Deleted {idascore_deleted} orphaned idascore records")
        except Exception as e:
            logger.debug(f"Could not clean idascore (may not have PatientIDx column): {e}")
        
        return {
            'treatments_deleted': treatments_deleted,
            'embryo_deleted': embryo_deleted
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up orphaned records: {e}")
        return None

def get_table_counts(conn, logger):
    """Get current record counts for all silver tables."""
    try:
        counts = {}
        tables = ['patients', 'treatments', 'embryo_data', 'idascore']
        
        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
                counts[table] = count
            except Exception as e:
                logger.debug(f"Could not get count for silver.{table}: {e}")
                counts[table] = 0
        
        return counts
        
    except Exception as e:
        logger.error(f"Error getting table counts: {e}")
        return {}

def process_database(db_path, logger):
    """Process a single database file."""
    db_name = os.path.basename(db_path)
    logger.info(f"Processing database: {db_path}")
    
    try:
        conn = duckdb.connect(db_path)
        
        # Get initial counts
        initial_counts = get_table_counts(conn, logger)
        logger.info(f"Initial counts: {initial_counts}")
        
        # Get discarded patient IDs
        discarded_patients = get_discarded_patient_ids(conn, logger)
        
        if discarded_patients:
            # Clean up orphaned records
            cleanup_results = cleanup_orphaned_records(conn, discarded_patients, logger)
            
            if cleanup_results:
                # Get final counts
                final_counts = get_table_counts(conn, logger)
                logger.info(f"Final counts: {final_counts}")
                
                # Log summary
                logger.info(f"Cleanup summary for {db_name}:")
                logger.info(f"  - Discarded patients: {len(discarded_patients)}")
                logger.info(f"  - Treatments deleted: {cleanup_results['treatments_deleted']}")
                logger.info(f"  - Embryo data deleted: {cleanup_results['embryo_deleted']}")
        else:
            logger.info("No cleanup needed - no discarded patients found")
        
        conn.close()
        logger.info(f"Finished processing: {db_path}")
        
    except Exception as e:
        logger.error(f"Error processing {db_path}: {e}")

def main():
    """Main execution function."""
    logger = setup_logging()
    
    # Find database files
    database_dir = "../../database"
    db_files = find_database_files(database_dir)
    
    if not db_files:
        logger.error(f"No embryoscope database files found in {database_dir}")
        return
    
    logger.info(f"Found {len(db_files)} database files to process")
    
    # Process each database
    for db_path in db_files:
        process_database(db_path, logger)
    
    logger.info("Silver layer cleanup completed for all databases")

if __name__ == "__main__":
    main() 