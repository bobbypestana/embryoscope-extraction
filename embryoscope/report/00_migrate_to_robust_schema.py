"""
Migration Script: Initialize Robust Embryo Image Availability Schema

This script creates the new Bronze/Silver/Gold tables and migrates existing data
from the current gold.embryo_image_availability_raw table.

New Schema:
- bronze.embryo_image_availability_logs: Append-only log of all API checks
- silver.embryo_image_availability_latest: Latest status per embryo
- gold.embryo_image_status_changes: Audit log of status changes
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = 'database/huntington_data_lake.duckdb'


def create_bronze_table(conn):
    """Create bronze.embryo_image_availability_logs table"""
    logger.info("Creating bronze.embryo_image_availability_logs...")
    
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS bronze
    """)
    
    conn.execute("""
        DROP TABLE IF EXISTS bronze.embryo_image_availability_logs
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.embryo_image_availability_logs (
            log_id INTEGER PRIMARY KEY,
            embryo_EmbryoID VARCHAR NOT NULL,
            prontuario BIGINT,
            patient_unit_huntington VARCHAR,
            image_available BOOLEAN,
            image_runs_count DOUBLE,
            api_response_status VARCHAR,
            api_response_code BIGINT,
            error_message VARCHAR,
            checked_at VARCHAR,
            extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    logger.info("✓ Bronze table created")


def create_silver_table(conn):
    """Create silver.embryo_image_availability_latest table"""
    logger.info("Creating silver.embryo_image_availability_latest...")
    
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS silver
    """)
    
    conn.execute("""
        DROP TABLE IF EXISTS silver.embryo_image_availability_latest
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver.embryo_image_availability_latest (
            embryo_EmbryoID VARCHAR PRIMARY KEY,
            prontuario BIGINT,
            patient_PatientID BIGINT,
            patient_PatientIDx VARCHAR,
            patient_unit_huntington VARCHAR,
            treatment_TreatmentName VARCHAR,
            embryo_EmbryoDate TIMESTAMP,
            image_available BOOLEAN,
            image_runs_count DOUBLE,
            api_response_status VARCHAR,
            api_response_code BIGINT,
            error_message VARCHAR,
            checked_at VARCHAR,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    logger.info("✓ Silver table created")


def create_gold_changes_table(conn):
    """Create gold.embryo_image_status_changes table"""
    logger.info("Creating gold.embryo_image_status_changes...")
    
    conn.execute("""
        DROP TABLE IF EXISTS gold.embryo_image_status_changes
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold.embryo_image_status_changes (
            change_id INTEGER PRIMARY KEY,
            embryo_EmbryoID VARCHAR NOT NULL,
            previous_status VARCHAR,
            previous_code BIGINT,
            new_status VARCHAR,
            new_code BIGINT,
            change_type VARCHAR,  -- 'new', 'error_to_success', 'success_to_error', etc.
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    logger.info("✓ Gold changes table created")


def migrate_existing_data(conn):
    """Migrate data from gold.embryo_image_availability_raw to new schema"""
    logger.info("Migrating existing data...")
    
    # Check if source table exists
    check = conn.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'gold' AND table_name = 'embryo_image_availability_raw'
    """).fetchone()[0]
    
    if check == 0:
        logger.warning("Source table gold.embryo_image_availability_raw not found. Skipping migration.")
        return
    
    # 1. Populate Bronze (append-only log)
    logger.info("Populating bronze.embryo_image_availability_logs...")
    conn.execute("""
        INSERT INTO bronze.embryo_image_availability_logs 
        (log_id, embryo_EmbryoID, prontuario, patient_unit_huntington, 
         image_available, image_runs_count, api_response_status, 
         api_response_code, error_message, checked_at)
        SELECT 
            ROW_NUMBER() OVER (ORDER BY checked_at) as log_id,
            embryo_EmbryoID,
            prontuario,
            patient_unit_huntington,
            image_available,
            image_runs_count,
            api_response_status,
            api_response_code,
            error_message,
            checked_at
        FROM gold.embryo_image_availability_raw
    """)
    
    bronze_count = conn.execute("SELECT COUNT(*) FROM bronze.embryo_image_availability_logs").fetchone()[0]
    logger.info(f"✓ Migrated {bronze_count:,} records to Bronze")
    
    # 2. Populate Silver (latest status per embryo)
    logger.info("Populating silver.embryo_image_availability_latest...")
    conn.execute("""
        INSERT INTO silver.embryo_image_availability_latest
        SELECT 
            embryo_EmbryoID,
            prontuario,
            patient_PatientID,
            patient_PatientIDx,
            patient_unit_huntington,
            treatment_TreatmentName,
            embryo_EmbryoDate,
            image_available,
            image_runs_count,
            api_response_status,
            api_response_code,
            error_message,
            checked_at,
            CURRENT_TIMESTAMP as last_updated
        FROM gold.embryo_image_availability_raw
    """)
    
    silver_count = conn.execute("SELECT COUNT(*) FROM silver.embryo_image_availability_latest").fetchone()[0]
    logger.info(f"✓ Migrated {silver_count:,} unique embryos to Silver")
    
    logger.info("Migration complete!")


def verify_migration(conn):
    """Verify the migration was successful"""
    logger.info("\nVerifying migration...")
    
    # Count records in each table
    bronze_count = conn.execute("SELECT COUNT(*) FROM bronze.embryo_image_availability_logs").fetchone()[0]
    silver_count = conn.execute("SELECT COUNT(*) FROM silver.embryo_image_availability_latest").fetchone()[0]
    
    # Get status distribution
    status_dist = conn.execute("""
        SELECT api_response_code, COUNT(*) as count
        FROM silver.embryo_image_availability_latest
        GROUP BY api_response_code
        ORDER BY count DESC
    """).df()
    
    print("\n" + "=" * 80)
    print("MIGRATION VERIFICATION")
    print("=" * 80)
    print(f"Bronze (logs): {bronze_count:,} records")
    print(f"Silver (latest): {silver_count:,} unique embryos")
    print("\nStatus Distribution in Silver:")
    print(status_dist.to_string(index=False))
    print("=" * 80)


def main():
    """Main migration execution"""
    logger.info("Starting migration to robust schema...")
    logger.info(f"Database: {DB_PATH}")
    
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Create new tables
        create_bronze_table(conn)
        create_silver_table(conn)
        create_gold_changes_table(conn)
        
        # Migrate existing data
        migrate_existing_data(conn)
        
        # Verify
        verify_migration(conn)
        
        logger.info("\n✓ Migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()
