#!/usr/bin/env python3
"""
Create gold.data_ploidia table from gold.planilha_embryoscope_combined

This script:
1. Reads the column mapping configuration
2. Creates a SELECT query that maps source columns to target columns
3. Leaves NULL for unmapped columns
4. Creates the table gold.data_ploidia with columns in the specified order
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging
import sys

# Import column mapping configuration
sys.path.insert(0, os.path.dirname(__file__))
from column_mapping import TARGET_COLUMNS, COLUMN_MAPPING, FILTER_PATIENT_ID, FILTER_EMBRYO_IDS

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_connection(read_only=False):
    """Create and return a connection to the huntington_data_lake database"""
    repo_root = os.path.dirname(os.path.dirname(__file__))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    return conn

def get_available_columns(conn):
    """Get list of available columns in planilha_embryoscope_combined"""
    col_info = conn.execute("DESCRIBE gold.planilha_embryoscope_combined").df()
    available_columns = col_info['column_name'].tolist()
    logger.info(f"Found {len(available_columns)} columns in gold.planilha_embryoscope_combined")
    return available_columns

def build_select_clause(target_columns, column_mapping, available_columns):
    """
    Build SELECT clause that maps source columns to target columns
    
    Args:
        target_columns: List of target column names in order
        column_mapping: Dictionary mapping target -> source column names (can be None)
        available_columns: List of available columns in source table
    
    Returns:
        SELECT clause string, unmapped columns list, missing source columns list
    """
    select_parts = []
    unmapped_columns = []
    missing_source_columns = []
    
    for target_col in target_columns:
        # Special handling for Age - calculate from date difference (must come before None check)
        if target_col == "Age":
            fertilization_col = "embryo_FertilizationTime"
            birth_col = "patient_DateOfBirth"
            if fertilization_col in available_columns and birth_col in available_columns:
                # Calculate age in years with 2 decimal places: difference between fertilization time and birth date
                # Calculate days difference and divide by 365.25 (accounting for leap years), then round to 2 decimals
                select_parts.append(f'ROUND(CAST(DATE_DIFF(\'day\', "{birth_col}", "{fertilization_col}") AS DOUBLE) / 365.25, 2) as "{target_col}"')
            else:
                missing_cols = []
                if fertilization_col not in available_columns:
                    missing_cols.append(fertilization_col)
                if birth_col not in available_columns:
                    missing_cols.append(birth_col)
                logger.warning(f"Missing columns for Age calculation: {missing_cols}. Using NULL.")
                select_parts.append(f'NULL as "{target_col}"')
                missing_source_columns.append((target_col, ', '.join(missing_cols)))
        # Special handling for BMI - calculate from weight and height (must come before None check)
        elif target_col == "BMI":
            weight_col = "trat_peso_paciente"
            height_col = "trat_altura_paciente"
            if weight_col in available_columns and height_col in available_columns:
                # Calculate BMI: weight / (height)^2
                # Weight and height are patient+slide-level attributes, so use MAX() to get non-NULL values
                # Group by Patient ID and Slide ID (everything before "-" in embryo_EmbryoID)
                # Height is in meters, BMI = weight(kg) / height(m)^2
                select_parts.append(f'''CASE 
                    WHEN MAX("{weight_col}") OVER (PARTITION BY "micro_prontuario", SPLIT_PART("embryo_EmbryoID", '-', 1)) IS NOT NULL 
                         AND MAX("{height_col}") OVER (PARTITION BY "micro_prontuario", SPLIT_PART("embryo_EmbryoID", '-', 1)) IS NOT NULL 
                         AND CAST(MAX("{height_col}") OVER (PARTITION BY "micro_prontuario", SPLIT_PART("embryo_EmbryoID", '-', 1)) AS DOUBLE) > 0 
                    THEN ROUND(CAST(MAX("{weight_col}") OVER (PARTITION BY "micro_prontuario", SPLIT_PART("embryo_EmbryoID", '-', 1)) AS DOUBLE) / 
                                  POWER(CAST(MAX("{height_col}") OVER (PARTITION BY "micro_prontuario", SPLIT_PART("embryo_EmbryoID", '-', 1)) AS DOUBLE), 2), 2)
                    ELSE NULL 
                END as "{target_col}"''')
            else:
                missing_cols = []
                if weight_col not in available_columns:
                    missing_cols.append(weight_col)
                if height_col not in available_columns:
                    missing_cols.append(height_col)
                logger.warning(f"Missing columns for BMI calculation: {missing_cols}. Using NULL.")
                select_parts.append(f'NULL as "{target_col}"')
                missing_source_columns.append((target_col, ', '.join(missing_cols)))
        # Special handling for Previus ET: rank ET cycles per patient by Slide ID groups
        elif target_col == "Previus ET":
            # We want: oldest slide group = 0, next = 1, etc. (monotonic by slide prefix)
            # Let r = DENSE_RANK over slide prefixes ASC (oldest = 1, newest = N)
            # Then Previus ET = r - 1 → oldest: 1-1=0, next: 2-1=1, ...
            select_parts.append(
                'DENSE_RANK() OVER ('
                'PARTITION BY "micro_prontuario" '
                'ORDER BY SPLIT_PART("embryo_EmbryoID", \'-\', 1) ASC'
                ') - 1 as "Previus ET"'
            )
        # Special handling for patient+slide-level columns that need MAX() aggregation
        elif target_col in ["Diagnosis", "Patient Comments", "Previus OD ET", "Oocyte Source"]:
            source_col = column_mapping.get(target_col)
            if source_col is not None and source_col in available_columns:
                # Use MAX() window function to get non-NULL values grouped by Patient ID and Slide ID
                select_parts.append(f'MAX("{source_col}") OVER (PARTITION BY "micro_prontuario", SPLIT_PART("embryo_EmbryoID", \'-\', 1)) as "{target_col}"')
            else:
                if source_col is None:
                    select_parts.append(f'NULL as "{target_col}"')
                    unmapped_columns.append(target_col)
                else:
                    logger.warning(f"Source column '{source_col}' not found for target '{target_col}'. Using NULL.")
                    select_parts.append(f'NULL as "{target_col}"')
                    missing_source_columns.append((target_col, source_col))
        elif target_col in column_mapping:
            source_col = column_mapping[target_col]
            
            # Handle None mapping (explicitly unmapped)
            if source_col is None:
                select_parts.append(f'NULL as "{target_col}"')
                unmapped_columns.append(target_col)
            # Special handling for Birth Year - extract year from date
            elif target_col == "Birth Year" and source_col == "patient_DateOfBirth":
                if source_col in available_columns:
                    select_parts.append(f'EXTRACT(YEAR FROM "{source_col}") as "{target_col}"')
                else:
                    select_parts.append(f'NULL as "{target_col}"')
                    missing_source_columns.append((target_col, source_col))
            # Check if source column exists
            elif source_col in available_columns:
                # Escape column name properly (handle special characters)
                select_parts.append(f'"{source_col}" as "{target_col}"')
            else:
                # Source column doesn't exist, use NULL
                logger.warning(f"Source column '{source_col}' not found for target '{target_col}'. Using NULL.")
                select_parts.append(f'NULL as "{target_col}"')
                missing_source_columns.append((target_col, source_col))
        else:
            # No mapping, use NULL
            select_parts.append(f'NULL as "{target_col}"')
            unmapped_columns.append(target_col)
    
    if unmapped_columns:
        logger.info(f"Unmapped columns (will be NULL): {unmapped_columns}")
    if missing_source_columns:
        logger.warning(f"Missing source columns (will be NULL): {missing_source_columns}")
    
    return ', '.join(select_parts), unmapped_columns, missing_source_columns

def create_data_ploidia_table(conn):
    """Create gold.data_ploidia table with mapped columns"""
    
    logger.info("=" * 80)
    logger.info("CREATING gold.data_ploidia TABLE")
    logger.info("=" * 80)
    
    # Get available columns from source table
    logger.info("Checking available columns in source table...")
    available_columns = get_available_columns(conn)
    
    # Build SELECT clause
    logger.info("Building SELECT clause with column mappings...")
    select_clause, unmapped, missing = build_select_clause(
        TARGET_COLUMNS, 
        COLUMN_MAPPING, 
        available_columns
    )
    
    # Create the table
    logger.info("Creating gold.data_ploidia table...")
    
    # Ensure gold schema exists
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.data_ploidia")
    logger.info("Dropped existing gold.data_ploidia table if it existed")
    
    # Build WHERE clause
    where_conditions = []
    
    # Always exclude rows where Embryo Description is NULL
    where_conditions.append('"embryo_Description" IS NOT NULL')
    logger.info("Filter: Embryo Description IS NOT NULL")
    
    if FILTER_PATIENT_ID is not None:
        # Filter by Patient ID (micro_prontuario)
        where_conditions.append(f'"micro_prontuario" = {FILTER_PATIENT_ID}')
        logger.info(f"Filter: Patient ID = {FILTER_PATIENT_ID}")
    
    if FILTER_EMBRYO_IDS is not None and len(FILTER_EMBRYO_IDS) > 0:
        # Filter by Embryo ID (embryo_EmbryoID)
        embryo_ids_str = ', '.join([f"'{eid}'" for eid in FILTER_EMBRYO_IDS])
        where_conditions.append(f'"embryo_EmbryoID" IN ({embryo_ids_str})')
        logger.info(f"Filter: Embryo ID IN ({', '.join(FILTER_EMBRYO_IDS)})")
    
    # Always exclude NULL embryo IDs to avoid incomplete records
    where_conditions.append('"embryo_EmbryoID" IS NOT NULL')
    
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Create table with mapped columns
    create_query = f"""
    CREATE TABLE gold.data_ploidia AS
    SELECT 
        {select_clause}
    FROM gold.planilha_embryoscope_combined
    {where_clause}
    """
    
    logger.info("Executing CREATE TABLE query...")
    logger.info(f"Creating table with {len(TARGET_COLUMNS)} columns")
    if where_clause:
        logger.info(f"Applying filters: {where_clause}")
    
    conn.execute(create_query)
    logger.info("Table gold.data_ploidia created successfully")
    
    # Get statistics
    row_count = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia").fetchone()[0]
    col_info = conn.execute("DESCRIBE gold.data_ploidia").df()
    col_count = len(col_info)
    
    logger.info("=" * 80)
    logger.info("TABLE STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Rows: {row_count:,}")
    logger.info(f"Columns: {col_count}")
    logger.info("")
    
    # Show column list
    logger.info("Columns in gold.data_ploidia (in order):")
    for i, col in enumerate(TARGET_COLUMNS, 1):
        # Special handling for Age (calculated dynamically)
        if col == "Age":
            source_col = "ROUND(CAST(DATE_DIFF('day', patient_DateOfBirth, embryo_FertilizationTime) AS DOUBLE) / 365.25, 2)"
            status = "[OK]" if "embryo_FertilizationTime" in available_columns and "patient_DateOfBirth" in available_columns else "[--]"
        # Special handling for BMI (calculated dynamically with patient+slide-level aggregation)
        elif col == "BMI":
            source_col = "ROUND(MAX(trat_peso_paciente) OVER (PARTITION BY micro_prontuario, SPLIT_PART(embryo_EmbryoID, '-', 1)) / POWER(MAX(trat_altura_paciente) OVER (PARTITION BY micro_prontuario, SPLIT_PART(embryo_EmbryoID, '-', 1)), 2), 2)"
            status = "[OK]" if "trat_peso_paciente" in available_columns and "trat_altura_paciente" in available_columns else "[--]"
        # Special handling for patient+slide-level columns with MAX() aggregation
        elif col in ["Diagnosis", "Patient Comments", "Previus OD ET", "Oocyte Source"]:
            source_col_mapped = COLUMN_MAPPING.get(col)
            if source_col_mapped is not None and source_col_mapped in available_columns:
                source_col = f"MAX({source_col_mapped}) OVER (PARTITION BY micro_prontuario, SPLIT_PART(embryo_EmbryoID, '-', 1))"
                status = "[OK]"
            else:
                source_col = "NULL (unmapped or missing)" if source_col_mapped is None else f"NULL (missing: {source_col_mapped})"
                status = "[--]"
        # Special handling for Previus ET ranking
        elif col == "Previus ET":
            source_col = (
                "DENSE_RANK() OVER ("
                "PARTITION BY micro_prontuario "
                "ORDER BY SPLIT_PART(embryo_EmbryoID, '-', 1) ASC"
                ") - 1"
            )
            status = "[OK]"
        else:
            source_col = COLUMN_MAPPING.get(col, "NULL (unmapped)")
            if source_col == "NULL (unmapped)":
                status = "[--]"
            elif source_col is None:
                status = "[--]"
                source_col = "NULL (explicitly unmapped)"
            elif col == "Birth Year" and source_col == "patient_DateOfBirth":
                status = "[OK]" if source_col in available_columns else "[--]"
                source_col = f"EXTRACT(YEAR FROM {source_col})"
            else:
                status = "[OK]" if source_col in available_columns else "[--]"
        logger.info(f"  {i:2d}. {status} {col:35s} <- {source_col}")
    
    if unmapped:
        logger.info("")
        logger.info(f"Unmapped columns ({len(unmapped)}): {unmapped}")
    if missing:
        logger.info("")
        logger.info(f"Missing source columns ({len(missing)}):")
        for target, source in missing:
            logger.info(f"  {target} <- {source} (not found in source table)")

def main():
    """Main function"""
    
    logger.info("=== CREATING DATA_PLOIDIA TABLE ===")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("")
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_database_connection(read_only=False)
        
        # Create the table
        create_data_ploidia_table(conn)
        
        logger.info("")
        logger.info("Successfully created gold.data_ploidia table")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()


