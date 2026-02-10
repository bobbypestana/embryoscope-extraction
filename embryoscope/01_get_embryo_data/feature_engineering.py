import logging
import duckdb

logger = logging.getLogger(__name__)

def feature_creation(con, table, db_name='silver'):
    """
    Apply feature engineering to specific tables.
    
    Args:
        con: DuckDB connection object
        table: Name of the table to process (e.g., 'patients', 'treatments')
        db_name: Name of the database schema (default: 'silver')
    """
    logger.info(f"Checking for feature engineering for table: {table}")
    
    if table == 'patients':
         apply_patient_features(con, table, db_name)
    elif table == 'treatments':
         apply_treatment_features(con, table, db_name)
    elif table == 'embryo_data':
         apply_embryo_features(con, table, db_name)
    else:
        logger.debug(f"No feature engineering defined for table: {table}")

def apply_patient_features(con, table, db_name):
    """Features for patients table"""
    logger.info(f"[{db_name}] Applying features for {table}")
    
    # 1. Year of Birth
    # Extract year from DateOfBirth if it exists
    count = con.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
    if count > 0:
        try:
            # Check if DateOfBirth exists
            cols = con.execute(f"DESCRIBE silver.{table}").df()['column_name'].tolist()
            if 'DateOfBirth' in cols:
                con.execute(f"""
                    ALTER TABLE silver.{table} ADD COLUMN IF NOT EXISTS YearOfBirth INTEGER;
                    UPDATE silver.{table} 
                    SET YearOfBirth = EXTRACT(YEAR FROM CAST(DateOfBirth AS DATE))
                    WHERE DateOfBirth IS NOT NULL;
                """)
                logger.info(f"[{db_name}] Added YearOfBirth feature")
            else:
                logger.warning(f"[{db_name}] DateOfBirth column missing, cannot calculate YearOfBirth")
        except Exception as e:
            logger.error(f"[{db_name}] Error adding YearOfBirth: {e}")

def apply_treatment_features(con, table, db_name):
    """Features for treatments table"""
    pass

def apply_embryo_features(con, table, db_name):
    """Features for embryo_data table"""
    logger.info(f"[{db_name}] Applying features for {table}")
    
    # 1. Date from EmbryoID
    # EmbryoID format often contains date information or we need to extract it
    # User request: "Date fromembryo_EmbryoID"
    # Assuming EmbryoID format like: D2023.05.15_...
    
    count = con.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0]
    if count > 0:
        try:
            # Check if EmbryoID exists
            cols = con.execute(f"DESCRIBE silver.{table}").df()['column_name'].tolist()
            if 'EmbryoID' in cols:
                # Logic to extract date from EmbryoID
                # Tries to parse 'YYYY.MM.DD' pattern from the ID
                # Example: 2023.05.15_S123... -> 2023-05-15
                con.execute(f"""
                    ALTER TABLE silver.{table} ADD COLUMN IF NOT EXISTS EmbryoDate DATE;
                    
                    UPDATE silver.{table}
                    SET EmbryoDate = CASE
                        -- Try to extract pattern YYYY.MM.DD
                        WHEN regexp_matches(EmbryoID, '20[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}') THEN
                            TRY_CAST(regexp_extract(EmbryoID, '(20[0-9]{2}\\.[0-9]{2}\\.[0-9]{2})', 1) AS DATE)
                        ELSE NULL
                    END;
                """)
                logger.info(f"[{db_name}] Added EmbryoDate feature extracted from EmbryoID")
            else:
                logger.warning(f"[{db_name}] EmbryoID column missing, cannot extract date")
        except Exception as e:
            logger.error(f"[{db_name}] Error adding EmbryoDate: {e}")
