#!/usr/bin/env python3
"""
Fill missing values in gold.data_ploidia table

This script fills NULL values in BMI, Diagnosis, and Oocyte Source columns
by using the most frequent values from silver.view_tratamentos per patient (prontuario).
"""

import duckdb
import pandas as pd
from datetime import datetime
import os
import logging
import sys

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
    """Create and return a connection to clinisys_all database and attach huntington_data_lake"""
    repo_root = os.path.dirname(os.path.dirname(__file__))
    
    # Connect to clinisys_all (has view_tratamentos)
    clinisys_db = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn = duckdb.connect(clinisys_db, read_only=True)
    logger.info(f"Connected to clinisys database: {clinisys_db}")
    
    # Attach huntington_data_lake (has gold.data_ploidia)
    huntington_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn.execute(f"ATTACH '{huntington_db}' AS huntington (READ_WRITE)")
    logger.info(f"Attached huntington database: {huntington_db}")
    
    return conn

def log_all_null_counts(conn, label=""):
    """Fetch and log NULL/blank counts for all columns in gold.data_ploidia"""
    logger.info("=" * 80)
    logger.info(f"NULL/BLANK COUNTS {label}")
    logger.info("=" * 80)
    
    # Get column names
    try:
        columns = conn.execute("DESCRIBE huntington.gold.data_ploidia").df()['column_name'].tolist()
    except Exception as e:
        logger.error(f"Error describing table: {e}")
        return

    # Build query for all columns
    # We check for NULL or empty string (blanks)
    sum_parts = []
    for col in columns:
        sum_parts.append(f'SUM(CASE WHEN "{col}" IS NULL OR CAST("{col}" AS VARCHAR) = \'\' THEN 1 ELSE 0 END) as "{col}"')
    
    query = f"SELECT COUNT(*) as total_rows, {', '.join(sum_parts)} FROM huntington.gold.data_ploidia"
    result_df = conn.execute(query).df()
    total_rows = result_df['total_rows'].iloc[0]
    
    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"{'Column':<35} | {'Null/Blank':>10} | {'%':>7}")
    logger.info("-" * 60)
    
    # Collect counts
    null_counts = []
    for col in columns:
        count = int(result_df[col].iloc[0])
        null_counts.append((col, count))
    
    # Sort by count descending
    null_counts.sort(key=lambda x: x[1], reverse=True)
    
    for col, count in null_counts:
        pct = (count / total_rows * 100) if total_rows > 0 else 0
        logger.info(f"{col:<35} | {count:>10,} | {pct:>6.1f}%")
    
    logger.info("-" * 60)
    logger.info("")
    
    # Return specific counts for the legacy summary logic if needed
    # (BMI, Diagnosis, Oocyte Source are the ones explicitly tracked in summary)
    return {
        'total_rows': total_rows,
        'null_bmi': int(result_df['BMI'].iloc[0]) if 'BMI' in result_df else 0,
        'null_diagnosis': int(result_df['Diagnosis'].iloc[0]) if 'Diagnosis' in result_df else 0,
        'null_oocyte_source': int(result_df['Oocyte Source'].iloc[0]) if 'Oocyte Source' in result_df else 0
    }

def fill_bmi_values(conn):
    """Fill NULL BMI values using most frequent weight/height from view_tratamentos per patient"""
    logger.info("=" * 80)
    logger.info("FILLING BMI VALUES")
    logger.info("=" * 80)
    
    # Get most frequent weight and height per patient from view_tratamentos
    query = """
    WITH patient_bmi AS (
        SELECT 
            prontuario,
            peso_paciente,
            altura_paciente,
            ROUND(peso_paciente / POWER(altura_paciente, 2), 2) as calculated_bmi,
            COUNT(*) as frequency,
            ROW_NUMBER() OVER (
                PARTITION BY prontuario 
                ORDER BY COUNT(*) DESC, peso_paciente DESC
            ) as rn
        FROM silver.view_tratamentos
        WHERE peso_paciente IS NOT NULL 
          AND altura_paciente IS NOT NULL
          AND altura_paciente > 0
        GROUP BY prontuario, peso_paciente, altura_paciente
    )
    SELECT 
        prontuario,
        calculated_bmi
    FROM patient_bmi
    WHERE rn = 1
    """
    
    bmi_values = conn.execute(query).df()
    logger.info(f"Found BMI values for {len(bmi_values)} patients")
    
    # Update data_ploidia with BMI values
    updates = 0
    for _, row in bmi_values.iterrows():
        result = conn.execute(f"""
            UPDATE huntington.gold.data_ploidia
            SET "BMI" = {row['calculated_bmi']}
            WHERE "Patient ID" = {row['prontuario']}
              AND "BMI" IS NULL
        """)
        updates += result.fetchone()[0] if result else 0
    
    logger.info(f"Updated {updates} rows with BMI values")
    return updates

def fill_diagnosis_values(conn):
    """Fill NULL Diagnosis values using most frequent diagnosis from view_tratamentos per patient"""
    logger.info("=" * 80)
    logger.info("FILLING DIAGNOSIS VALUES")
    logger.info("=" * 80)
    
    # Get most frequent diagnosis per patient
    query = """
    WITH patient_diagnosis AS (
        SELECT 
            prontuario,
            fator_infertilidade1 as diagnosis,
            COUNT(*) as frequency,
            ROW_NUMBER() OVER (
                PARTITION BY prontuario 
                ORDER BY COUNT(*) DESC
            ) as rn
        FROM silver.view_tratamentos
        WHERE fator_infertilidade1 IS NOT NULL
        GROUP BY prontuario, fator_infertilidade1
    )
    SELECT 
        prontuario,
        diagnosis
    FROM patient_diagnosis
    WHERE rn = 1
    """
    
    diagnosis_values = conn.execute(query).df()
    logger.info(f"Found Diagnosis values for {len(diagnosis_values)} patients")
    
    # Update data_ploidia with Diagnosis values
    updates = 0
    for _, row in diagnosis_values.iterrows():
        # Escape single quotes in diagnosis
        diagnosis_escaped = str(row['diagnosis']).replace("'", "''")
        result = conn.execute(f"""
            UPDATE huntington.gold.data_ploidia
            SET "Diagnosis" = '{diagnosis_escaped}'
            WHERE "Patient ID" = {row['prontuario']}
              AND "Diagnosis" IS NULL
        """)
        updates += result.fetchone()[0] if result else 0
    
    logger.info(f"Updated {updates} rows with Diagnosis values")
    return updates

def fill_oocyte_source_values(conn):
    """Fill NULL Oocyte Source values using most frequent source from view_tratamentos per patient"""
    logger.info("=" * 80)
    logger.info("FILLING OOCYTE SOURCE VALUES")
    logger.info("=" * 80)
    
    # Get most frequent oocyte source per patient
    query = """
    WITH patient_oocyte_source AS (
        SELECT 
            prontuario,
            origem_ovulo as oocyte_source,
            COUNT(*) as frequency,
            ROW_NUMBER() OVER (
                PARTITION BY prontuario 
                ORDER BY COUNT(*) DESC
            ) as rn
        FROM silver.view_tratamentos
        WHERE origem_ovulo IS NOT NULL
        GROUP BY prontuario, origem_ovulo
    )
    SELECT 
        prontuario,
        oocyte_source
    FROM patient_oocyte_source
    WHERE rn = 1
    """
    
    oocyte_source_values = conn.execute(query).df()
    logger.info(f"Found Oocyte Source values for {len(oocyte_source_values)} patients")
    
    # Update data_ploidia with Oocyte Source values
    updates = 0
    for _, row in oocyte_source_values.iterrows():
        # Escape single quotes in oocyte source
        source_escaped = str(row['oocyte_source']).replace("'", "''")
        result = conn.execute(f"""
            UPDATE huntington.gold.data_ploidia
            SET "Oocyte Source" = '{source_escaped}'
            WHERE "Patient ID" = {row['prontuario']}
              AND "Oocyte Source" IS NULL
        """)
        updates += result.fetchone()[0] if result else 0
    
    logger.info(f"Updated {updates} rows with Oocyte Source values")
    return updates

def main():
    """Main function"""
    logger.info("=" * 80)
    logger.info("FILL MISSING VALUES IN DATA_PLOIDIA")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("")
    
    try:
        # Connect to database
        conn = get_database_connection(read_only=False)
        
        # Get NULL counts before filling
        before_counts = log_all_null_counts(conn, "(BEFORE FILLING)")
        
        # Fill missing values
        bmi_updates = 0
        try:
            bmi_updates = fill_bmi_values(conn)
        except Exception as e:
            logger.error(f"Error filling BMI values: {e}")
            logger.error("Continuing with other updates...")

        diagnosis_updates = 0
        try:
            diagnosis_updates = fill_diagnosis_values(conn)
        except Exception as e:
            logger.error(f"Error filling Diagnosis values: {e}")
            logger.error("Continuing with other updates...")

        # oocyte_source_updates = 0
        # try:
        #     oocyte_source_updates = fill_oocyte_source_values(conn)
        # except Exception as e:
        #     logger.error(f"Error filling Oocyte Source values: {e}")
        #     logger.error("Continuing with other updates...")
        oocyte_source_updates = 0 # Handled in creation script
        
        # Get NULL counts after filling
        after_counts = log_all_null_counts(conn, "(AFTER FILLING)")
        
        # Summary
        logger.info("=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"BMI: Filled {bmi_updates:,} rows ({before_counts['null_bmi'] - after_counts['null_bmi']:,} NULLs removed)")
        logger.info(f"Diagnosis: Filled {diagnosis_updates:,} rows ({before_counts['null_diagnosis'] - after_counts['null_diagnosis']:,} NULLs removed)")
        logger.info(f"Oocyte Source: Filled {oocyte_source_updates:,} rows ({before_counts['null_oocyte_source'] - after_counts['null_oocyte_source']:,} NULLs removed)")
        logger.info("")
        logger.info("=" * 80)
        logger.info("COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
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
