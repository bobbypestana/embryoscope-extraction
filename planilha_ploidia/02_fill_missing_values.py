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

def get_null_counts(conn):
    """Get count of NULL values for each column"""
    query = """
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN "BMI" IS NULL THEN 1 ELSE 0 END) as null_bmi,
        SUM(CASE WHEN "Diagnosis" IS NULL THEN 1 ELSE 0 END) as null_diagnosis,
        SUM(CASE WHEN "Oocyte Source" IS NULL THEN 1 ELSE 0 END) as null_oocyte_source
    FROM huntington.gold.data_ploidia
    """
    result = conn.execute(query).fetchone()
    return {
        'total_rows': result[0],
        'null_bmi': result[1],
        'null_diagnosis': result[2],
        'null_oocyte_source': result[3]
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
            origem_material as oocyte_source,
            COUNT(*) as frequency,
            ROW_NUMBER() OVER (
                PARTITION BY prontuario 
                ORDER BY COUNT(*) DESC
            ) as rn
        FROM silver.view_tratamentos
        WHERE origem_material IS NOT NULL
        GROUP BY prontuario, origem_material
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
        logger.info("NULL counts BEFORE filling:")
        before_counts = get_null_counts(conn)
        logger.info(f"  Total rows: {before_counts['total_rows']:,}")
        logger.info(f"  NULL BMI: {before_counts['null_bmi']:,}")
        logger.info(f"  NULL Diagnosis: {before_counts['null_diagnosis']:,}")
        logger.info(f"  NULL Oocyte Source: {before_counts['null_oocyte_source']:,}")
        logger.info("")
        
        # Fill missing values
        bmi_updates = fill_bmi_values(conn)
        diagnosis_updates = fill_diagnosis_values(conn)
        oocyte_source_updates = fill_oocyte_source_values(conn)
        
        # Get NULL counts after filling
        logger.info("")
        logger.info("NULL counts AFTER filling:")
        after_counts = get_null_counts(conn)
        logger.info(f"  Total rows: {after_counts['total_rows']:,}")
        logger.info(f"  NULL BMI: {after_counts['null_bmi']:,}")
        logger.info(f"  NULL Diagnosis: {after_counts['null_diagnosis']:,}")
        logger.info(f"  NULL Oocyte Source: {after_counts['null_oocyte_source']:,}")
        logger.info("")
        
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
