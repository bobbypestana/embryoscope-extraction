#!/usr/bin/env python3
"""
Check current finops_summary for patient 162173
"""

import duckdb as db
import pandas as pd

def check_patient_162173_current():
    """Check current finops_summary for patient 162173"""
    
    print("🔍 Checking current finops_summary for patient 162173...")
    
    try:
        # Connect to database
        path_to_db = '../database/huntington_data_lake.duckdb'
        conn = db.connect(path_to_db)
        print(f"✅ Connected to database: {path_to_db}")
        
        # Check current finops_summary for this patient
        print("\n💰 Current finops_summary for patient 162173:")
        finops_row = conn.execute("""
            SELECT *
            FROM gold.finops_summary
            WHERE prontuario = 162173
        """).fetchdf()
        print(finops_row.to_string(index=False))
        
        # Check what the query should return for this patient
        print("\n🔢 What the query should return for patient 162173:")
        expected_result = conn.execute("""
            SELECT
                prontuario,
                COUNT(CASE
                    WHEN LOWER(COALESCE(resultado_tratamento, '')) NOT IN ('no transfer', '') THEN 1
                END) AS cycle_with_transfer,
                COUNT(CASE
                    WHEN LOWER(COALESCE(resultado_tratamento, '')) IN (
                        'no transfer',
                        'sem transferencia',
                        'sem transferência',
                        'sem transfer',
                        ''
                    ) THEN 1
                END) AS cycle_without_transfer,
                MIN(event_date) AS timeline_first_date,
                MAX(event_date) AS timeline_last_date
            FROM gold.recent_patients_timeline
            WHERE reference_value = 'Ciclo a Fresco FIV'
                AND flag_date_estimated = FALSE
                AND prontuario = 162173
            GROUP BY prontuario
        """).fetchdf()
        print(expected_result.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    check_patient_162173_current()


