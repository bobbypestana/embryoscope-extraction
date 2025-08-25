#!/usr/bin/env python3
"""
Debug script to check patient 162173 and understand the data structure
"""

import duckdb as db
import pandas as pd

def debug_patient_162173():
    """Debug patient 162173 data"""
    
    print("🔍 Debugging patient 162173...")
    
    try:
        # Connect to database
        path_to_db = '../database/huntington_data_lake.duckdb'
        conn = db.connect(path_to_db)
        print(f"✅ Connected to database: {path_to_db}")
        
        # Check the structure of recent_patients_timeline
        print("\n📋 Checking recent_patients_timeline structure:")
        structure = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'gold' 
            AND table_name = 'recent_patients_timeline'
            ORDER BY ordinal_position
        """).fetchdf()
        print(structure.to_string(index=False))
        
        # Check all FIV cycles for patient 162173
        print("\n🔄 All FIV cycles for patient 162173:")
        fiv_cycles = conn.execute("""
            SELECT prontuario, event_date, reference, reference_value, 
                   flag_date_estimated, resultado_tratamento, additional_info
            FROM gold.recent_patients_timeline
            WHERE prontuario = 162173
            AND reference_value = 'Ciclo a Fresco FIV'
            ORDER BY event_date
        """).fetchdf()
        print(fiv_cycles.to_string(index=False))
        
        # Check current finops_summary for this patient
        print("\n💰 Current finops_summary for patient 162173:")
        finops_row = conn.execute("""
            SELECT *
            FROM gold.finops_summary
            WHERE prontuario = 162173
        """).fetchdf()
        print(finops_row.to_string(index=False))
        
        # Count FIV cycles manually
        print("\n🔢 Manual count of FIV cycles for patient 162173:")
        manual_count = conn.execute("""
            SELECT 
                COUNT(*) as total_fiv_cycles,
                COUNT(CASE WHEN flag_date_estimated = FALSE THEN 1 END) as non_estimated_cycles,
                COUNT(CASE WHEN flag_date_estimated = TRUE THEN 1 END) as estimated_cycles
            FROM gold.recent_patients_timeline
            WHERE prontuario = 162173
            AND reference_value = 'Ciclo a Fresco FIV'
        """).fetchdf()
        print(manual_count.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    debug_patient_162173()


