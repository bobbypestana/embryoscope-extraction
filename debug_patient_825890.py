#!/usr/bin/env python3
"""
Debug script to investigate prontuario 825890 timeline data
"""

import duckdb as db
import pandas as pd

def debug_patient_825890():
    """Debug prontuario 825890 timeline data"""
    
    print("🔍 Debugging prontuario 825890...")
    
    try:
        # Connect to database
        path_to_db = 'database/huntington_data_lake.duckdb'
        conn = db.connect(path_to_db)
        print(f"✅ Connected to database: {path_to_db}")
        
        # Check if patient exists in recent_patients_timeline
        patient_check = conn.execute("""
            SELECT COUNT(*) as total_events,
                   COUNT(CASE WHEN reference_value = 'Ciclo a Fresco FIV' THEN 1 END) as fiv_cycles,
                   MIN(event_date) as first_event_date,
                   MAX(event_date) as last_event_date
            FROM gold.recent_patients_timeline
            WHERE prontuario = 825890
        """).fetchdf()
        
        print(f"\n📋 Patient 825890 in recent_patients_timeline:")
        print(patient_check.to_string(index=False))
        
        # Get all events for this patient
        all_events = conn.execute("""
            SELECT prontuario, event_date, reference, reference_value, flag_date_estimated, additional_info
            FROM gold.recent_patients_timeline
            WHERE prontuario = 825890
            ORDER BY event_date
        """).fetchdf()
        
        print(f"\n📋 All events for patient 825890:")
        print(all_events.to_string(index=False))
        
        # Check FIV cycles specifically
        fiv_cycles = conn.execute("""
            SELECT prontuario, event_date, reference, reference_value, flag_date_estimated, additional_info
            FROM gold.recent_patients_timeline
            WHERE prontuario = 825890
            AND reference_value = 'Ciclo a Fresco FIV'
            ORDER BY event_date
        """).fetchdf()
        
        print(f"\n🔄 FIV cycles for patient 825890:")
        print(fiv_cycles.to_string(index=False))
        
        # Check the JSON parsing for FIV cycles
        if not fiv_cycles.empty:
            print(f"\n🔍 JSON parsing for FIV cycles:")
            for idx, row in fiv_cycles.iterrows():
                print(f"Event {idx + 1}:")
                print(f"  - flag_date_estimated: {row['flag_date_estimated']}")
                print(f"  - additional_info: {row['additional_info']}")
                
                # Test JSON extraction
                resultado_test = conn.execute("""
                    SELECT 
                        json_extract_string(?, '$."ResultadoTratamento"') as key1,
                        json_extract_string(?, '$."Resultado Tratamento"') as key2,
                        json_extract_string(?, '$."Resultado do Tratamento"') as key3
                """, [row['additional_info'], row['additional_info'], row['additional_info']]).fetchdf()
                
                print(f"  - JSON extraction test:")
                print(f"    * ResultadoTratamento: '{resultado_test['key1'].iloc[0]}'")
                print(f"    * Resultado Tratamento: '{resultado_test['key2'].iloc[0]}'")
                print(f"    * Resultado do Tratamento: '{resultado_test['key3'].iloc[0]}'")
        
        # Check current finops_summary for this patient
        finops_row = conn.execute("""
            SELECT *
            FROM gold.finops_summary
            WHERE prontuario = 825890
        """).fetchdf()
        
        print(f"\n💰 Current finops_summary for patient 825890:")
        print(finops_row.to_string(index=False))
        
        # Check if patient exists in original timeline (not filtered)
        original_check = conn.execute("""
            SELECT COUNT(*) as total_events,
                   COUNT(CASE WHEN reference_value = 'Ciclo a Fresco FIV' THEN 1 END) as fiv_cycles,
                   MIN(event_date) as first_event_date,
                   MAX(event_date) as last_event_date
            FROM gold.all_patients_timeline
            WHERE prontuario = 825890
        """).fetchdf()
        
        print(f"\n📋 Patient 825890 in original all_patients_timeline:")
        print(original_check.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    debug_patient_825890()

