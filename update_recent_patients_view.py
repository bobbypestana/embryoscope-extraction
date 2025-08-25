#!/usr/bin/env python3
"""
Update the recent_patients_timeline view to use 2023-01-01 as cutoff date
"""

import duckdb as db
import pandas as pd
from datetime import datetime

def update_recent_patients_view():
    """Update the recent_patients_timeline view with 2023-01-01 cutoff"""
    
    print("🔄 Updating recent_patients_timeline view...")
    
    try:
        # Connect to database
        path_to_db = 'database/huntington_data_lake.duckdb'
        conn = db.connect(path_to_db)
        print(f"✅ Connected to database: {path_to_db}")
        
        # Drop and recreate the view with 2023-01-01 cutoff
        conn.execute("DROP VIEW IF EXISTS gold.recent_patients_timeline")
        
        create_view_query = """
        CREATE VIEW gold.recent_patients_timeline AS
        WITH clean_data AS (
            SELECT *,
            MIN(event_date) OVER (PARTITION BY prontuario) as first_event_date
            FROM gold.all_patients_timeline
        )
        SELECT *
        FROM clean_data
        WHERE first_event_date >= '2023-01-01'
        ORDER BY prontuario, event_date DESC
        """
        
        conn.execute(create_view_query)
        
        # Verify the view was created
        view_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT prontuario) as unique_patients,
                COUNT(DISTINCT event_date) as unique_dates,
                COUNT(DISTINCT reference) as unique_references,
                COUNT(DISTINCT reference_value) as unique_reference_values,
                COUNT(CASE WHEN reference_value = 'Ciclo a Fresco FIV' THEN 1 END) as fiv_cycles
            FROM gold.recent_patients_timeline
        """).fetchdf()
        
        print(f"✅ View updated successfully!")
        print(f"📊 View Statistics (2023-01-01 cutoff):")
        print(f"   - Total events: {view_stats['total_events'].iloc[0]:,}")
        print(f"   - Unique patients: {view_stats['unique_patients'].iloc[0]:,}")
        print(f"   - FIV cycles: {view_stats['fiv_cycles'].iloc[0]:,}")
        
        # Check if patient 825890 is now included
        patient_check = conn.execute("""
            SELECT COUNT(*) as total_events,
                   COUNT(CASE WHEN reference_value = 'Ciclo a Fresco FIV' THEN 1 END) as fiv_cycles,
                   MIN(event_date) as first_event_date,
                   MAX(event_date) as last_event_date
            FROM gold.recent_patients_timeline
            WHERE prontuario = 825890
        """).fetchdf()
        
        print(f"\n🔍 Patient 825890 check:")
        print(patient_check.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    update_recent_patients_view()


