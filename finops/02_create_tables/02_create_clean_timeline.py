#!/usr/bin/env python3
"""
Create clean timeline view for patients with first event after 2024-01-01
"""

import pandas as pd
import duckdb as db
import os
from datetime import datetime

def get_database_connection():
    """Create and return a connection to the huntington_data_lake database"""
    # Resolve DB path relative to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db)
    
    print(f"Connected to database: {path_to_db}")
    print(f"Database file exists: {os.path.exists(path_to_db)}")
    
    return conn

def create_recent_patients_timeline_view(conn):
    """Create a view for patients with first event after 2024-01-01"""
    
    print("Creating recent_patients_timeline view...")
    
    # Drop view if it exists
    conn.execute("DROP VIEW IF EXISTS gold.recent_patients_timeline")
    
    # Create the view
    create_view_query = """
    CREATE VIEW gold.recent_patients_timeline AS
    with clean_data as (
            SELECT *,
            MIN(event_date) OVER (PARTITION BY prontuario) as first_event_date
            FROM gold.all_patients_timeline
        )
        SELECT* 
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
            MIN(event_date) as earliest_date,
            MAX(event_date) as latest_date,
            COUNT(DISTINCT reference) as reference_types
        FROM gold.recent_patients_timeline
    """).fetchdf()
    
    print("View created successfully.")
    print("View Statistics:")
    print(f"   - Total events: {view_stats['total_events'].iloc[0]:,}")
    print(f"   - Unique patients: {view_stats['unique_patients'].iloc[0]:,}")
    print(f"   - Date range: {view_stats['earliest_date'].iloc[0]} to {view_stats['latest_date'].iloc[0]}")
    print(f"   - Reference types: {view_stats['reference_types'].iloc[0]}")
    
    return view_stats


def main():
    """Main function to create the recent patients timeline view"""
    
    print("=== CREATING RECENT PATIENTS TIMELINE VIEW ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    try:
        # Connect to database
        conn = get_database_connection()
        
        # Create the view
        view_stats = create_recent_patients_timeline_view(conn)

        
        print("\nSuccessfully created gold.recent_patients_timeline view")
        print(f"View contains {view_stats['unique_patients'].iloc[0]:,} patients with {view_stats['total_events'].iloc[0]:,} total events")
        print(f"Date range: {view_stats['earliest_date'].iloc[0]} to {view_stats['latest_date'].iloc[0]}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()