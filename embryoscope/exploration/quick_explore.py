#!/usr/bin/env python3
"""
Quick Embryoscope Data Exploration Script
Run this script to quickly explore the embryoscope database without Jupyter.
"""

import duckdb
import pandas as pd
from datetime import datetime
import sys
import os

def connect_to_database():
    """Connect to the embryoscope database."""
    db_path = "../database/embryoscope_vila_mariana.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        print("Please run the extraction first.")
        return None
    
    try:
        conn = duckdb.connect(db_path)
        print(f"Connected to database: {db_path}")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def show_database_overview(conn):
    """Show overview of the database."""
    print("\n=== Database Overview ===")
    
    # List tables
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Available tables: {[table[0] for table in tables]}")
    
    # Data tables
    data_tables = ['embryoscope.data_patients', 'embryoscope.data_treatments', 'embryoscope.data_embryo_data', 'embryoscope.data_idascore']
    
    for table in data_tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            latest = conn.execute(f"SELECT MAX(_extraction_timestamp) FROM {table}").fetchone()[0]
            locations = conn.execute(f"SELECT DISTINCT _location FROM {table}").fetchall()
            location_list = [loc[0] for loc in locations]
            
            print(f"\n{table}:")
            print(f"  - Total rows: {count:,}")
            print(f"  - Latest extraction: {latest}")
            print(f"  - Locations: {location_list}")
            
        except Exception as e:
            print(f"\n{table}: Error - {e}")

def show_latest_data_sample(conn, table_name, limit=5):
    """Show a sample of the latest data from a table."""
    print(f"\n=== Latest {table_name} Sample ===")
    
    query = f"""
    SELECT * FROM embryoscope.{table_name} 
    WHERE _extraction_timestamp = (
        SELECT MAX(_extraction_timestamp) FROM embryoscope.{table_name}
    )
    ORDER BY _location
    LIMIT {limit}
    """
    
    try:
        df = conn.execute(query).df()
        if not df.empty:
            print(f"Shape: {df.shape}")
            print(df.to_string(index=False))
        else:
            print("No data found.")
    except Exception as e:
        print(f"Error: {e}")

def show_summary_statistics(conn):
    """Show summary statistics."""
    print("\n=== Summary Statistics ===")
    
    # Comprehensive query
    query = """
    WITH latest_patients AS (
        SELECT * FROM embryoscope.data_patients 
        WHERE _extraction_timestamp = (SELECT MAX(_extraction_timestamp) FROM embryoscope.data_patients)
    ),
    latest_treatments AS (
        SELECT * FROM embryoscope.data_treatments 
        WHERE _extraction_timestamp = (SELECT MAX(_extraction_timestamp) FROM embryoscope.data_treatments)
    ),
    latest_embryos AS (
        SELECT * FROM embryoscope.data_embryo_data 
        WHERE _extraction_timestamp = (SELECT MAX(_extraction_timestamp) FROM embryoscope.data_embryo_data)
    ),
    latest_scores AS (
        SELECT * FROM embryoscope.data_idascore 
        WHERE _extraction_timestamp = (SELECT MAX(_extraction_timestamp) FROM embryoscope.data_idascore)
    )
    SELECT 
        e._location,
        COUNT(DISTINCT e.PatientIDx) as unique_patients,
        COUNT(DISTINCT e.TreatmentName) as unique_treatments,
        COUNT(e.EmbryoID) as total_embryos,
        AVG(s.Score) as avg_score,
        COUNT(s.Score) as scored_embryos,
        SUM(CASE WHEN s.Viability = 'Viable' THEN 1 ELSE 0 END) as viable_embryos
    FROM latest_embryos e
    LEFT JOIN latest_scores s ON e.EmbryoID = s.EmbryoID AND e._location = s._location
    GROUP BY e._location
    ORDER BY e._location
    """
    
    try:
        df = conn.execute(query).df()
        if not df.empty:
            df['viability_rate'] = (df['viable_embryos'] / df['total_embryos'] * 100).round(1)
            print(df.to_string(index=False))
        else:
            print("No data found.")
    except Exception as e:
        print(f"Error: {e}")

def show_extraction_history(conn, limit=5):
    """Show recent extraction history."""
    print(f"\n=== Recent Extraction History (Last {limit}) ===")
    
    query = f"""
    SELECT 
        run_id,
        location,
        extraction_timestamp,
        total_rows_processed,
        processing_time_seconds,
        status
    FROM embryoscope.incremental_runs
    ORDER BY extraction_timestamp DESC
    LIMIT {limit}
    """
    
    try:
        df = conn.execute(query).df()
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No extraction history found.")
    except Exception as e:
        print(f"Error: {e}")

def export_data(conn, output_dir="data_output"):
    """Export data to CSV files."""
    print(f"\n=== Exporting Data to {output_dir} ===")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Export each table
    tables = ['embryoscope.data_patients', 'embryoscope.data_treatments', 'embryoscope.data_embryo_data', 'embryoscope.data_idascore']
    
    for table in tables:
        try:
            query = f"""
            SELECT * FROM {table} 
            WHERE _extraction_timestamp = (
                SELECT MAX(_extraction_timestamp) FROM {table}
            )
            """
            
            df = conn.execute(query).df()
            if not df.empty:
                filename = f"{output_dir}/{table}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"Exported {len(df)} rows to {filename}")
            else:
                print(f"No data to export for {table}")
                
        except Exception as e:
            print(f"Error exporting {table}: {e}")

def main():
    """Main function."""
    print("Embryoscope Data Explorer")
    print("=" * 50)
    
    # Connect to database
    conn = connect_to_database()
    if conn is None:
        return 1
    
    try:
        # Show overview
        show_database_overview(conn)
        
        # Show samples
        show_latest_data_sample(conn, 'data_patients', 3)
        show_latest_data_sample(conn, 'data_embryo_data', 3)
        
        # Show statistics
        show_summary_statistics(conn)
        
        # Show extraction history
        show_extraction_history(conn)
        
        # Export data
        export_data(conn)
        
    except Exception as e:
        print(f"Error during exploration: {e}")
        return 1
    finally:
        conn.close()
        print("\nDatabase connection closed.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 