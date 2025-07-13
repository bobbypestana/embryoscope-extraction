#!/usr/bin/env python3
"""
Simple script to check what's in the embryoscope database.
"""

import duckdb
import os

def check_database():
    """Check the database contents."""
    
    # Check if the database file exists
    db_path = "../../database/embryoscope_vila_mariana.db"
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return
    
    print(f"Database file exists: {db_path}")
    print(f"File size: {os.path.getsize(db_path)} bytes")
    
    # Connect to database
    conn = duckdb.connect(db_path)
    
    # List all schemas
    print("\n=== Schemas ===")
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
    for schema in schemas:
        print(f"  - {schema[0]}")
    
    # List all tables
    print("\n=== All Tables ===")
    tables = conn.execute("SELECT table_name FROM information_schema.tables;").fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    # Check embryoscope schema specifically
    print("\n=== Embryoscope Schema Tables ===")
    try:
        embryoscope_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'embryoscope';").fetchall()
        for table in embryoscope_tables:
            print(f"  - embryoscope.{table[0]}")
            
            # Get row count for each table
            try:
                count_result = conn.execute(f"SELECT COUNT(*) FROM embryoscope.{table[0]}").fetchone()
                count = count_result[0] if count_result else 0
                print(f"    Rows: {count}")
            except Exception as e:
                print(f"    Error getting count: {e}")
                
    except Exception as e:
        print(f"Error accessing embryoscope schema: {e}")
    
    # Check for any data
    print("\n=== Sample Data ===")
    try:
        # Try to get a sample from patients table
        sample = conn.execute("SELECT * FROM embryoscope.data_patients LIMIT 3").fetchall()
        if sample:
            print("Sample patients data:")
            for row in sample:
                print(f"  {row}")
        else:
            print("No patients data found")
    except Exception as e:
        print(f"Error getting sample data: {e}")
    
    conn.close()

if __name__ == "__main__":
    check_database() 