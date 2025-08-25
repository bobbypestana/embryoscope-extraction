#!/usr/bin/env python3
import duckdb as db

print("Starting simple test...")

try:
    # Connect to the database
    con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)
    print("Connected to database successfully")
    
    # Simple query
    result = con.execute('SELECT 1 as test').fetchdf()
    print(f"Simple query result: {result}")
    
    con.close()
    print("Connection closed successfully")
    
except Exception as e:
    print(f"Error: {e}")

print("Test completed.")
