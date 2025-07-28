import duckdb
import pandas as pd

# Connect to database
db = duckdb.connect('../database/clinisys_all.duckdb')

# Get bronze tables
bronze_tables = db.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'bronze' 
    ORDER BY table_name
""").df()

print("=== Bronze Tables ===")
for table in bronze_tables['table_name']:
    print(f"\n--- {table} ---")
    try:
        # Get column info
        columns = db.execute(f"DESCRIBE bronze.{table}").df()
        print("Columns:")
        for _, row in columns.iterrows():
            print(f"  {row['column_name']}: {row['column_type']}")
        
        # Check for potential primary key columns
        potential_pks = []
        for col in columns['column_name']:
            if any(pk_indicator in col.lower() for pk_indicator in ['id', 'codigo', 'ficha', 'prontuario']):
                potential_pks.append(col)
        
        if potential_pks:
            print(f"Potential primary keys: {potential_pks}")
        
        # Sample data to understand structure
        sample = db.execute(f"SELECT * FROM bronze.{table} LIMIT 3").df()
        print(f"Sample data shape: {sample.shape}")
        
    except Exception as e:
        print(f"Error: {e}")

db.close() 