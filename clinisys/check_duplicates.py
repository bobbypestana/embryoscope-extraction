import duckdb
import pandas as pd

# Connect to database
db = duckdb.connect('../database/clinisys_all.duckdb')

# Check micromanipulacao table for duplicates
print("=== Checking micromanipulacao table ===")
df = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp, COUNT(*) as count
    FROM silver.view_micromanipulacao 
    WHERE prontuario = 160751 
    GROUP BY codigo_ficha, prontuario, extraction_timestamp
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df)

print("\n=== All records for prontuario=160751 ===")
df2 = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp
    FROM silver.view_micromanipulacao 
    WHERE prontuario = 160751 
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df2)

# Check which tables have extraction_timestamp
print("\n=== Tables with extraction_timestamp ===")
tables = db.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'silver' 
    ORDER BY table_name
""").df()

for table in tables['table_name']:
    try:
        has_timestamp = db.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' 
            AND table_name = '{table}' 
            AND column_name = 'extraction_timestamp'
        """).df()
        if has_timestamp['count'].iloc[0] > 0:
            print(f"✓ {table}")
        else:
            print(f"✗ {table}")
    except:
        print(f"? {table}")

db.close() 