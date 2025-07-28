import duckdb
import pandas as pd

# Connect to database
db = duckdb.connect('../database/clinisys_all.duckdb')

print("=== Testing Deduplication for micromanipulacao ===")

# Check current state in silver
print("\n--- Current silver table state ---")
df_silver = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp, COUNT(*) as count
    FROM silver.view_micromanipulacao 
    WHERE prontuario = 160751 
    GROUP BY codigo_ficha, prontuario, extraction_timestamp
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df_silver)

print("\n--- All records for prontuario=160751 in silver ---")
df_all = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp
    FROM silver.view_micromanipulacao 
    WHERE prontuario = 160751 
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df_all)

# Check bronze table to see the duplicates
print("\n--- Bronze table state (before deduplication) ---")
df_bronze = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp, COUNT(*) as count
    FROM bronze.view_micromanipulacao 
    WHERE prontuario = '160751' 
    GROUP BY codigo_ficha, prontuario, extraction_timestamp
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df_bronze)

print("\n--- All records for prontuario=160751 in bronze ---")
df_bronze_all = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp
    FROM bronze.view_micromanipulacao 
    WHERE prontuario = '160751' 
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df_bronze_all)

# Test the deduplication logic
print("\n--- Testing deduplication logic ---")
df_test = db.execute("""
    SELECT codigo_ficha, prontuario, extraction_timestamp
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY codigo_ficha ORDER BY extraction_timestamp DESC) AS rn
        FROM bronze.view_micromanipulacao
        WHERE prontuario = '160751'
    )
    WHERE rn = 1
    ORDER BY codigo_ficha, extraction_timestamp DESC
""").df()
print(df_test)

db.close() 