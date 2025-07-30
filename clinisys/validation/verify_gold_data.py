import duckdb
import pandas as pd

# Connect to the data lake database
db = duckdb.connect('../database/huntington_data_lake.duckdb')

print("=== Verifying Gold Data ===")

# Check the gold table structure
print("\n--- Gold Table Structure ---")
schema = db.execute("DESCRIBE gold.clinisys_embrioes").df()
print(f"Total columns: {len(schema)}")
print(f"Sample columns: {schema['column_name'].head(10).tolist()}")

# Check row count
print("\n--- Row Count ---")
row_count = db.execute("SELECT COUNT(*) as count FROM gold.clinisys_embrioes").df()['count'].iloc[0]
print(f"Total rows: {row_count}")

# Check for the specific case we fixed
print("\n--- Checking Specific Case: prontuario=160751, codigo_ficha=26549 ---")
df_specific = db.execute("""
    SELECT 
        oocito_id,
        oocito_id_micromanipulacao,
        micro_prontuario,
        micro_codigo_ficha,
        micro_Data_DL,
        micro_oocitos
    FROM gold.clinisys_embrioes 
    WHERE micro_prontuario = 160751 AND micro_codigo_ficha = 26549
    ORDER BY oocito_id
    LIMIT 10
""").df()

if len(df_specific) > 0:
    print(f"✓ Found {len(df_specific)} records for the specific case")
    print(df_specific)
else:
    print("? No records found for the specific case")

# Check for duplicates in key fields
print("\n--- Checking for Duplicates in Key Fields ---")

# Check micromanipulacao duplicates
df_micro_dups = db.execute("""
    SELECT micro_codigo_ficha, COUNT(*) as count
    FROM gold.clinisys_embrioes 
    WHERE micro_codigo_ficha IS NOT NULL
    GROUP BY micro_codigo_ficha
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 5
""").df()

if df_micro_dups.empty:
    print("✓ No duplicate micromanipulacao records found")
else:
    print(f"✗ Found {len(df_micro_dups)} duplicate micromanipulacao records")
    print(df_micro_dups)

# Check oocito duplicates
df_oocito_dups = db.execute("""
    SELECT oocito_id, COUNT(*) as count
    FROM gold.clinisys_embrioes 
    WHERE oocito_id IS NOT NULL
    GROUP BY oocito_id
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 5
""").df()

if df_oocito_dups.empty:
    print("✓ No duplicate oocito records found")
else:
    print(f"✗ Found {len(df_oocito_dups)} duplicate oocito records")
    print(df_oocito_dups)

# Sample data quality check
print("\n--- Sample Data Quality Check ---")
sample = db.execute("""
    SELECT 
        oocito_id,
        oocito_id_micromanipulacao,
        oocito_Maturidade,
        micro_prontuario,
        micro_codigo_ficha,
        micro_Data_DL,
        micro_oocitos
    FROM gold.clinisys_embrioes 
    WHERE micro_prontuario IS NOT NULL
    ORDER BY micro_prontuario, micro_codigo_ficha
    LIMIT 10
""").df()

print("Sample data:")
print(sample)

db.close() 