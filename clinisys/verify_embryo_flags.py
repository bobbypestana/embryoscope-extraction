import duckdb
import pandas as pd

# Connect to database
con = duckdb.connect('../database/clinisys_all.duckdb')

print("=== Verification of Embryo Matching Logic ===")

# Check if new columns exist
print("\n1. Checking if new columns exist:")
try:
    cols = con.execute('DESCRIBE silver.view_micromanipulacao_oocitos').df()
    new_cols = cols[cols['column_name'].isin(['flag_embryoscope', 'embryo_number'])]
    if not new_cols.empty:
        print("✓ New columns found:")
        print(new_cols.to_string())
    else:
        print("✗ New columns not found")
except Exception as e:
    print(f"✗ Error checking columns: {e}")

# Check sample data
print("\n2. Sample data (first 10 rows):")
try:
    sample = con.execute('''
        SELECT id, id_micromanipulacao, InseminacaoOocito, flag_embryoscope, embryo_number 
        FROM silver.view_micromanipulacao_oocitos 
        LIMIT 10
    ''').df()
    print(sample.to_string())
except Exception as e:
    print(f"✗ Error getting sample data: {e}")

# Check flag distribution
print("\n3. Flag distribution:")
try:
    flag_dist = con.execute('''
        SELECT flag_embryoscope, COUNT(*) as count 
        FROM silver.view_micromanipulacao_oocitos 
        GROUP BY flag_embryoscope
    ''').df()
    print(flag_dist.to_string())
except Exception as e:
    print(f"✗ Error getting flag distribution: {e}")

# Check embryo numbering logic
print("\n4. Embryo numbering verification:")
try:
    # Get a sample micromanipulation with multiple embryos
    sample_micromanip = con.execute('''
        SELECT id_micromanipulacao, COUNT(*) as embryo_count
        FROM silver.view_micromanipulacao_oocitos 
        WHERE flag_embryoscope = 1
        GROUP BY id_micromanipulacao 
        HAVING COUNT(*) > 1
        LIMIT 1
    ''').df()
    
    if not sample_micromanip.empty:
        micromanip_id = sample_micromanip.iloc[0]['id_micromanipulacao']
        print(f"Sample micromanipulation ID: {micromanip_id}")
        
        embryos = con.execute(f'''
            SELECT id, id_micromanipulacao, InseminacaoOocito, flag_embryoscope, embryo_number
            FROM silver.view_micromanipulacao_oocitos 
            WHERE id_micromanipulacao = {micromanip_id}
            ORDER BY id
        ''').df()
        print(embryos.to_string())
    else:
        print("No micromanipulations with multiple embryos found")
except Exception as e:
    print(f"✗ Error verifying embryo numbering: {e}")

con.close()
print("\n=== Verification Complete ===") 